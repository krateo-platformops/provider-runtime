package credentialuse

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/types"
)

// The write loops (§7.5, IP4): the release loop, the missing-ref prober and
// the label janitor. They write, so a process runs them once it is elected,
// while its informers may have run, read-only, long before.

const (
	// releaseWorkers is how many (K, N) batches the release loop works on
	// at once. Two, as §6.6's cost example assumes: a batch is mostly a few
	// GETs, one list and a few patches, so two keep a namespace teardown's
	// batches moving without crowding the API server, and the queue never
	// gives one key to two workers.
	releaseWorkers = 2
	// releaseBackoffBase and releaseBackoffMax bound the backoff of an object
	// a batch failed at (§6.2): it doubles from the base at each of its retries
	// that fails, and starts again once a batch decides the object. The
	// startup recheck's, and the index's checks', are bounded by them too.
	releaseBackoffBase = time.Second
	releaseBackoffMax  = 5 * time.Minute
)

// errInformersEnded is RunWriteLoops' error when RunInformers ends first.
var errInformersEnded = errors.New("credential-use: the informers have stopped")

// errWriteLoopsRunning is RunWriteLoops' error when another RunWriteLoops of
// the same Shared is still running: one process runs one set of write loops.
var errWriteLoopsRunning = errors.New("credential-use: RunWriteLoops is already running")

// RunWriteLoops runs the write loops until ctx is done: the release loop,
// the missing-ref prober and the label janitor. It needs leader election
// (§7.5, IP4), and it may start before, with or after RunInformers: it waits
// for the informers' first sync, then rescans the protected caches and
// starts. It returns at once when RunInformers ends.
//
// The rescan enqueues the batch of every (K, N) finalizer on every cached
// protected object, and every object whose labels the janitor would clean.
// Beside it, when SharedOptions.Namespaces restricts the caches, one live
// list per protected kind finds the held objects outside them and enqueues
// their batches too (§10.5); and each index is read live, with a GET of
// each object it names, for the PerName kinds (§10.6.5).
// The informers may have started long before this process was elected, so
// this covers what they saw before then, and a leader that died between a
// holder's Forget and R3 (§7.5, M1 of the review of revision 16).
func (s *Shared) RunWriteLoops(ctx context.Context) error {
	select {
	case <-s.syncedCh:
	case <-s.runDone:
		return errInformersEnded
	case <-ctx.Done():
		return nil
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	s.mu.Lock()
	if s.writeCtx != nil {
		s.mu.Unlock()
		return errWriteLoopsRunning
	}
	s.writeCtx = ctx
	trackers := make([]*Tracker, 0, len(s.trackers))
	for _, t := range s.trackers {
		trackers = append(trackers, t)
	}
	s.mu.Unlock()
	defer func() {
		// Ending ctx under mu stops endTracking from starting a recheck
		// (it adds to writeWG only under mu while ctx is live), so the Wait
		// below races no Add. writeCtx stays set until this run has fully
		// stopped, so another RunWriteLoops can't start in between and have
		// its count cleared, or its rechecks waited on, by this one.
		s.mu.Lock()
		cancel()
		s.mu.Unlock()
		s.writeWG.Wait()
		// What the release loop counted outside the caches is stale once it
		// stops: only the process that runs the write loops reports it.
		for _, t := range s.trackerList() {
			t.clearUncached()
		}
		s.mu.Lock()
		s.writeCtx = nil
		s.mu.Unlock()
	}()
	s.rescan()
	var wg sync.WaitGroup
	// The recheck outside the caches (§10.5) runs beside the loops: it
	// lists live, and the batches it finds join the queue as it goes.
	wg.Add(1)
	go func() {
		defer wg.Done()
		s.recheckUncached(ctx, trackers)
	}()
	for range releaseWorkers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.releaseWorker(ctx)
		}()
	}
	wg.Add(4)
	go func() {
		defer wg.Done()
		s.runJanitor(ctx)
	}()
	go func() {
		defer wg.Done()
		s.runIndexes(ctx)
	}()
	go func() {
		defer wg.Done()
		s.runDisown(ctx)
	}()
	go func() {
		defer wg.Done()
		s.runProber(ctx)
	}()

	var err error
	select {
	case <-ctx.Done():
	case <-s.runDone:
		err = errInformersEnded
	}
	cancel()
	wg.Wait()
	return err
}

// rescan enqueues, from every protected cache, the batch of each (K, N)
// finalizer of a tracked kind, and the objects the janitor would clean.
func (s *Shared) rescan() {
	for _, p := range s.allProtected() {
		trackers := p.snapshot()
		for _, obj := range p.inf.GetStore().List() {
			m, ok := metadataOf(obj)
			if !ok {
				continue
			}
			id := ID{Group: p.gk.Group, Kind: p.gk.Kind, Namespace: m.Namespace, Name: m.Name}
			for _, t := range trackers {
				for n := range holds(t, m) {
					s.queue.Add(batchKey{t.gk, n}, id)
				}
			}
			if s.needsJanitor(m) {
				s.janitor.Add(id)
			}
		}
	}
}

// releaseWorker takes batches until ctx ends or the queue shuts down.
func (s *Shared) releaseWorker(ctx context.Context) {
	for {
		k, ids, ok := s.queue.Get(ctx)
		if !ok {
			return
		}
		s.releaseBatch(ctx, k, ids)
		s.queue.Done(k)
	}
}

// releaseBatch runs one batch (§6.2 R0 to R3), and requeues its objects as its
// result says: what it decided at the time it asks for, as at a grace's end;
// what it couldn't, and only that, with backoff, or at once, for the next
// write loops, when ctx ended under it. A namespace's list that fails is
// retried so too, and doesn't hold up what there is. A batch that fillAll marks, from an Add with no objects, also covers
// every object of its namespace that holds K's finalizer for N, which
// heldIn lists, beside the objects added to it since. A batch for a kind
// that a Track is adding waits for that Track: it
// goes back to the queue if the Track succeeds (Shared.endTracking, the
// review of #114). A batch for a kind nobody tracks or is tracking is
// dropped: a Track after start that failed and rolled back may have left
// batches and timers for its kind, from replaying a shared cache (the
// re-review of #109).
func (s *Shared) releaseBatch(ctx context.Context, k batchKey, ids []ID) {
	t := s.trackerOrPark(k, ids)
	if t == nil {
		s.queue.Settle(k, ids...) // dropped, or parked: nothing to remember
		return
	}
	fill := len(ids) == 0 || slices.Contains(ids, fillAll)
	failed := map[ID]bool{} // what is retried with backoff
	var errs []error
	if fill {
		ids = slices.DeleteFunc(slices.Clone(ids), func(id ID) bool { return id == fillAll })
		held, err := s.heldIn(ctx, t, k.Namespace)
		if err != nil {
			errs = append(errs, err)
			failed[fillAll] = true
		}
		ids = SortedIDs(append(ids, held...))
		ids = slices.Compact(ids)
	}
	res, err := t.proto.release(ctx, k.Namespace, ids)
	if f := s.hooks.released; f != nil {
		f(&res)
	}
	if err != nil {
		errs = append(errs, err)
	}
	for _, id := range res.Failed {
		failed[id] = true
	}
	s.reportKept(t, k, res.Kept)
	if t.idx != nil {
		// The prune after R3, in one write per batch (§10.6.2, L1). One that
		// fails retries its objects, which R1 finds bare again.
		bare := t.idx.prunable(res.Bare)
		if err := t.idx.prune(ctx, bare); err != nil {
			errs = append(errs, err)
			for _, id := range bare {
				failed[id] = true
			}
		}
	}
	decided := slices.DeleteFunc(slices.Clone(ids), func(id ID) bool { return failed[id] })
	t.keptUncached(decided, res.Kept)
	if fill && !failed[fillAll] {
		s.queue.Settle(k, fillAll)
	}
	s.queue.Settle(k, decided...)
	at := map[time.Time][]ID{} // what asked to be looked at again, by when
	for _, a := range res.Asked {
		at[a.At] = append(at[a.At], a.ID)
	}
	for when, asked := range at {
		s.queue.AddAt(k, when, asked...)
	}
	if len(errs) > 0 {
		s.retryBatch(ctx, t, k, SortedIDs(keysOf(failed)), errors.Join(errs...))
	}
}

// retryBatch requeues objects ids of batch k after err: at once, for the next
// write loops, when ctx ended under it; otherwise each with its own backoff. A
// failure of none, as of a holder namespace that can't be one, is only said: an
// Add of none would ask for the whole namespace.
func (s *Shared) retryBatch(ctx context.Context, t *Tracker, k batchKey, ids []ID, err error) {
	if ctx.Err() != nil {
		if len(ids) > 0 {
			s.queue.Add(k, ids...)
		}
		return
	}
	retryIn := "as scheduled before"
	if len(ids) == 0 {
		retryIn = "never"
	} else if d := s.queue.Fail(k, ids...); d > 0 {
		retryIn = d.String()
	}
	t.warn("credential-use: a release batch failed; what failed of it is retried with backoff",
		"kind", t.gk.String(), "namespace", k.Namespace, "objects", len(ids), "retryIn", retryIn, "error", errorSample(err))
}

// maxLoggedErrors and maxLoggedBytes bound what a log line has of a joined
// error, which has a line for each object that failed.
const (
	maxLoggedErrors = 3
	maxLoggedBytes  = 2000
)

// errorSample is err's first few lines, and how many more it has.
func errorSample(err error) string {
	lines := strings.SplitN(err.Error(), "\n", maxLoggedErrors+1)
	var more string
	if len(lines) > maxLoggedErrors {
		more = fmt.Sprintf("\nand %d more", 1+strings.Count(lines[maxLoggedErrors], "\n"))
		lines = lines[:maxLoggedErrors]
	}
	text := strings.Join(lines, "\n")
	if len(text) > maxLoggedBytes {
		text = strings.ToValidUTF8(text[:maxLoggedBytes], "") + "..."
	}
	return text + more
}

// heldIn returns the protected objects that carry t's finalizer for holder
// namespace n: what a batch added with no objects covers. They are read
// from the caches, the per-name ones included, and live in a namespace the
// caches don't cover (§10.5). On an error it still returns what it could read.
func (s *Shared) heldIn(ctx context.Context, t *Tracker, n string) ([]ID, error) {
	var out []ID
	var err error
	if !s.namespaceCached(n) {
		out, err = s.liveHeldIn(ctx, t, n)
	}
	for _, p := range s.allProtected() {
		for _, obj := range p.inf.GetStore().List() {
			m, ok := metadataOf(obj)
			if !ok {
				continue
			}
			if _, held := holds(t, m)[n]; held {
				out = append(out, ID{Group: p.gk.Group, Kind: p.gk.Kind, Namespace: m.Namespace, Name: m.Name})
			}
		}
	}
	return SortedIDs(out), err
}

// The events of §12 that the release loop records: WaitingForCredentialUsers
// on a protected object kept for its users, and CredentialDeleting on each
// live user of one.
const (
	EventWaitingForUsers   = "WaitingForCredentialUsers"
	EventProtectedDeleting = "CredentialDeleting"
)

// reportKept records §12's events for the objects being deleted that a
// batch kept: WaitingForCredentialUsers, Normal, on the object, naming the
// kind, the namespace, the users' count and the deleting users; and
// CredentialDeleting, Warning, on each live user, naming the object and why
// it is kept. The event sink records an event again only when its text, and
// so the state it describes, changes, or after its throttle (§12: "once per
// state change"). The waiting gauge counts from the caches instead, so that
// every replica reports it (Tracker.setCounts).
func (s *Shared) reportKept(t *Tracker, k batchKey, kept []Kept) {
	for _, kp := range kept {
		what := fmt.Sprintf("%s %s", kp.P.Kind, nsName(kp.P.Namespace, kp.P.Name))
		note := fmt.Sprintf("waiting for %d user(s) of kind %s in namespace %s: %s", len(kp.Users), kindLabel(t.gk), k.Namespace, kp.Why)
		if kp.Why == KeptGrace && !kp.GraceEnd.IsZero() {
			note += " until " + kp.GraceEnd.UTC().Format(time.RFC3339)
		}
		if len(kp.DeletingUsers) > 0 {
			note += "; being deleted: " + joinNames(kp.DeletingUsers)
		}
		s.events.record(s.events.regarding(kp.P), "Normal", EventWaitingForUsers, note)
		t.sinkInfo("credential-use: a protected object being deleted is kept for its users",
			"object", what, "kind", t.gk.String(), "namespace", k.Namespace, "users", len(kp.Users), "why", string(kp.Why))

		deleting := map[types.NamespacedName]bool{}
		for _, d := range kp.DeletingUsers {
			deleting[d] = true
		}
		for _, u := range kp.Users {
			if deleting[u] {
				continue
			}
			holder := ID{Group: t.gk.Group, Kind: t.gk.Kind, Namespace: u.Namespace, Name: u.Name}
			s.events.record(s.events.regarding(holder), "Warning", EventProtectedDeleting,
				fmt.Sprintf("%s is being deleted and is kept for this object's kind: %s", what, kp.Why))
		}
	}
}

func nsName(ns, name string) string {
	if ns == "" {
		return name
	}
	return ns + "/" + name
}

func joinNames(nns []types.NamespacedName) string {
	parts := make([]string, len(nns))
	for i, n := range nns {
		parts[i] = nsName(n.Namespace, n.Name)
	}
	return strings.Join(parts, ", ")
}
