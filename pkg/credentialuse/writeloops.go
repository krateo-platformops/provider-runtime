package credentialuse

import (
	"context"
	"errors"
	"fmt"
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
	// releaseBackoffBase and releaseBackoffMax bound the backoff of a batch
	// that failed (§6.2): it doubles from the base, per (K, N), and resets
	// when the batch succeeds.
	releaseBackoffBase = time.Second
	releaseBackoffMax  = 5 * time.Minute
)

// errInformersEnded is RunWriteLoops' error when RunInformers ends first.
var errInformersEnded = errors.New("credential-use: the informers have stopped")

// RunWriteLoops runs the write loops until ctx is done: the release loop,
// the missing-ref prober and the label janitor. It needs leader election
// (§7.5, IP4), and it may start before, with or after RunInformers: it waits
// for the informers' first sync, then rescans the protected caches and
// starts. It returns at once when RunInformers ends.
//
// The rescan enqueues the batch of every (K, N) finalizer on every cached
// protected object, and every object whose labels the janitor would clean.
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
	s.rescan()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var wg sync.WaitGroup
	for range releaseWorkers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.releaseWorker(ctx)
		}()
	}
	wg.Add(2)
	go func() {
		defer wg.Done()
		s.runJanitor(ctx)
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
	for _, p := range s.protectedInformers() {
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

// releaseBatch runs one batch (§6.2 R0 to R3), and requeues it as its
// result says: at the time it asks for, as at a grace's end; with backoff
// after an error; at once, for the next write loops, when ctx ended under
// it. A batch for a kind that a Track is adding waits for that Track: it
// goes back to the queue if the Track succeeds (Shared.endTracking, the
// review of #114). A batch for a kind nobody tracks or is tracking is
// dropped: a Track after start that failed and rolled back may have left
// batches and timers for its kind, from replaying a shared cache (the
// re-review of #109).
func (s *Shared) releaseBatch(ctx context.Context, k batchKey, ids []ID) {
	t := s.trackerOrPark(k, ids)
	if t == nil {
		return
	}
	if len(ids) == 0 {
		ids = s.heldIn(t, k.Namespace)
		if len(ids) == 0 {
			return
		}
	}
	res, err := t.proto.release(ctx, k.Namespace, ids)
	s.reportKept(t, k, res.Kept)
	if err != nil {
		if ctx.Err() != nil {
			s.queue.Add(k, ids...)
			return
		}
		d := s.backoff.fail(k)
		t.warn("credential-use: a release batch failed; it is retried with backoff",
			"kind", t.gk.String(), "namespace", k.Namespace, "retryIn", d.String(), "error", err.Error())
		s.queue.AddAfter(k, d, ids...)
		return
	}
	s.backoff.reset(k)
	if !res.RequeueAt.IsZero() {
		s.queue.AddAfter(k, res.RequeueAt.Sub(s.clock.Now()), ids...)
	}
}

// heldIn returns the cached protected objects that carry t's finalizer for
// holder namespace n: what a batch added with no objects covers.
func (s *Shared) heldIn(t *Tracker, n string) []ID {
	var out []ID
	for _, p := range s.protectedInformers() {
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
	return SortedIDs(out)
}

// batchBackoff is the release loop's per-(K, N) failure backoff.
type batchBackoff struct {
	mu       sync.Mutex
	failures map[batchKey]int
}

func (b *batchBackoff) fail(k batchKey) time.Duration {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.failures == nil {
		b.failures = map[batchKey]int{}
	}
	n := b.failures[k]
	b.failures[k] = n + 1
	d := releaseBackoffBase
	for range n {
		d *= 2
		if d >= releaseBackoffMax {
			return releaseBackoffMax
		}
	}
	return d
}

func (b *batchBackoff) reset(k batchKey) {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.failures, k)
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
