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
	// stopDrainBound bounds the drain, as the write loops stop, of the live
	// checks an index holds in memory (§10.6.3, decided 2026-11-25): the
	// 10s an A2 in flight is given after a stop (a2Grace), for what else a
	// stop owes the index.
	stopDrainBound = a2Grace
	// drainPause is the wait after the drain's first round, while some of
	// its checks fail; it doubles at each round, to drainPauseMax, so that a
	// failure that won't clear takes few turns from the acquirers still
	// ending.
	drainPause    = 200 * time.Millisecond
	drainPauseMax = 2 * time.Second
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
// starts. When RunInformers ends, the loops end with it, and it returns once
// they have, and its drain after them, which waits for ctx's end, and for the
// acquirer calls begun before it, within 25s of the loops' end (§7.2,
// §10.6.3).
//
// The rescan enqueues the batch of every (K, N) finalizer on every cached
// protected object, and every object whose labels the janitor would clean.
// Beside it, when SharedOptions.Namespaces restricts the caches, one live
// list per protected kind finds the held objects outside them and enqueues
// their batches too (§10.5); and each index is read live, with a GET of
// each object it names, for the PerName kinds (§10.6.5). If the queue dropped
// names while no write loops ran, one more list for each LabelScoped kind, of
// every object, finds what they were for (collapsedPart).
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

	stopping := ctx // its end is the manager's stop of the leader's runnables
	var stopMu sync.Mutex
	var stoppedAt, endedAt time.Time // stopping's end, and the loops'
	unhook := context.AfterFunc(stopping, func() {
		stopMu.Lock()
		stoppedAt = time.Now()
		stopMu.Unlock()
	})
	defer unhook()
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
		t.startQuiet()
	}
	s.mu.Unlock()
	s.queue.setDraining(true)
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
		// Nothing of the loops runs any more. What is queued dies with the
		// process unless it is made now, and the acquirers still ending may
		// queue more (§10.6.3).
		stopMu.Lock()
		if endedAt.IsZero() {
			endedAt = time.Now()
		}
		ended := endedAt
		stopMu.Unlock()
		s.drainIndexes(stopping, ended, func() time.Time {
			stopMu.Lock()
			defer stopMu.Unlock()
			return stoppedAt
		})
		// What the release loop counted outside the caches is stale once it
		// stops: only the process that runs the write loops reports it.
		for _, t := range s.trackerList() {
			t.clearUncached()
		}
		s.waits.clear()
		s.queue.setDraining(false)
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
	stopMu.Lock()
	endedAt = time.Now()
	stopMu.Unlock()
	cancel()
	wg.Wait()
	return err
}

// drainIndexes makes, as the write loops stop, the live checks every index
// holds in memory (§10.6.3, decided 2026-11-25): the re-adds a prune couldn't
// write, the checks an acquirer's line check queued when it failed, the checks
// of lines someone else dropped, and the late checks of A2s of unknown
// outcome whose margin is over, whose batches it queues too. Each reads its
// object live and writes its line back if it carries the kind's prefix, or
// can't be read, as the write loops would have.
//
// The acquirers may still queue some: a manager stops the informers first,
// whose end ends the loops, and only then the controllers, whose calls go on
// up to callGrace after that (§7.2). So the drain goes on while stopping
// lives, or a call begun before it ended (stoppedAt) is in flight, and
// stopDrainBound after the last moment one of them was so, or after its own
// start; at most callGrace and stopDrainBound after the loops ended, when
// stopping did or RunInformers did, whichever was first, so that a prune's
// write that ran on after that doesn't push the stop past a manager's 30s. A
// call begun after the stop isn't waited for: its controller wasn't stopped
// with the loops. What the drain leaves is logged, with how many: the checks
// left undone, the late holds whose margin isn't over, and the calls still in
// flight, whose checks may be lost, beside the prunes put off.
func (s *Shared) drainIndexes(stopping context.Context, ended time.Time, stoppedAt func() time.Time) {
	d, grace := stopDrainBound, callGrace
	if b := s.hooks.drainBound; b != nil {
		d = *b
	}
	if g := s.hooks.callGrace; g != nil {
		grace = *g
	}
	capped, cancel := context.WithDeadline(context.WithoutCancel(stopping), ended.Add(grace+d))
	defer cancel()
	ctx, quiet := context.WithCancel(capped)
	defer quiet()
	// waited are the calls in flight the drain waits for: those begun
	// before stopping ended, or all while it hasn't.
	waited := func() int {
		at := stoppedAt()
		if at.IsZero() {
			at = time.Now()
		}
		return s.calls.before(at)
	}
	busy := func() bool { return stopping.Err() == nil || waited() > 0 }
	go func() {
		// Ends the drain d after it was last busy.
		last := time.Now()
		for {
			if busy() {
				last = time.Now()
			}
			if time.Since(last) >= d {
				quiet()
				return
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(min(d/10, 100*time.Millisecond)):
			}
		}
	}()
	var wg sync.WaitGroup
	for _, t := range s.trackerList() {
		if t.idx == nil {
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			left, late, deferred := t.idx.drain(ctx, busy, func(id ID, n string) { s.queue.Add(batchKey{t.gk, n}, id) })
			if calls := waited(); left+late+calls > 0 {
				t.warn("credential-use: the write loops stopped with live checks of the index left undone: their lines may be missing until their holders' next passes (§10.6.3)",
					"kind", t.gk.String(), "index", t.idx.ref(), "left", left, "lateHoldsNotDue", late, "callsInFlight", calls, "prunesPutOff", deferred, "bound", d.String())
			}
		}()
	}
	wg.Wait()
}

// callsInFlight are the acquirer calls of a Shared that are running, an
// Acquire or a BeforeEstablish, from its detach to its stop (§7.2), by when
// each began: a drain waits for those begun before the stop.
type callsInFlight struct {
	mu    sync.Mutex
	next  int
	began map[int]time.Time
}

// begin counts one call, until end, which may be called more than once.
func (c *callsInFlight) begin() (end func()) {
	c.mu.Lock()
	if c.began == nil {
		c.began = map[int]time.Time{}
	}
	id := c.next
	c.next++
	c.began[id] = time.Now()
	c.mu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			c.mu.Lock()
			delete(c.began, id)
			c.mu.Unlock()
		})
	}
}

// before counts the calls in flight that began before t.
func (c *callsInFlight) before(t time.Time) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, at := range c.began {
		if at.Before(t) {
			n++
		}
	}
	return n
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
					s.queue.AddHeld(batchKey{t.gk, n}, id)
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

// releaseBatch runs one batch (§6.2 R0 to R3), a turn of its key, and requeues
// its objects as its result says: those that asked, each at its time, as at a
// grace's end; those whose release must wait (§6.4), each when its wait ends;
// those it couldn't decide, and only those, with backoff, or at once, for the next
// write loops, when ctx ended under it; and those it didn't get to where they were
// in the queue, behind the other keys' turns, with nothing of theirs settled,
// failed or waited for. A namespace's list that fails is retried so too, and
// doesn't hold up what there is.
//
// A batch that fillAll marks, from an Add with no objects, also covers every
// object of its namespace that holds K's finalizer for N, which heldIn lists,
// beside the objects added to it since.
//
// A batch for a kind that a Track is adding waits for that Track: it goes back
// to the queue if the Track succeeds (Shared.endTracking, the review of #114).
// A batch for a kind nobody tracks or is tracking is dropped: a Track after
// start that failed and rolled back may have left batches and timers for its
// kind, from replaying a shared cache (the re-review of #109).
func (s *Shared) releaseBatch(ctx context.Context, k batchKey, ids []ID) {
	t := s.trackerOrPark(k, ids)
	if t == nil {
		s.queue.Settle(k, ids...) // dropped, or parked: nothing to remember
		s.waits.settle(k, ids...)
		return
	}
	fill := len(ids) == 0 || slices.Contains(ids, fillAll)
	failed := map[ID]bool{} // what is retried with backoff
	var errs []error
	// fail notes err, and what it left undecided: of, or, if err comes with no
	// object, all there are, so that one that forgets to say loses none.
	fail := func(err error, of ...ID) {
		errs = append(errs, err)
		if len(of) == 0 {
			of = ids
		}
		for _, id := range of {
			failed[id] = true
		}
	}
	if fill {
		held, err := s.heldIn(ctx, t, k.Namespace)
		if err != nil {
			fail(err, fillAll)
		}
		s.queue.Known(k, held...)
		ids = expand(ids, held)
	}
	res, err := t.proto.release(ctx, k.Namespace, ids)
	if f := s.hooks.released; f != nil {
		f(&res)
	}
	s.queue.Known(k, res.Known...)
	s.queue.PutBack(k, res.Rest...)
	rest := map[ID]bool{}
	for _, id := range res.Rest {
		rest[id] = true
	}
	ids = slices.DeleteFunc(slices.Clone(ids), func(id ID) bool { return rest[id] }) // from here on, the turn's
	if err == nil && len(res.Failed) > 0 {
		err = errors.New("credential-use: a batch left objects undecided, and no error says why")
	}
	if err != nil {
		fail(err, res.Failed...)
	}
	s.wait(k, res.Waiting)
	waiting := map[ID]bool{}
	for _, o := range res.Waiting {
		waiting[o.ID] = true
	}
	undecided := func(id ID) bool { return failed[id] || waiting[id] }
	// The next release of what the batch decided waits again, though its prune,
	// below, fails.
	s.waits.settle(k, slices.DeleteFunc(slices.Clone(ids), undecided)...)
	s.reportKept(t, k, res.Kept)
	if t.idx != nil {
		// The prune after R3, many lines in one write (§10.6.2, L1). One that
		// fails retries its objects, which R1 finds bare again.
		bare := t.idx.prunable(res.Bare)
		if err := t.idx.prune(ctx, bare); err != nil {
			fail(err, bare...)
		}
	}
	decided := slices.DeleteFunc(slices.Clone(ids), undecided)
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
		s.retryBatch(ctx, t, k, SortedIDs(keysOf(failed)), joined(errs))
	}
}

// expand is ids with the namespace's objects, held, where fillAll stands among
// them, each once, where it first is. None is the whole namespace.
func expand(ids, held []ID) []ID {
	if len(ids) == 0 {
		return held
	}
	out, seen := make([]ID, 0, len(ids)+len(held)), map[ID]bool{}
	add := func(of ...ID) {
		for _, id := range of {
			if !seen[id] {
				seen[id] = true
				out = append(out, id)
			}
		}
	}
	for _, id := range ids {
		if id == fillAll {
			add(held...)
		} else {
			add(id)
		}
	}
	return out
}

// retryBatch requeues objects ids of batch k after err: at once, for the next
// write loops, when ctx ended under it; otherwise each with its own backoff. A
// failed batch of none, as of a holder namespace that can't be one, is only
// said: an Add of none would ask for the whole namespace.
func (s *Shared) retryBatch(ctx context.Context, t *Tracker, k batchKey, ids []ID, err error) {
	if ctx.Err() != nil {
		if len(ids) > 0 {
			s.queue.PutBack(k, ids...)
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

// batchErrors are the errors of a batch, one for each object that failed at most,
// joined as errors.Join does; a log line builds the texts of a few of them alone.
type batchErrors []error

// joined is errs as one error, or nil.
func joined(errs []error) error {
	if len(errs) == 0 {
		return nil
	}
	return batchErrors(errs)
}

func (e batchErrors) Error() string {
	texts := make([]string, len(e))
	for i, err := range e {
		texts[i] = err.Error()
	}
	return strings.Join(texts, "\n")
}

func (e batchErrors) Unwrap() []error { return e }

// leaves gives do each of the errors that err joins, or err.
func leaves(err error, do func(error)) {
	if es, ok := err.(batchErrors); ok {
		for _, e := range es {
			leaves(e, do)
		}
		return
	}
	do(err)
}

// errorSample is err's first few errors' lines, and how many more it has.
func errorSample(err error) string {
	var lines []string
	n := 0
	leaves(err, func(e error) {
		if n < maxLoggedErrors {
			lines = append(lines, strings.Split(e.Error(), "\n")...)
		}
		n++
	})
	var more string
	if rest := n - min(n, maxLoggedErrors) + len(lines) - min(len(lines), maxLoggedErrors); rest > 0 {
		more = fmt.Sprintf("\nand %d more", rest)
		lines = lines[:min(len(lines), maxLoggedErrors)]
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
