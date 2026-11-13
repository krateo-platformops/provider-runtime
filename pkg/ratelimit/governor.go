package ratelimit

import (
	"context"
	cryptorand "crypto/rand"
	"fmt"
	"io"
	"math/big"
	"slices"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/krateo-platformops/provider-runtime/pkg/errors"
	"github.com/krateo-platformops/provider-runtime/pkg/logging"
)

// DefaultRecoveryInterval is how long a key, or the local capacity after a
// cut, must go clean before its capacity rises by one.
const DefaultRecoveryInterval = 30 * time.Second

// DefaultMaxHold is the longest the governor holds a key after a refusal when
// Options.MaxHold is unset. A server's Retry-After or reset time beyond it is
// cut to it, so a broken or hostile server can't freeze every object on a key,
// deletions included, for a day or a year.
const DefaultMaxHold = time.Hour

// capacityFloor is 1, never 0. One call in flight is how a throttled key finds
// out the limit has lifted, and how a saturated controller still finishes
// deletions.
const capacityFloor = 1

// minCapacityJitter is the lower bound of the at-capacity requeue delay.
const minCapacityJitter = time.Second

// DeferReason says why a call was not let through.
type DeferReason string

const (
	// DeferThrottled: the server refused a call for this key and named a
	// deadline. Deferral.Until is that deadline.
	DeferThrottled DeferReason = "Throttled"
	// DeferAtCapacity: the key has as many calls in flight as it allows. There
	// is no server deadline.
	DeferAtCapacity DeferReason = "AtCapacity"
)

// Cause is the value of the cause attribute on the deferrals metric: throttled
// or capacity. The controller's own saturation defers nothing: it holds work
// in the queue (NewGatedQueue), so it has no cause here.
func (r DeferReason) Cause() string {
	switch r {
	case DeferThrottled:
		return "throttled"
	case DeferAtCapacity:
		return "capacity"
	}
	return string(r)
}

// A Deferral says a call must not be made now, why, and when to try again.
type Deferral struct {
	// Key is the key that stopped the call.
	Key string
	// Reason says why.
	Reason DeferReason
	// Until is the server's own deadline, cut to MaxHold after the refusal
	// when the server asked for longer. It is set only for DeferThrottled. The
	// governor never invents a deadline: it only shortens one past MaxHold.
	Until time.Time
	// RequeueAfter is how long the caller waits before it tries again. For
	// DeferThrottled it is the time left until Until, never less. Otherwise it
	// is a random delay between 1s and the RecoveryInterval.
	RequeueAfter time.Duration
	// Capacity is the key's capacity that was full, for DeferAtCapacity.
	Capacity int
}

// Message is the Synced condition message for a deferred pass. It names the
// key or the cause, and the deadline as an RFC 3339 time when there is one, or
// says that it is waiting for capacity.
//
// The throttled wording is the same as unstructured-runtime's, so two
// controllers deferring against the same endpoint say it in the same words.
func (d *Deferral) Message() string {
	switch d.Reason {
	case DeferThrottled:
		return fmt.Sprintf("reconciliation deferred: %s is rate-limited by the server; next attempt %s",
			d.Key, formatDeadline(d.Until))
	}
	return fmt.Sprintf("reconciliation deferred: %s is at its concurrency limit of %d call(s); waiting for capacity; no server deadline was given",
		d.Key, d.Capacity)
}

// formatDeadline formats t in RFC 3339 rounded up to the whole second, so the
// time a reader sees is never earlier than the server's deadline.
func formatDeadline(t time.Time) string {
	r := t.Truncate(time.Second)
	if r.Before(t) {
		r = r.Add(time.Second)
	}
	return r.UTC().Format(time.RFC3339)
}

// MetricsRecorder records the governor's metrics. *telemetry.Metrics
// implements it.
type MetricsRecorder interface {
	// IncRateLimitDeferral counts one deferral of a call for key.
	IncRateLimitDeferral(ctx context.Context, key string, reason DeferReason)
	// IncRateLimitSignal counts one throttled or advisory signal for key.
	IncRateLimitSignal(ctx context.Context, key string, kind SignalKind)
	// IncRateLimitHoldClamped counts one throttled signal for key whose
	// deadline was past MaxHold and was cut to it.
	IncRateLimitHoldClamped(ctx context.Context, key string)
	// ObserveRateLimit registers snapshot as the source of the governor's
	// gauges and observable counters: per-key capacity and seconds to reset,
	// local capacity, each saturation input, each controller's use of the
	// local capacity, the cuts, and the capacity model's measurements.
	ObserveRateLimit(snapshot func() Snapshot)
}

// Options configures a Governor.
type Options struct {
	// MaxConcurrentReconciles is each controller's worker count (a provider's
	// --max-reconcile-rate). The ceiling every capacity recovers to, per key
	// and for the process-wide local capacity, is this times the number of
	// reconcilers sharing the Governor (each reconciler.WithRateLimitGovernor
	// registers one), that is the process's total workers: enabling the
	// governor never holds a healthy endpoint below what the workers could do
	// today. Required, at least 1; controller.Options.SetupRateLimitGovernor
	// fills it from controller.Options.MaxConcurrentReconciles.
	MaxConcurrentReconciles int

	// IgnoreSignals makes Observe drop every Signal, so only the saturation
	// input gates calls. controller.Options.SetupRateLimitGovernor sets it
	// when saturation is enabled and the rate-limit input is not. Default
	// false: signals are honoured.
	IgnoreSignals bool

	// RecoveryInterval is how long a key, or the local capacity after a cut,
	// must go clean before its capacity rises by one, up to the ceiling. It is
	// also the upper bound of the at-capacity requeue delay. Defaults to 30s.
	RecoveryInterval time.Duration

	// MaxHold is the longest a Throttled signal holds its key: a deadline past
	// now+MaxHold is cut to now+MaxHold. After the cut hold one call goes
	// through, and if the server still refuses it the key is held again. nil
	// means DefaultMaxHold (1h). A pointer to 0 means no cap: the server's
	// deadline holds in full, however far away. A negative value is an error.
	//
	// It is a pointer because unset and 0 differ: unset is the 1h cap, and 0
	// turns the cap off. The first cut for a key is logged at Info, and not
	// again until the key has gone a whole RecoveryInterval clean. Every cut is
	// counted (MetricsRecorder.IncRateLimitHoldClamped).
	MaxHold *time.Duration

	// Saturation configures the local input (design §7). Off unless Enabled.
	Saturation SaturationOptions

	// Logger logs disabled saturation inputs (once each) and capacity
	// changes. Defaults to a no-op logger.
	Logger logging.Logger

	// Metrics records deferrals, signals and the gauges. Optional.
	Metrics MetricsRecorder
}

type keyState struct {
	capacity int
	inFlight int
	// holdUntil is the server's deadline, cut to MaxHold; the gate is closed
	// before it.
	holdUntil time.Time
	// lastReduce is when a throttled or advisory signal last cut capacity.
	lastReduce time.Time
	// lastRaise is when capacity last rose by one.
	lastRaise time.Time
	// clampLogged is set once a cut to MaxHold has been logged for the key.
	// The first throttled signal after the key has gone a whole
	// RecoveryInterval clean clears it, so a server that keeps asking for too
	// long is logged once, not once per hold.
	clampLogged bool
}

// localState is the process-wide local capacity the saturation sampler
// publishes. It is replaced, never mutated, so readers take it with one atomic
// load.
type localState struct {
	capacity int
	// cause names the inputs that made the last sample saturated. A clean
	// sample clears it.
	cause string
}

// A Governor gates external calls per endpoint key, and across all keys by the
// controller's own saturation. Create it with New, share one per process, pass
// it to each reconciler with reconciler.WithRateLimitGovernor, and add it to
// the manager (mgr.Add) so its saturation sampler runs.
//
// A Governor is safe for concurrent use.
type Governor struct {
	perController int
	ignoreSignals bool
	recovery      time.Duration
	// maxHold caps a hold; 0 is no cap.
	maxHold time.Duration
	log     logging.Logger
	metrics MetricsRecorder

	// now and jitter are replaced in tests.
	now    func() time.Time
	jitter func(hi time.Duration) time.Duration

	mu   sync.Mutex
	keys map[string]*keyState
	// localInFlight counts the local slots gated queues hold: one per reconcile
	// a worker popped and hasn't called Done for.
	localInFlight int
	// slotTime is the time local slots have been held, summed over the slots,
	// up to slotTimeAt: localInFlight added up over time. See
	// advanceSlotTimeLocked.
	slotTime   time.Duration
	slotTimeAt time.Time
	// queues are the gated queues waiting on local capacity, and nextQueue is
	// where the next round-robin grant starts.
	queues    []gatedQueueRef
	nextQueue int
	// reconcilers is how many reconcilers registered; see ceilingLocked.
	reconcilers int
	// gatedControllers is how many controllers' queues ForControllerRuntime
	// gated; see Start.
	gatedControllers int

	// arrivals counts the items that became ready to run in any gated queue,
	// and completions the reconciles that gave their slot back. The queues add
	// to them under their own locks, without mu.
	arrivals, completions atomic.Int64

	// local is written under mu and read without it.
	local atomic.Pointer[localState]
	// sat is nil when saturation is disabled.
	sat *saturation
}

// runningSamplers counts the saturation samplers running in this process. Two
// Governors with saturation each allow their full capacity on the same cgroup.
var runningSamplers atomic.Int32

// New returns a Governor. It returns an error if MaxConcurrentReconciles is
// below 1 or a saturation setting is out of range: a governor that silently
// capped every key at 1 would be a slow-down nobody configured.
func New(o Options) (*Governor, error) {
	if o.MaxConcurrentReconciles < 1 {
		return nil, errors.Errorf("ratelimit: MaxConcurrentReconciles must be at least 1, got %d", o.MaxConcurrentReconciles)
	}
	if o.RecoveryInterval < 0 {
		return nil, errors.Errorf("ratelimit: RecoveryInterval must not be negative, got %s", o.RecoveryInterval)
	}
	if o.RecoveryInterval == 0 {
		o.RecoveryInterval = DefaultRecoveryInterval
	}
	maxHold := DefaultMaxHold
	if o.MaxHold != nil {
		maxHold = *o.MaxHold
	}
	if maxHold < 0 {
		return nil, errors.Errorf("ratelimit: MaxHold must not be negative, got %s", maxHold)
	}
	if o.Logger == nil {
		o.Logger = logging.NewNopLogger()
	}

	g := &Governor{
		perController: o.MaxConcurrentReconciles,
		ignoreSignals: o.IgnoreSignals,
		recovery:      o.RecoveryInterval,
		maxHold:       maxHold,
		log:           o.Logger,
		metrics:       o.Metrics,
		now:           time.Now,
		keys:          map[string]*keyState{},
	}
	g.jitter = func(hi time.Duration) time.Duration { return jitterBetween(cryptorand.Reader, minCapacityJitter, hi) }

	// Without saturation, local capacity is the ceiling and stays there. With
	// it, local capacity starts at 1 and ramps up (see saturation.step).
	g.local.Store(&localState{capacity: g.perController})
	if o.Saturation.Enabled {
		sat, err := newSaturation(o.Saturation, o.Logger)
		if err != nil {
			return nil, err
		}
		g.sat = sat
		g.local.Store(&localState{capacity: capacityFloor})
	}

	if g.metrics != nil {
		g.metrics.ObserveRateLimit(g.Snapshot)
	}
	return g, nil
}

// jitterBetween returns a uniformly random delay in [lo, hi], read from r.
//
// It uses crypto/rand, not math/rand. The jitter isn't security-sensitive, but
// gosec flags math/rand (G404), and a suppression is an exception every later
// reviewer has to re-justify. cryptorand.Int draws uniformly by rejection
// sampling, so there is no modulo bias either.
//
// If the read fails it returns hi, the full RecoveryInterval: a shorter delay
// would bring the call back early, and early is the direction that gets a
// client banned rather than throttled. When hi is not above lo it returns hi.
func jitterBetween(r io.Reader, lo, hi time.Duration) time.Duration {
	if hi <= lo {
		return hi
	}
	n, err := cryptorand.Int(r, big.NewInt(int64(hi-lo)+1))
	if err != nil {
		return hi
	}
	return lo + time.Duration(n.Int64())
}

// ceilingLocked is the capacity every key and the local capacity recover to:
// MaxConcurrentReconciles times the registered reconcilers (at least one).
// The caller holds g.mu.
func (g *Governor) ceilingLocked() int {
	return g.perController * max(1, g.reconcilers)
}

// RegisterReconciler tells the Governor one more reconciler, running
// MaxConcurrentReconciles workers, shares it, and raises the ceiling to match.
// Key capacities still at the old ceiling move to the new one, and so does the
// local capacity when saturation is off; with saturation on, local capacity
// recovers to the new ceiling at its own pace. reconciler.
// WithRateLimitGovernor calls it; call it yourself only if you gate calls
// through Acquire outside pkg/reconciler.
func (g *Governor) RegisterReconciler() {
	g.mu.Lock()
	defer g.mu.Unlock()
	old := g.ceilingLocked()
	g.reconcilers++
	ceiling := g.ceilingLocked()
	for _, st := range g.keys {
		if st.capacity == old {
			st.capacity = ceiling
		}
	}
	if cur := g.local.Load(); g.sat == nil && cur.capacity == old {
		next := *cur
		next.capacity = ceiling
		g.local.Store(&next)
	}
}

// SaturationEnabled reports whether the local (saturation) input is on, in
// which case controller.Options.ForControllerRuntime gates each controller's
// queue on the local capacity.
func (g *Governor) SaturationEnabled() bool { return g.sat != nil }

// RegisterGatedController records that one more controller's queue is gated on
// this Governor. controller.Options.ForControllerRuntime calls it when it
// builds a gated queue's constructor, at setup, so Start can tell a reconciler
// whose controller was built without it: that reconciler would run with no
// local bound at all.
func (g *Governor) RegisterGatedController() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.gatedControllers++
}

// addQueue makes q eligible for local slots.
func (g *Governor) addQueue(q gatedQueueRef) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.queues = append(g.queues, q)
}

// removeQueue stops granting q slots: it is shutting down.
func (g *Governor) removeQueue(q gatedQueueRef) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if i := slices.Index(g.queues, q); i >= 0 {
		g.queues = slices.Delete(g.queues, i, i+1)
		if g.nextQueue > i {
			g.nextQueue--
		}
	}
}

// dispatch grants free local slots to queues that have work.
func (g *Governor) dispatch() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.dispatchLocked()
}

// releaseLocal gives back one local slot and grants it on.
// A release with no slot in flight means a queue gave back a slot twice, which
// must never happen: it is reported, not hidden, and the count stays at 0.
func (g *Governor) releaseLocal() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.localInFlight == 0 {
		reportBrokenBooks(g.log, "a local slot was released with none in flight")
	} else {
		g.advanceSlotTimeLocked(g.now())
		g.localInFlight--
	}
	g.dispatchLocked()
}

// advanceSlotTimeLocked adds to slotTime the time the slots in flight were
// held since it was last called. Calling it before every change of
// localInFlight, and at every sample, makes slotTime exact at now. The caller
// holds g.mu.
func (g *Governor) advanceSlotTimeLocked(now time.Time) {
	if !now.After(g.slotTimeAt) {
		return
	}
	if !g.slotTimeAt.IsZero() {
		g.slotTime += time.Duration(g.localInFlight) * now.Sub(g.slotTimeAt)
	}
	g.slotTimeAt = now
}

// guaranteedLocked is G, the share of the local capacity each gated queue is
// guaranteed: the capacity divided equally, at least 1. With the capacity
// under the number of queues it is 1 each, and they can't all be met. The
// caller holds g.mu.
func (g *Governor) guaranteedLocked() int {
	return max(1, g.local.Load().capacity/max(1, len(g.queues)))
}

// dispatchLocked hands free local slots, one at a time, to queues with an idle
// worker and an item to give it. A slot goes first to a queue that is owed:
// one that holds fewer slots than its guaranteed share. With none owed it goes
// to any queue with work, which is the lending: a queue may hold more than its
// share while the others don't need theirs. Both are round-robin from where
// the last grant left off. Nothing is taken back: a lent slot returns when its
// reconcile ends, and an owed queue is then first in line. Inside a queue the
// order is the queue's. The caller holds g.mu.
//
// A queue in the middle of a pop is skipped in both passes, owed or not, so a
// dispatch never waits on a queue's lock (design-local-slot-before-pop §4,
// "Containment"). A slot freed in that moment can be lent although that queue
// is owed; the queue dispatches again when its pop ends, and is first in line
// for the next slot.
func (g *Governor) dispatchLocked() {
	for g.localInFlight < g.local.Load().capacity {
		share := g.guaranteedLocked()
		granted := g.grantLocked(func(q gatedQueueRef) bool { return q.tryGrantOwed(share) }) ||
			g.grantLocked(gatedQueueRef.tryGrant)
		if !granted {
			return
		}
	}
}

// grantLocked gives one slot to the first queue, round-robin from nextQueue,
// that try grants one to, and reports whether there was one. The caller holds
// g.mu.
func (g *Governor) grantLocked(try func(gatedQueueRef) bool) bool {
	n := len(g.queues)
	for i := range n {
		idx := (g.nextQueue + i) % n
		if try(g.queues[idx]) {
			g.advanceSlotTimeLocked(g.now())
			g.localInFlight++
			g.nextQueue = (idx + 1) % n
			return true
		}
	}
	return false
}

// Ceiling reports the capacity every key and the local capacity recover to.
func (g *Governor) Ceiling() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.ceilingLocked()
}

// state returns key's state, creating it at the ceiling: the governor never
// slows a key it has heard nothing about. The caller holds g.mu.
func (g *Governor) state(key string) *keyState {
	st, ok := g.keys[key]
	if !ok {
		st = &keyState{capacity: g.ceilingLocked()}
		g.keys[key] = st
	}
	return st
}

// maybeRaiseLocked adds one to the key's capacity when it has gone a whole
// RecoveryInterval clean, counted from the later of its last cut and the end of
// its hold, and at most once per interval. The caller holds g.mu.
func (g *Governor) maybeRaiseLocked(st *keyState, now time.Time) {
	if st.capacity >= g.ceilingLocked() || !g.cleanLocked(st, now) || now.Sub(st.lastRaise) < g.recovery {
		return
	}
	st.capacity++
	st.lastRaise = now
}

// cleanLocked reports whether the key has gone a whole RecoveryInterval clean
// at now: it isn't held, and a whole RecoveryInterval has passed since the
// later of its last throttled or advisory signal and the end of its hold. The
// caller holds g.mu.
func (g *Governor) cleanLocked(st *keyState, now time.Time) bool {
	if now.Before(st.holdUntil) {
		return false
	}
	cleanSince := st.lastReduce
	if st.holdUntil.After(cleanSince) {
		cleanSince = st.holdUntil
	}
	return now.Sub(cleanSince) >= g.recovery
}

// Acquire asks to make one gated call for key. It never blocks and does no
// I/O: it either grants a slot in the key's capacity, returning the release
// func to call when the call returns, or it returns a Deferral at once. An empty
// key gates nothing. The local capacity isn't asked here: a gated queue takes
// its slot before the reconcile starts (NewGatedQueue).
//
// A blocking gate would park workers: with MaxConcurrentReconciles workers and
// one throttled key, every worker could end up waiting on that key while
// objects on healthy endpoints stop reconciling (design §5).
//
// release is nil exactly when the Deferral is not. Calling release more than
// once is safe.
func (g *Governor) Acquire(ctx context.Context, key string) (release func(), d *Deferral) {
	return g.AcquireAll(ctx, []string{key})
}

// AcquireAll is Acquire for a call that talks to several rate-limited scopes,
// such as a sync between two endpoints. The call needs a slot in every key,
// all or nothing: it either holds them all, or holds none and gets a Deferral.
// Nothing is held while deferred, so two calls that share keys can't wait on
// each other.
//
// Empty keys are ignored and repeated keys count once, so two names for the
// same scope take one slot. With no keys left it gates nothing.
//
// A hold on any key outranks a full capacity, as it does for one key. With
// several keys held the Deferral names the latest deadline: the call can't run
// before that one lifts, so waking at an earlier one would only defer it again.
func (g *Governor) AcquireAll(ctx context.Context, keys []string) (release func(), d *Deferral) {
	keys = normalizeKeys(keys)
	if len(keys) == 0 {
		return func() {}, nil
	}
	d = g.acquire(keys)
	if d != nil {
		if g.metrics != nil {
			g.metrics.IncRateLimitDeferral(ctx, d.Key, d.Reason)
		}
		return nil, d
	}
	var once sync.Once
	return func() {
		once.Do(func() {
			g.mu.Lock()
			defer g.mu.Unlock()
			for _, key := range keys {
				if st, ok := g.keys[key]; ok && st.inFlight > 0 {
					st.inFlight--
				}
			}
		})
	}, nil
}

// normalizeKeys returns keys sorted, without empty or repeated entries, in a
// new slice: the release func keeps it, and the caller still owns its own.
func normalizeKeys(keys []string) []string {
	out := slices.DeleteFunc(slices.Clone(keys), func(k string) bool { return k == "" })
	slices.Sort(out)
	return slices.Compact(out)
}

// acquire takes a slot in every key of keys, which normalizeKeys has prepared,
// or takes nothing and returns why. Every check runs before the first slot is
// taken, under one hold of g.mu, so a deferred call leaves no slot behind.
func (g *Governor) acquire(keys []string) *Deferral {
	g.mu.Lock()
	defer g.mu.Unlock()

	now := g.now()
	states := make([]*keyState, len(keys))
	var held *Deferral
	for i, key := range keys {
		st := g.state(key)
		g.maybeRaiseLocked(st, now)
		states[i] = st
		if now.Before(st.holdUntil) && (held == nil || st.holdUntil.After(held.Until)) {
			held = &Deferral{Key: key, Reason: DeferThrottled, Until: st.holdUntil, RequeueAfter: st.holdUntil.Sub(now)}
		}
	}
	if held != nil {
		return held
	}
	for i, st := range states {
		if st.inFlight >= st.capacity {
			return &Deferral{Key: keys[i], Reason: DeferAtCapacity, RequeueAfter: g.jitter(g.recovery), Capacity: st.capacity}
		}
	}
	for _, st := range states {
		st.inFlight++
	}
	return nil
}

// Observe folds a provider's signal into its key's state. The Governor is the
// Sink the reconciler installs, so Report reaches it. A signal is applied to
// its own key, even when that differs from the key the call was gated on.
//
//   - Throttled: capacity drops to 1 and the gate closes until the signal's
//     deadline, max(RetryAfter, ResetAt), cut to now+MaxHold when it is later
//     than that. A later deadline extends the hold; an earlier one never
//     shortens it. Every cut is counted, and the first is logged at Info, not
//     again until the key has gone a whole RecoveryInterval clean.
//   - Advisory: capacity halves, with a minimum of 1.
//   - Either restarts the key's clean RecoveryInterval.
//   - Anything else (a healthy quota report) changes nothing.
func (g *Governor) Observe(s Signal) {
	if s.Key == "" || g.ignoreSignals {
		return
	}
	now := g.now()
	kind := Classify(s, now)
	if kind == SignalNone {
		return
	}

	var requested, limit time.Time
	clamped, logClamp := false, false

	g.mu.Lock()
	st := g.state(s.Key)
	switch kind {
	case SignalThrottled:
		// Checked before lastReduce moves to now, which makes no key clean.
		if st.clampLogged && g.cleanLocked(st, now) {
			st.clampLogged = false
		}
		st.capacity = capacityFloor
		d := s.Deadline(now)
		if limit = now.Add(g.maxHold); g.maxHold > 0 && d.After(limit) {
			requested, clamped = d, true
			logClamp = !st.clampLogged
			st.clampLogged = true
			d = limit
		}
		if d.After(st.holdUntil) {
			st.holdUntil = d
		}
	case SignalAdvisory:
		st.capacity = max(capacityFloor, st.capacity/2)
	}
	st.lastReduce = now
	g.mu.Unlock()

	if logClamp {
		g.log.Info("Server asked to hold a rate-limit key past the maximum hold: capping the hold",
			"key", s.Key,
			"requestedDeadline", formatDeadline(requested),
			"maxHold", g.maxHold.String(),
			"holdUntil", formatDeadline(limit))
	}
	if g.metrics != nil {
		g.metrics.IncRateLimitSignal(context.Background(), s.Key, kind)
		if clamped {
			g.metrics.IncRateLimitHoldClamped(context.Background(), s.Key)
		}
	}
}

// Capacity reports key's current capacity: the most calls for it that may be
// in flight at once. It is the number operators used to tune by hand.
func (g *Governor) Capacity(key string) int {
	g.mu.Lock()
	defer g.mu.Unlock()
	st, ok := g.keys[key]
	if !ok {
		return g.ceilingLocked()
	}
	g.maybeRaiseLocked(st, g.now())
	return st.capacity
}

// LocalCapacity reports the process-wide local capacity.
func (g *Governor) LocalCapacity() int {
	return g.local.Load().capacity
}

// Start runs the saturation sampler until ctx is done, when saturation is
// enabled; otherwise it waits for ctx. It satisfies manager.Runnable, so add
// the Governor to the manager with mgr.Add. Until it runs, local capacity stays
// at 1 and every saturation input reports as disabled.
//
// It logs an error once when more reconcilers registered with the Governor than
// controllers had their queue gated: such a reconciler's controller was built
// without controller.Options.ForControllerRuntime and has no local bound. It
// warns when another Governor's sampler is already running in the process,
// because each would allow its full capacity on the same container.
func (g *Governor) Start(ctx context.Context) error {
	if g.sat == nil {
		<-ctx.Done()
		return nil
	}
	if runningSamplers.Add(1) > 1 {
		g.log.Warn("Another Governor with saturation is already running in this process: each allows its full local capacity on the same container. Share one Governor (controller.Options.SetupRateLimitGovernor, called once)")
	}
	defer runningSamplers.Add(-1)
	g.mu.Lock()
	reconcilers, gated := g.reconcilers, g.gatedControllers
	g.mu.Unlock()
	if reconcilers > gated {
		g.log.Error(errors.New("a reconciler has no local bound"),
			"More reconcilers use this Governor than controllers have a gated queue. Build every controller that uses it with controller.Options.ForControllerRuntime",
			"reconcilers", reconcilers, "gatedControllers", gated)
	}
	g.log.Info("Saturation sampler started", "cgroup", g.sat.dir, "samplePeriod", g.sat.opts.SamplePeriod.String())
	g.sample()
	t := time.NewTicker(g.sat.opts.SamplePeriod)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			g.sample()
		}
	}
}

// NeedLeaderElection is false: every replica samples its own container.
func (g *Governor) NeedLeaderElection() bool { return false }

// sample takes one saturation sample, publishes the new local capacity, and
// grants any slot a rise freed. Only the sampler goroutine calls it (tests call
// it directly instead). The files are read before g.mu is taken, so Acquire and
// the gated queues never wait on I/O.
func (g *Governor) sample() {
	now := g.now()
	smp := g.sat.read(now)
	g.mu.Lock()
	cur := g.local.Load()
	g.advanceSlotTimeLocked(now)
	work := workTotals{slotTime: g.slotTime, completions: g.completions.Load(), arrivals: g.arrivals.Load()}
	next, cutBy := g.sat.advance(now, smp, *cur, g.ceilingLocked(), g.recovery, work)
	if next != *cur {
		g.local.Store(&next)
	}
	g.dispatchLocked()
	g.mu.Unlock()
	switch {
	case next.capacity >= cur.capacity:
	case cutBy != "":
		g.log.Info("Controller saturated: lowering local capacity", "from", cur.capacity, "to", next.capacity, "cause", next.cause)
	default:
		g.log.Info("Capacity model: lowering local capacity to what the measured CPU use allows", "from", cur.capacity, "to", next.capacity)
	}
}

// KeySnapshot is one key's state, for metrics.
type KeySnapshot struct {
	Key      string
	Capacity int
	// SecondsToReset is the time left until the key's hold lifts, 0 when it
	// isn't held.
	SecondsToReset float64
}

// InputSnapshot is one saturation input's last sample, for metrics.
type InputSnapshot struct {
	// Input is cpu_pressure or memory_working_set.
	Input string
	// Enabled is false when the input is missing (no cgroup v2, no PSI, no
	// memory limit) or saturation is off. A disabled input has no Percent:
	// zero would read as healthy.
	Enabled bool
	// Percent is the input's value: PSI some avg10 for cpu_pressure, the working
	// set as a share of memory.max for the working set.
	Percent float64
}

// ControllerSnapshot is one controller's use of the local capacity, for
// metrics: what its gated queue holds, is guaranteed and is waiting for.
type ControllerSnapshot struct {
	// Controller is the gated queue's name.
	Controller string
	// InFlight is the local slots the queue holds now.
	InFlight int
	// Guaranteed is the share of the local capacity the queue is guaranteed.
	Guaranteed int
	// Waiting is the items ready to run and waiting for a slot: the demand
	// the local capacity holds back.
	Waiting int
	// Arrivals counts the items that became ready to run, cumulative. An item
	// added again while it waits counts once.
	Arrivals int64
	// Completions counts the reconciles that gave their slot back, cumulative.
	Completions int64
}

// CutSnapshot is the number of cuts one saturation input made, for metrics.
type CutSnapshot struct {
	// Input is cpu_pressure or memory_working_set.
	Input string
	// Cuts counts the times the input's step lowered the local capacity,
	// cumulative.
	Cuts int64
}

// CPUSnapshot is what the saturation input measured of the container's CPU,
// for metrics. A nil field has no value: zero would read as a measurement.
type CPUSnapshot struct {
	// Limit is the CPU limit read from cpu.max, in cores. Nil with no limit,
	// or with saturation off.
	Limit *float64
	// ModelCapacity is the capacity model's capacity M. Nil while the law
	// doesn't apply; so are the fields below.
	ModelCapacity *float64
	// Used is U, the CPU used over the last interval, in cores, and
	// Background is B, the CPU used with no reconcile in flight. Nil until an
	// interval has been measured.
	Used, Background *float64
	// PerReconcile is the CPU seconds one reconcile used, over the last
	// interval with completions. Nil until there was one.
	PerReconcile *float64
	// Required is the CPU limit, in cores, at which the work that arrived in
	// the last interval would run at the target: an estimate, not a
	// measurement. Nil until PerReconcile is known.
	Required *float64
}

// Snapshot is the governor's state, for metrics.
type Snapshot struct {
	Keys []KeySnapshot
	// LocalCapacity is enforced only when SaturationEnabled: queues are gated
	// on it then, and on nothing otherwise, so metrics report it only then.
	LocalCapacity     int
	SaturationEnabled bool
	Inputs            []InputSnapshot
	// Controllers has one entry per gated queue name, sorted by name. It is
	// empty with saturation off: nothing is gated then.
	Controllers []ControllerSnapshot
	// Cuts has one entry per saturation input, with saturation on.
	Cuts []CutSnapshot
	// CPU is what the saturation input measured of the container's CPU.
	CPU CPUSnapshot
}

// Snapshot returns the governor's current state. It takes no queue's lock.
func (g *Governor) Snapshot() Snapshot {
	g.mu.Lock()
	now := g.now()
	keys := make([]KeySnapshot, 0, len(g.keys))
	for k, st := range g.keys {
		g.maybeRaiseLocked(st, now)
		ks := KeySnapshot{Key: k, Capacity: st.capacity}
		if now.Before(st.holdUntil) {
			ks.SecondsToReset = st.holdUntil.Sub(now).Seconds()
		}
		keys = append(keys, ks)
	}
	guaranteed := g.guaranteedLocked()
	queues := make([]*queueCounts, 0, len(g.queues))
	for _, q := range g.queues {
		queues = append(queues, q.counts())
	}
	g.mu.Unlock()
	sort.Slice(keys, func(i, j int) bool { return keys[i].Key < keys[j].Key })

	snap := Snapshot{
		Keys:              keys,
		LocalCapacity:     g.LocalCapacity(),
		SaturationEnabled: g.SaturationEnabled(),
		Inputs:            g.sat.inputs(),
	}
	if g.sat == nil {
		return snap
	}
	snap.Controllers = controllerSnapshots(queues, guaranteed)
	snap.Cuts = g.sat.cuts()
	snap.CPU = g.sat.cpu()
	return snap
}

// controllerSnapshots returns one entry per queue name, sorted by name. Queues
// that share a name are one controller to a reader of the metrics, so their
// counts are added up.
func controllerSnapshots(queues []*queueCounts, guaranteed int) []ControllerSnapshot {
	byName := map[string]*ControllerSnapshot{}
	for _, q := range queues {
		cs, ok := byName[q.name]
		if !ok {
			cs = &ControllerSnapshot{Controller: q.name}
			byName[q.name] = cs
		}
		cs.InFlight += int(q.held.Load())
		cs.Guaranteed += guaranteed
		cs.Waiting += int(q.waiting.Load())
		cs.Arrivals += q.arrivals.Load()
		cs.Completions += q.completions.Load()
	}
	out := make([]ControllerSnapshot, 0, len(byName))
	for _, cs := range byName {
		out = append(out, *cs)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Controller < out[j].Controller })
	return out
}
