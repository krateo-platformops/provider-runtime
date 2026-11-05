package ratelimit

import (
	"context"
	cryptorand "crypto/rand"
	"fmt"
	"io"
	"math/big"
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
	// DeferAtCapacity: the key, or the local capacity while it ramps up after
	// a start or a saturation, has as many calls in flight as it allows. There
	// is no server deadline.
	DeferAtCapacity DeferReason = "AtCapacity"
	// DeferSaturated: the controller's own container is saturated and the
	// process-wide local capacity is full. There is no server deadline.
	DeferSaturated DeferReason = "Saturated"
)

// Cause is the value of the cause attribute on the deferrals metric:
// throttled, capacity or saturated.
func (r DeferReason) Cause() string {
	switch r {
	case DeferThrottled:
		return "throttled"
	case DeferAtCapacity:
		return "capacity"
	case DeferSaturated:
		return "saturated"
	}
	return string(r)
}

// A Deferral says a call must not be made now, why, and when to try again.
type Deferral struct {
	// Key is the key of the call that was deferred.
	Key string
	// Reason says why.
	Reason DeferReason
	// Until is the server's own deadline. It is set only for DeferThrottled;
	// the governor never invents a deadline.
	Until time.Time
	// RequeueAfter is how long the caller waits before it tries again. For
	// DeferThrottled it is the time left until Until, never less. Otherwise it
	// is a random delay between 1s and the RecoveryInterval.
	RequeueAfter time.Duration
	// Capacity is the capacity that was full: the key's, or the local one when
	// Local is set.
	Capacity int
	// Local is set when the process-wide local capacity was full, rather than
	// the key's. Its message names the cause and no key: the controller is
	// saturated (DeferSaturated), or ramping up after a start or a saturation
	// (DeferAtCapacity).
	Local bool
	// Cause names the saturated inputs for DeferSaturated, for example
	// "memory working set 93% of limit".
	Cause string
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
	case DeferSaturated:
		return fmt.Sprintf("reconciliation deferred: controller saturated: %s; waiting for capacity (%d call(s) in flight allowed); no server deadline was given",
			d.Cause, d.Capacity)
	}
	if d.Local {
		return fmt.Sprintf("reconciliation deferred: controller ramping up its local concurrency limit (%d call(s) in flight allowed); waiting for capacity; no server deadline was given",
			d.Capacity)
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
	// ObserveRateLimit registers snapshot as the source of the governor's
	// gauges: per-key capacity and seconds to reset, local capacity, and each
	// saturation input.
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
	// holdUntil is the server's deadline; the gate is closed before it.
	holdUntil time.Time
	// lastReduce is when a throttled or advisory signal last cut capacity.
	lastReduce time.Time
	// lastRaise is when capacity last rose by one.
	lastRaise time.Time
}

// localState is the process-wide local capacity the saturation sampler
// publishes. It is replaced, never mutated, so Acquire reads it with one
// atomic load.
type localState struct {
	capacity int
	// cause names the inputs that last cut capacity, until it recovers to the
	// ceiling.
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
	log           logging.Logger
	metrics       MetricsRecorder

	// now and jitter are replaced in tests.
	now    func() time.Time
	jitter func() time.Duration

	mu            sync.Mutex
	keys          map[string]*keyState
	localInFlight int
	// reconcilers is how many reconcilers registered; see ceilingLocked.
	reconcilers int

	// local is written under mu and read without it.
	local atomic.Pointer[localState]
	// sat is nil when saturation is disabled.
	sat *saturation
}

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
	if o.Logger == nil {
		o.Logger = logging.NewNopLogger()
	}

	g := &Governor{
		perController: o.MaxConcurrentReconciles,
		ignoreSignals: o.IgnoreSignals,
		recovery:      o.RecoveryInterval,
		log:           o.Logger,
		metrics:       o.Metrics,
		now:           time.Now,
		keys:          map[string]*keyState{},
	}
	g.jitter = func() time.Duration { return jitterBetween(cryptorand.Reader, minCapacityJitter, g.recovery) }

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
// which case every gated call needs a local slot, keyed or not.
func (g *Governor) SaturationEnabled() bool { return g.sat != nil }

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
	if st.capacity >= g.ceilingLocked() || now.Before(st.holdUntil) {
		return
	}
	cleanSince := st.lastReduce
	if st.holdUntil.After(cleanSince) {
		cleanSince = st.holdUntil
	}
	if now.Sub(cleanSince) < g.recovery || now.Sub(st.lastRaise) < g.recovery {
		return
	}
	st.capacity++
	st.lastRaise = now
}

// Acquire asks to make one gated call for key. It never blocks and does no
// I/O: it either grants a slot in both the key's capacity and the local
// capacity, returning the release func to call when the call returns, or it
// returns a Deferral at once. An empty key asks for a local slot only: the
// call of a provider without a RateLimitKeyer, which saturation still bounds.
//
// A blocking gate would park workers: with MaxConcurrentReconciles workers and
// one throttled key, every worker could end up waiting on that key while
// objects on healthy endpoints stop reconciling (design §5).
//
// release is nil exactly when the Deferral is not. Calling release more than
// once is safe.
func (g *Governor) Acquire(ctx context.Context, key string) (release func(), d *Deferral) {
	d = g.acquire(key)
	if d != nil {
		if g.metrics != nil {
			g.metrics.IncRateLimitDeferral(ctx, key, d.Reason)
		}
		return nil, d
	}
	var once sync.Once
	return func() {
		once.Do(func() {
			g.mu.Lock()
			defer g.mu.Unlock()
			if st, ok := g.keys[key]; key != "" && ok && st.inFlight > 0 {
				st.inFlight--
			}
			if g.localInFlight > 0 {
				g.localInFlight--
			}
		})
	}, nil
}

func (g *Governor) acquire(key string) *Deferral {
	g.mu.Lock()
	defer g.mu.Unlock()

	var st *keyState
	if key != "" {
		now := g.now()
		st = g.state(key)
		g.maybeRaiseLocked(st, now)
		if now.Before(st.holdUntil) {
			return &Deferral{Key: key, Reason: DeferThrottled, Until: st.holdUntil, RequeueAfter: st.holdUntil.Sub(now)}
		}
		if st.inFlight >= st.capacity {
			return &Deferral{Key: key, Reason: DeferAtCapacity, RequeueAfter: g.jitter(), Capacity: st.capacity}
		}
	}
	local := g.local.Load()
	if g.localInFlight >= local.capacity {
		reason := DeferAtCapacity
		if local.cause != "" {
			reason = DeferSaturated
		}
		return &Deferral{Key: key, Reason: reason, Local: true, RequeueAfter: g.jitter(), Capacity: local.capacity, Cause: local.cause}
	}

	if st != nil {
		st.inFlight++
	}
	g.localInFlight++
	return nil
}

// Observe folds a provider's signal into its key's state. The Governor is the
// Sink the reconciler installs, so Report reaches it. A signal is applied to
// its own key, even when that differs from the key the call was gated on.
//
//   - Throttled: capacity drops to 1 and the gate closes until the signal's
//     deadline, max(RetryAfter, ResetAt). A later deadline extends the hold; an
//     earlier one never shortens it.
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

	g.mu.Lock()
	st := g.state(s.Key)
	switch kind {
	case SignalThrottled:
		st.capacity = capacityFloor
		if d := s.Deadline(now); d.After(st.holdUntil) {
			st.holdUntil = d
		}
	case SignalAdvisory:
		st.capacity = max(capacityFloor, st.capacity/2)
	}
	st.lastReduce = now
	g.mu.Unlock()

	if g.metrics != nil {
		g.metrics.IncRateLimitSignal(context.Background(), s.Key, kind)
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
// the Governor to the manager with mgr.Add.
func (g *Governor) Start(ctx context.Context) error {
	if g.sat == nil {
		<-ctx.Done()
		return nil
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

// sample takes one saturation sample and publishes the new local capacity.
// Only the sampler goroutine calls it (tests call it directly instead). The
// files are read before g.mu is taken, so Acquire never waits on I/O.
func (g *Governor) sample() {
	smp := g.sat.read()
	g.mu.Lock()
	cur := g.local.Load()
	next := g.sat.step(g.now(), smp, *cur, g.ceilingLocked(), g.recovery)
	if next != *cur {
		g.local.Store(&next)
	}
	g.mu.Unlock()
	if next.capacity < cur.capacity {
		g.log.Info("Controller saturated: lowering local capacity", "from", cur.capacity, "to", next.capacity, "cause", next.cause)
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

// Snapshot is the governor's state, for metrics.
type Snapshot struct {
	Keys          []KeySnapshot
	LocalCapacity int
	Inputs        []InputSnapshot
}

// Snapshot returns the governor's current state.
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
	g.mu.Unlock()
	sort.Slice(keys, func(i, j int) bool { return keys[i].Key < keys[j].Key })

	return Snapshot{
		Keys:          keys,
		LocalCapacity: g.LocalCapacity(),
		Inputs:        g.sat.inputs(),
	}
}
