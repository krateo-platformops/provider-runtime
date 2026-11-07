package ratelimit

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

// clock is a settable clock: every decision the governor makes is relative to
// now, so a real clock would make deadline assertions sleeps.
type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *clock) add(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

func testGovernor(t *testing.T, o Options) (*Governor, *clock) {
	t.Helper()
	g, err := New(o)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	c := &clock{t: time.Date(2026, 11, 5, 10, 0, 0, 0, time.UTC)}
	g.now = c.now
	return g, c
}

func mustAcquire(t *testing.T, g *Governor, key string) func() {
	t.Helper()
	release, d := g.Acquire(context.Background(), key)
	if d != nil {
		t.Fatalf("Acquire(%q) was deferred (%+v), want a slot", key, d)
	}
	return release
}

// Capacity bounds concurrency: without this the governor is a gauge, not a gate.
func TestCapacityBoundsConcurrency(t *testing.T) {
	g, _ := testGovernor(t, Options{MaxConcurrentReconciles: 2})

	r1 := mustAcquire(t, g, "h")
	r2 := mustAcquire(t, g, "h")
	release, d := g.Acquire(context.Background(), "h")
	if d == nil {
		t.Fatal("a third concurrent call was let through with a ceiling of 2")
	}
	if release != nil {
		t.Error("a deferred Acquire returned a release func")
	}
	if d.Reason != DeferAtCapacity || !d.Until.IsZero() {
		t.Errorf("deferral = %+v, want AtCapacity with no deadline: the governor never invents one", d)
	}

	r1()
	r1() // twice must not free a second slot
	r3 := mustAcquire(t, g, "h")
	if _, d := g.Acquire(context.Background(), "h"); d == nil {
		t.Error("calling a release func twice freed two slots")
	}
	r2()
	r3()
}

// The server's deadline is respected: RetryAfter=30s defers for at least 30s,
// never less, and a later, shorter signal doesn't shorten the hold.
func TestServerDeadlineIsRespected(t *testing.T) {
	g, c := testGovernor(t, Options{MaxConcurrentReconciles: 4})
	start := c.now()

	g.Observe(Signal{Key: "api.github.com", Throttled: true, RetryAfter: 30 * time.Second})

	_, d := g.Acquire(context.Background(), "api.github.com")
	if d == nil {
		t.Fatal("a throttled key let a call through")
	}
	if d.Reason != DeferThrottled {
		t.Fatalf("reason = %q, want %q", d.Reason, DeferThrottled)
	}
	if want := start.Add(30 * time.Second); d.Until.Before(want) {
		t.Errorf("Until = %v, earlier than the server's deadline %v", d.Until, want)
	}
	if d.RequeueAfter < 30*time.Second {
		t.Errorf("RequeueAfter = %v, want at least 30s", d.RequeueAfter)
	}

	// A shorter signal arriving later must not move the deadline earlier.
	g.Observe(Signal{Key: "api.github.com", Throttled: true, RetryAfter: time.Second})
	c.add(29 * time.Second)
	_, d = g.Acquire(context.Background(), "api.github.com")
	if d == nil || d.Reason != DeferThrottled {
		t.Fatalf("at 29s the gate was open (%+v): a shorter later signal shortened the server's deadline", d)
	}
	if d.RequeueAfter < time.Second {
		t.Errorf("RequeueAfter at 29s = %v, want the remaining 1s", d.RequeueAfter)
	}

	// And the hold lifts at the deadline: a gate that never reopens is worse
	// than none.
	c.add(time.Second)
	mustAcquire(t, g, "api.github.com")()
}

// With both RetryAfter and ResetAt set, the later wins.
func TestThrottleTakesTheLaterOfRetryAfterAndResetAt(t *testing.T) {
	g, c := testGovernor(t, Options{MaxConcurrentReconciles: 4})
	later := c.now().Add(90 * time.Second)
	g.Observe(Signal{Key: "h", Throttled: true, RetryAfter: 10 * time.Second, ResetAt: later})

	_, d := g.Acquire(context.Background(), "h")
	if d == nil || d.Until.Before(later) {
		t.Fatalf("deferral = %+v, want Until >= %v", d, later)
	}
}

// Keys are independent: with two keys and one throttled, the other key's calls
// go through at once, at full capacity.
func TestKeysAreIndependent(t *testing.T) {
	g, _ := testGovernor(t, Options{MaxConcurrentReconciles: 3})
	g.Observe(Signal{Key: "ghe.example.com", Throttled: true, RetryAfter: time.Hour})

	if _, d := g.Acquire(context.Background(), "ghe.example.com"); d == nil {
		t.Fatal("the throttled key was not deferred, so this test shows nothing")
	}
	var releases []func()
	for range 3 {
		releases = append(releases, mustAcquire(t, g, "dev.azure.com"))
	}
	for _, r := range releases {
		r()
	}
	if got := g.Capacity("dev.azure.com"); got != 3 {
		t.Errorf("the healthy key's capacity is %d, want the ceiling 3", got)
	}
}

// Recovery is additive: capacity returns to the ceiling in N steps of
// RecoveryInterval, not in one.
func TestRecoveryIsAdditive(t *testing.T) {
	const ceiling = 4
	g, c := testGovernor(t, Options{MaxConcurrentReconciles: ceiling, RecoveryInterval: 10 * time.Second})

	g.Observe(Signal{Key: "h", Throttled: true, RetryAfter: time.Second})
	if got := g.Capacity("h"); got != 1 {
		t.Fatalf("capacity after a throttle = %d, want 1", got)
	}

	want := 1
	for step := 1; want < ceiling; step++ {
		c.add(5 * time.Second) // half an interval: no step
		mustAcquire(t, g, "h")()
		if got := g.Capacity("h"); got != want {
			t.Fatalf("step %d: capacity rose to %d after half a RecoveryInterval, want %d", step, got, want)
		}
		c.add(6 * time.Second)
		mustAcquire(t, g, "h")()
		want++
		if got := g.Capacity("h"); got != want {
			t.Fatalf("step %d: capacity = %d after one more RecoveryInterval, want %d (+1, not a jump)", step, got, want)
		}
	}

	c.add(time.Hour)
	if got := g.Capacity("h"); got != ceiling {
		t.Errorf("capacity = %d, want it to stop at the ceiling %d", got, ceiling)
	}
}

// Advisory halves and keeps the gate open: the call succeeded, the server is
// still serving.
func TestAdvisoryHalvesCapacityWithFloorOne(t *testing.T) {
	g, _ := testGovernor(t, Options{MaxConcurrentReconciles: 8})

	g.Observe(Signal{Key: "h", HasQuota: true, Limit: 5000, Remaining: 1000})
	if got := g.Capacity("h"); got != 4 {
		t.Fatalf("capacity = %d after one advisory signal, want 4", got)
	}
	mustAcquire(t, g, "h")()

	// Azure DevOps's shape: a delay on a served call, no counters.
	for range 5 {
		g.Observe(Signal{Key: "h", RetryAfter: 500 * time.Millisecond})
	}
	if got := g.Capacity("h"); got != 1 {
		t.Errorf("capacity = %d after repeated advisories, want the floor 1", got)
	}
	mustAcquire(t, g, "h")()
}

// A healthy quota report changes nothing, even with a future ResetAt. GitHub
// sends X-RateLimit-Reset on every response; reading it as advisory would
// halve every GitHub key on every call.
func TestHealthyQuotaChangesNothing(t *testing.T) {
	g, c := testGovernor(t, Options{MaxConcurrentReconciles: 8, RecoveryInterval: 10 * time.Second})
	g.Observe(Signal{Key: "h", HasQuota: true, Limit: 5000, Remaining: 4999, ResetAt: c.now().Add(time.Hour)})
	if got := g.Capacity("h"); got != 8 {
		t.Fatalf("capacity = %d after a healthy quota report, want the ceiling 8", got)
	}

	// Nor does it hold back recovery: a halved key keeps recovering while
	// healthy reports keep arriving.
	g.Observe(Signal{Key: "h", HasQuota: true, Limit: 5000, Remaining: 100})
	c.add(11 * time.Second)
	g.Observe(Signal{Key: "h", HasQuota: true, Limit: 5000, Remaining: 4999, ResetAt: c.now().Add(time.Hour)})
	if got := g.Capacity("h"); got != 5 {
		t.Errorf("capacity = %d, want 5: healthy reports must not restart the clean interval", got)
	}
}

func TestKeylessSignalCreatesNoState(t *testing.T) {
	g, _ := testGovernor(t, Options{MaxConcurrentReconciles: 4})
	g.Observe(Signal{Throttled: true, RetryAfter: time.Hour})
	if n := len(g.Snapshot().Keys); n != 0 {
		t.Errorf("a keyless signal created %d key state(s); the empty key is not a scope", n)
	}
}

// At capacity, the requeue is a random delay in [1s, RecoveryInterval].
func TestAtCapacityRequeueIsJitteredWithinOneSecondAndRecoveryInterval(t *testing.T) {
	g, err := New(Options{MaxConcurrentReconciles: 1, RecoveryInterval: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	release := mustAcquire(t, g, "h")
	defer release()

	seen := map[time.Duration]bool{}
	for range 2000 {
		_, d := g.Acquire(context.Background(), "h")
		if d == nil {
			t.Fatal("expected a deferral at capacity")
		}
		if d.RequeueAfter < time.Second || d.RequeueAfter > 5*time.Second {
			t.Fatalf("RequeueAfter = %v, outside [1s, 5s]", d.RequeueAfter)
		}
		seen[d.RequeueAfter] = true
	}
	if len(seen) < 100 {
		t.Errorf("only %d distinct delays in 2000 draws: the jitter is not random", len(seen))
	}
}

type failingReader struct{}

func (failingReader) Read([]byte) (int, error) { return 0, errors.New("entropy unavailable") }

// If the random read fails, the full RecoveryInterval is used, never less.
func TestJitterFallsBackToTheFullRecoveryInterval(t *testing.T) {
	if got := jitterBetween(failingReader{}, time.Second, 30*time.Second); got != 30*time.Second {
		t.Errorf("jitter on a failed read = %v, want the full 30s", got)
	}
	if got := jitterBetween(failingReader{}, time.Second, 500*time.Millisecond); got != 500*time.Millisecond {
		t.Errorf("jitter with an interval under 1s = %v, want the interval", got)
	}
}

func TestNewRejectsAZeroCeiling(t *testing.T) {
	if _, err := New(Options{}); err == nil {
		t.Error("New with MaxConcurrentReconciles 0 succeeded; it would cap every key at 1 silently")
	}
	if _, err := New(Options{MaxConcurrentReconciles: 1, Saturation: SaturationOptions{Enabled: true, WorkingSetSoftRatio: 0.97, WorkingSetHardRatio: 0.95}}); err == nil {
		t.Error("New accepted a soft ratio above the hard ratio")
	}
}

func TestDeferralMessages(t *testing.T) {
	until := time.Date(2026, 11, 5, 10, 0, 30, 200_000_000, time.UTC)
	throttled := (&Deferral{Key: "api.github.com", Reason: DeferThrottled, Until: until}).Message()
	// Rounded UP: the time a reader sees is never before the server's deadline.
	if !strings.Contains(throttled, "api.github.com") || !strings.Contains(throttled, "2026-11-05T10:00:31Z") {
		t.Errorf("throttled message %q must name the key and the deadline rounded up to the second", throttled)
	}
	capacity := (&Deferral{Key: "api.github.com", Reason: DeferAtCapacity, Capacity: 2}).Message()
	if !strings.Contains(capacity, "waiting for capacity") || strings.Contains(capacity, "0001-01-01") {
		t.Errorf("capacity message %q must say waiting for capacity and carry no date", capacity)
	}
}

func TestReportIsANoOpWithoutASink(t *testing.T) {
	Report(context.Background(), Signal{Key: "h", Throttled: true})
}

type recordingSink struct{ got []Signal }

func (r *recordingSink) Observe(s Signal) { r.got = append(r.got, s) }

func TestReportReachesTheSinkAndDropsKeylessSignals(t *testing.T) {
	sink := &recordingSink{}
	ctx := WithSink(context.Background(), sink)
	Report(ctx, Signal{Key: "h", Throttled: true})
	Report(ctx, Signal{Throttled: true})
	if len(sink.got) != 1 || sink.got[0].Key != "h" {
		t.Errorf("sink received %+v, want only the keyed signal", sink.got)
	}
}

// TestClassify is the shared vocabulary for what a Signal means. Its mirror is
// unstructured-runtime's TestClassificationMatchesTheSharedVocabulary, which
// has the same rows: the two runtimes duplicate Signal on purpose (design §10,
// open question 1), so this table is what keeps them reading it the same way.
// A change to either table must change both.
func TestClassify(t *testing.T) {
	now := time.Date(2026, 11, 5, 10, 0, 0, 0, time.UTC)
	cases := map[string]struct {
		s    Signal
		want SignalKind
	}{
		// An explicit RetryAfter cuts whatever the counters say. This row is
		// where unstructured-runtime#57 diverged before #58 adopted this order.
		"healthy quota with a delay":       {Signal{Key: "h", HasQuota: true, Limit: 5000, Remaining: 4900, RetryAfter: 2 * time.Second}, SignalAdvisory},
		"healthy quota alone":              {Signal{Key: "h", HasQuota: true, Limit: 5000, Remaining: 4900}, SignalNone},
		"healthy quota, reset in the past": {Signal{Key: "h", HasQuota: true, Limit: 5000, Remaining: 4900, ResetAt: now.Add(-time.Minute)}, SignalNone},
		"refused":                          {Signal{Key: "h", Throttled: true}, SignalThrottled},
		"quota a quarter left":             {Signal{Key: "h", HasQuota: true, Limit: 100, Remaining: 25}, SignalAdvisory},
		"quota exhausted on a 200":         {Signal{Key: "h", HasQuota: true, Limit: 200, Remaining: 0}, SignalAdvisory},
		"delay on a served call":           {Signal{Key: "h", RetryAfter: time.Second}, SignalAdvisory},
		"reset without counters":           {Signal{Key: "h", ResetAt: now.Add(time.Minute)}, SignalAdvisory},
		"healthy quota with a reset":       {Signal{Key: "h", HasQuota: true, Limit: 100, Remaining: 90, ResetAt: now.Add(time.Hour)}, SignalNone},
		"reset in the past, no counters":   {Signal{Key: "h", ResetAt: now.Add(-time.Minute)}, SignalNone},
		"nothing":                          {Signal{Key: "h"}, SignalNone},
	}
	for name, tc := range cases {
		if got := Classify(tc.s, now); got != tc.want {
			t.Errorf("%s: Classify = %q, want %q", name, got, tc.want)
		}
	}
}

// The ceiling is the process's total workers: MaxConcurrentReconciles per
// registered reconciler, so enabling the governor never caps a healthy key
// below what the workers could do today.
func TestCeilingScalesWithRegisteredReconcilers(t *testing.T) {
	g, _ := testGovernor(t, Options{MaxConcurrentReconciles: 3})
	if got := g.Ceiling(); got != 3 {
		t.Fatalf("ceiling with no reconcilers registered = %d, want 3", got)
	}
	g.RegisterReconciler()
	g.RegisterReconciler()
	if got := g.Ceiling(); got != 6 {
		t.Fatalf("ceiling with two reconcilers = %d, want 6", got)
	}
	if got := g.LocalCapacity(); got != 6 {
		t.Errorf("local capacity = %d, want it raised to the new ceiling 6", got)
	}
	var releases []func()
	for range 6 {
		releases = append(releases, mustAcquire(t, g, "h"))
	}
	if _, d := g.Acquire(context.Background(), "h"); d == nil {
		t.Error("a seventh call went through with a ceiling of 6")
	}
	for _, r := range releases {
		r()
	}
}

// With IgnoreSignals (saturation only), signals change nothing.
func TestIgnoreSignals(t *testing.T) {
	g, _ := testGovernor(t, Options{MaxConcurrentReconciles: 4, IgnoreSignals: true})
	g.Observe(Signal{Key: "h", Throttled: true, RetryAfter: time.Hour})
	if got := g.Capacity("h"); got != 4 {
		t.Errorf("capacity = %d, want 4: signals are ignored", got)
	}
	mustAcquire(t, g, "h")()
}

func mustAcquireAll(t *testing.T, g *Governor, keys ...string) func() {
	t.Helper()
	release, d := g.AcquireAll(context.Background(), keys)
	if d != nil {
		t.Fatalf("AcquireAll(%q) was deferred (%+v), want a slot in each", keys, d)
	}
	return release
}

// A call on several keys needs a slot in every one, and one full key stops it.
// The deferral names that key, not the one with room.
func TestAcquireAllNeedsASlotInEveryKey(t *testing.T) {
	g, _ := testGovernor(t, Options{MaxConcurrentReconciles: 1})
	held := mustAcquire(t, g, "b")
	defer held()

	_, d := g.AcquireAll(context.Background(), []string{"a", "b"})
	if d == nil || d.Reason != DeferAtCapacity || d.Key != "b" || d.Capacity != 1 {
		t.Fatalf("deferral = %+v, want AtCapacity on the full key b", d)
	}
	if !strings.Contains(d.Message(), "b is at its concurrency limit") {
		t.Errorf("message %q must name the key that stopped the call", d.Message())
	}
}

// All or nothing: a deferred call holds no slot on the keys that had room, or
// every deferral would leak one and the healthy endpoint would seize up.
func TestAcquireAllHoldsNothingWhileDeferred(t *testing.T) {
	g, _ := testGovernor(t, Options{MaxConcurrentReconciles: 4})
	g.Observe(Signal{Key: "z", RetryAfter: time.Second}) // advisory: 4 -> 2
	g.Observe(Signal{Key: "z", RetryAfter: time.Second}) // advisory: 2 -> 1
	held := mustAcquire(t, g, "z")
	defer held()

	for range 5 {
		if _, d := g.AcquireAll(context.Background(), []string{"a", "z"}); d == nil {
			t.Fatal("the call was not deferred, so this test shows nothing")
		}
	}
	if st := g.keys["a"]; st.inFlight != 0 {
		t.Errorf("a has %d calls in flight, want 0: deferred calls leaked its slots", st.inFlight)
	}
}

// Every key is released, once, however many times release runs.
func TestAcquireAllReleasesEveryKey(t *testing.T) {
	g, _ := testGovernor(t, Options{MaxConcurrentReconciles: 4})
	other := mustAcquire(t, g, "a")
	defer other()
	release := mustAcquireAll(t, g, "a", "b")
	if a, b := g.keys["a"].inFlight, g.keys["b"].inFlight; a != 2 || b != 1 {
		t.Fatalf("in flight a=%d b=%d, want 2 and 1", a, b)
	}
	release()
	release()
	if a, b := g.keys["a"].inFlight, g.keys["b"].inFlight; a != 1 || b != 0 {
		t.Errorf("after two releases in flight a=%d b=%d, want 1 and 0: release must count once on every key", a, b)
	}
}

// A throttled key stops the call however much room the others have. This is
// the case the feature exists for: a sync between two endpoints must stop
// calling either one when it refuses.
func TestAcquireAllDefersOnAThrottledKey(t *testing.T) {
	for name, keys := range map[string][]string{
		"first in order": {"throttled", "zz-healthy"},
		"last in order":  {"aa-healthy", "throttled"},
	} {
		t.Run(name, func(t *testing.T) {
			g, c := testGovernor(t, Options{MaxConcurrentReconciles: 2})
			g.Observe(Signal{Key: "throttled", Throttled: true, RetryAfter: time.Minute})

			_, d := g.AcquireAll(context.Background(), keys)
			if d == nil || d.Reason != DeferThrottled || d.Key != "throttled" {
				t.Fatalf("deferral = %+v, want Throttled on the key that was refused", d)
			}
			if want := c.now().Add(time.Minute); !d.Until.Equal(want) || d.RequeueAfter != time.Minute {
				t.Errorf("Until %v, RequeueAfter %v; want %v, 1m", d.Until, d.RequeueAfter, want)
			}
		})
	}
}

// With two keys held, the call can't run before the later deadline lifts, so
// that's the one it waits for, whichever key sorts first.
func TestAcquireAllWaitsForTheLatestDeadline(t *testing.T) {
	for name, deadlines := range map[string]map[string]time.Duration{
		"first key is later": {"a": time.Hour, "b": time.Minute},
		"last key is later":  {"a": time.Minute, "b": time.Hour},
	} {
		t.Run(name, func(t *testing.T) {
			g, _ := testGovernor(t, Options{MaxConcurrentReconciles: 2})
			later := ""
			for key, d := range deadlines {
				g.Observe(Signal{Key: key, Throttled: true, RetryAfter: d})
				if d == time.Hour {
					later = key
				}
			}
			_, d := g.AcquireAll(context.Background(), []string{"a", "b"})
			if d == nil || d.Key != later || d.RequeueAfter != time.Hour {
				t.Fatalf("deferral = %+v, want the deadline of %s, 1h away", d, later)
			}
		})
	}
}

// A hold outranks a full capacity, as it does for one key: a server's deadline
// is a fact, and "waiting for capacity" would bring the call back too early.
func TestAcquireAllPrefersAHoldToAFullKey(t *testing.T) {
	g, _ := testGovernor(t, Options{MaxConcurrentReconciles: 1})
	held := mustAcquire(t, g, "a")
	defer held()
	g.Observe(Signal{Key: "b", Throttled: true, RetryAfter: time.Minute})

	_, d := g.AcquireAll(context.Background(), []string{"a", "b"})
	if d == nil || d.Reason != DeferThrottled || d.Key != "b" {
		t.Fatalf("deferral = %+v, want Throttled on b, not AtCapacity on a", d)
	}
}

// A key held for one reconcile recovers even while another key stops the call,
// or an object on a long hold would keep its other endpoint's capacity down.
func TestAcquireAllRaisesEveryKey(t *testing.T) {
	g, c := testGovernor(t, Options{MaxConcurrentReconciles: 4, RecoveryInterval: time.Minute})
	g.Observe(Signal{Key: "z", RetryAfter: time.Second})
	if got := g.Capacity("z"); got != 2 {
		t.Fatalf("z's capacity = %d, want 2 after an advisory signal", got)
	}
	g.Observe(Signal{Key: "a", Throttled: true, RetryAfter: time.Hour})

	c.add(2 * time.Minute)
	if _, d := g.AcquireAll(context.Background(), []string{"a", "z"}); d == nil || d.Key != "a" {
		t.Fatalf("deferral = %+v, want the hold on a", d)
	}
	// Read directly: Capacity raises the key itself.
	if got := g.keys["z"].capacity; got != 3 {
		t.Errorf("z's capacity = %d, want 3: the clean interval passed", got)
	}
}

// Empty keys are ignored and a repeated key counts once, so two names for one
// endpoint take one slot. The caller's slice is left as it was: a provider may
// hand in the same slice every time.
func TestAcquireAllNormalizesKeys(t *testing.T) {
	g, _ := testGovernor(t, Options{MaxConcurrentReconciles: 4})
	keys := []string{"b", "", "a", "b"}
	release := mustAcquireAll(t, g, keys...)
	defer release()
	if !slices.Equal(keys, []string{"b", "", "a", "b"}) {
		t.Errorf("the caller's slice became %q", keys)
	}
	if a, b := g.keys["a"].inFlight, g.keys["b"].inFlight; a != 1 || b != 1 {
		t.Errorf("in flight a=%d b=%d, want 1 each: a repeated key counts once", a, b)
	}
	if _, ok := g.keys[""]; ok {
		t.Error("an empty key created state")
	}
}

// A gated call takes no local slot, keyed or not: the gated queue took one
// before the reconcile started, and it covers every call in it. A call with no
// key is not gated at all, and creates no key state.
func TestCallsTakeNoLocalSlot(t *testing.T) {
	g, _ := saturationGovernor(t, 4, "healthy", false)
	release := mustAcquireAll(t, g, "a", "b")
	defer release()
	keyless := mustAcquire(t, g, "")
	defer keyless()
	if g.localInFlight != 0 {
		t.Errorf("%d local slots in flight, want 0: calls don't take local slots", g.localInFlight)
	}
	if _, ok := g.keys[""]; ok {
		t.Error("a keyless call created key state")
	}
	// A second keyless call at local capacity 1 still goes through.
	mustAcquire(t, g, "")()
}

// Calls on the same two keys, in either order, never get stuck: nothing blocks,
// and a deferred call holds nothing.
func TestAcquireAllNeverDeadlocks(t *testing.T) {
	g, _ := testGovernor(t, Options{MaxConcurrentReconciles: 4})
	var wg sync.WaitGroup
	for i := range 8 {
		keys := []string{"a", "b"}
		if i%2 == 0 {
			keys = []string{"b", "a"}
		}
		wg.Go(func() {
			for range 200 {
				if release, d := g.AcquireAll(context.Background(), keys); d == nil {
					release()
				}
			}
		})
	}
	wg.Wait()
	mustAcquireAll(t, g, "a", "b")()
	if st := g.keys["a"]; st.inFlight != 0 {
		t.Errorf("a has %d calls in flight after every release, want 0", st.inFlight)
	}
}

// deferralCounter records the keys of the deferrals it is told about.
type deferralCounter struct {
	keys []string
}

func (c *deferralCounter) IncRateLimitDeferral(_ context.Context, key string, _ DeferReason) {
	c.keys = append(c.keys, key)
}

func (*deferralCounter) IncRateLimitSignal(context.Context, string, SignalKind) {}

func (*deferralCounter) ObserveRateLimit(func() Snapshot) {}

// The deferrals metric counts the key that stopped the call, so a dashboard
// shows which of an object's endpoints is the problem.
func TestDeferralMetricNamesTheKeyThatStoppedTheCall(t *testing.T) {
	m := &deferralCounter{}
	g, _ := testGovernor(t, Options{MaxConcurrentReconciles: 2, Metrics: m})
	g.Observe(Signal{Key: "b", Throttled: true, RetryAfter: time.Minute})

	if _, d := g.AcquireAll(context.Background(), []string{"a", "b"}); d == nil {
		t.Fatal("the call was not deferred, so this test shows nothing")
	}
	if !slices.Equal(m.keys, []string{"b"}) {
		t.Errorf("the metric counted keys %q, want just b", m.keys)
	}
}
