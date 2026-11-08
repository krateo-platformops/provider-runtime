package ratelimit

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/krateo-platformops/provider-runtime/pkg/logging"
)

const day = 24 * time.Hour

// heldUntil asserts key is held by the governor and returns the deadline the
// Deferral names.
func heldUntil(t *testing.T, g *Governor, key string) time.Time {
	t.Helper()
	_, d := g.Acquire(context.Background(), key)
	if d == nil || d.Reason != DeferThrottled {
		t.Fatalf("Acquire(%q) = %+v, want a throttled deferral", key, d)
	}
	return d.Until
}

// A server can ask for any delay. Past MaxHold the hold is cut to MaxHold: a
// Retry-After of a day, or a reset time a year away, holds the key for 1h by
// default, and the gate opens when that hour is up.
func TestMaxHoldCutsAFarDeadline(t *testing.T) {
	cases := map[string]struct {
		maxHold *time.Duration
		signal  func(now time.Time) Signal
		want    time.Duration
	}{
		"RetryAfter a day, default cap": {
			signal: func(time.Time) Signal { return Signal{Key: "h", Throttled: true, RetryAfter: day} },
			want:   DefaultMaxHold,
		},
		"ResetAt a year away, default cap": {
			signal: func(now time.Time) Signal { return Signal{Key: "h", Throttled: true, ResetAt: now.AddDate(1, 0, 0)} },
			want:   DefaultMaxHold,
		},
		"RetryAfter a day, 10m cap": {
			maxHold: new(10 * time.Minute),
			signal:  func(time.Time) Signal { return Signal{Key: "h", Throttled: true, RetryAfter: day} },
			want:    10 * time.Minute,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			g, c := testGovernor(t, Options{MaxConcurrentReconciles: 4, MaxHold: tc.maxHold})
			start := c.now()
			g.Observe(tc.signal(start))

			if got, want := heldUntil(t, g, "h"), start.Add(tc.want); !got.Equal(want) {
				t.Errorf("Until = %v, want %v: the hold must be cut to the cap", got, want)
			}
			_, d := g.Acquire(context.Background(), "h")
			if d.RequeueAfter != tc.want {
				t.Errorf("RequeueAfter = %v, want %v", d.RequeueAfter, tc.want)
			}
			if s := g.Snapshot().Keys[0].SecondsToReset; s != tc.want.Seconds() {
				t.Errorf("seconds_to_reset = %v, want %v", s, tc.want.Seconds())
			}

			c.add(tc.want - time.Second)
			heldUntil(t, g, "h")
			c.add(time.Second)
			mustAcquire(t, g, "h")()
		})
	}
}

// A deadline at or under the cap is the server's, unchanged.
func TestMaxHoldLeavesADeadlineWithinTheCapAlone(t *testing.T) {
	for _, after := range []time.Duration{30 * time.Minute, DefaultMaxHold} {
		t.Run(after.String(), func(t *testing.T) {
			m := &clampCounter{}
			g, c := testGovernor(t, Options{MaxConcurrentReconciles: 4, Metrics: m})
			start := c.now()
			g.Observe(Signal{Key: "h", Throttled: true, RetryAfter: after})

			if got, want := heldUntil(t, g, "h"), start.Add(after); !got.Equal(want) {
				t.Errorf("Until = %v, want the server's %v", got, want)
			}
			if n := m.count("h"); n != 0 {
				t.Errorf("counted %d clamps for a deadline within the cap, want 0", n)
			}
		})
	}
}

// MaxHold 0 turns the cap off: the server's deadline holds in full.
func TestMaxHoldZeroIsNoCap(t *testing.T) {
	m := &clampCounter{}
	g, c := testGovernor(t, Options{MaxConcurrentReconciles: 4, MaxHold: new(time.Duration(0)), Metrics: m})
	start := c.now()
	g.Observe(Signal{Key: "h", Throttled: true, RetryAfter: day})

	if got, want := heldUntil(t, g, "h"), start.Add(day); !got.Equal(want) {
		t.Errorf("Until = %v, want the server's %v with no cap", got, want)
	}
	c.add(day - time.Second)
	heldUntil(t, g, "h")
	if n := m.count("h"); n != 0 {
		t.Errorf("counted %d clamps with no cap, want 0", n)
	}
}

// After a cut hold one call goes through. If the server still refuses it, the
// key is held again, for the cap again: the cap bounds each hold, not how long
// a server may go on refusing.
func TestARefusalAfterACutHoldHoldsTheKeyAgain(t *testing.T) {
	g, c := testGovernor(t, Options{MaxConcurrentReconciles: 4})
	start := c.now()
	g.Observe(Signal{Key: "h", Throttled: true, RetryAfter: day})

	c.add(DefaultMaxHold)
	release := mustAcquire(t, g, "h")
	// The probe is the only call: capacity is still 1 after the refusal.
	if _, d := g.Acquire(context.Background(), "h"); d == nil || d.Reason != DeferAtCapacity {
		t.Fatalf("a second call alongside the probe = %+v, want AtCapacity", d)
	}
	g.Observe(Signal{Key: "h", Throttled: true, RetryAfter: day})
	release()

	if got, want := heldUntil(t, g, "h"), start.Add(2*DefaultMaxHold); !got.Equal(want) {
		t.Errorf("Until after the second refusal = %v, want %v", got, want)
	}
	if got := g.Capacity("h"); got != 1 {
		t.Errorf("capacity = %d after the second refusal, want 1", got)
	}
}

// An earlier deadline never shortens a cut hold, and a later far one extends
// it only to its own now+MaxHold.
func TestAnEarlierDeadlineNeverShortensACutHold(t *testing.T) {
	g, c := testGovernor(t, Options{MaxConcurrentReconciles: 4})
	start := c.now()
	g.Observe(Signal{Key: "h", Throttled: true, RetryAfter: day})

	c.add(10 * time.Minute)
	g.Observe(Signal{Key: "h", Throttled: true, RetryAfter: time.Second})
	if got, want := heldUntil(t, g, "h"), start.Add(DefaultMaxHold); !got.Equal(want) {
		t.Errorf("Until = %v after a shorter signal, want the cut hold's %v", got, want)
	}

	g.Observe(Signal{Key: "h", Throttled: true, RetryAfter: day})
	if got, want := heldUntil(t, g, "h"), c.now().Add(DefaultMaxHold); !got.Equal(want) {
		t.Errorf("Until = %v after a later far signal, want %v", got, want)
	}
}

// The first cut for a key is logged at Info with the key, the deadline the
// server asked for and the cap. A server that keeps asking for too long is not
// logged again until the key has gone a whole RecoveryInterval clean.
func TestACutIsLoggedOncePerKey(t *testing.T) {
	logs := &kvLog{}
	recovery := 30 * time.Second
	g, c := testGovernor(t, Options{MaxConcurrentReconciles: 4, RecoveryInterval: recovery, Logger: logs})
	start := c.now()

	g.Observe(Signal{Key: "a", Throttled: true, RetryAfter: day})
	if n := logs.count(); n != 1 {
		t.Fatalf("logged %d lines for the first cut, want 1", n)
	}
	got := logs.entry(0)
	want := map[string]string{
		"key":               "a",
		"requestedDeadline": start.Add(day).UTC().Format(time.RFC3339),
		"maxHold":           "1h0m0s",
		"holdUntil":         start.Add(DefaultMaxHold).UTC().Format(time.RFC3339),
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("log %s = %q, want %q (entry %v)", k, got[k], v, got)
		}
	}

	// The same key, again during the hold and after it, never clean between.
	g.Observe(Signal{Key: "a", Throttled: true, RetryAfter: day})
	c.add(DefaultMaxHold)
	g.Observe(Signal{Key: "a", Throttled: true, RetryAfter: day})
	if n := logs.count(); n != 1 {
		t.Errorf("logged %d lines while the key never went clean, want 1", n)
	}

	// Another key is logged on its own.
	g.Observe(Signal{Key: "b", Throttled: true, RetryAfter: day})
	if n := logs.count(); n != 2 || logs.entry(1)["key"] != "b" {
		t.Errorf("logs = %v, want a second line for key b", logs.entries)
	}

	// A refusal within the cap is not logged, and it restarts the clean
	// interval like any other: a cut just short of a whole RecoveryInterval
	// after its hold isn't logged either.
	c.add(DefaultMaxHold)
	g.Observe(Signal{Key: "a", Throttled: true, RetryAfter: time.Second})
	c.add(time.Second + recovery - time.Nanosecond)
	g.Observe(Signal{Key: "a", Throttled: true, RetryAfter: day})
	if n := logs.count(); n != 2 {
		t.Errorf("logged %d lines, want 2: key a was clean for less than a RecoveryInterval", n)
	}

	// Clean for a whole RecoveryInterval after the hold: the next cut is a new
	// episode and is logged.
	c.add(DefaultMaxHold + recovery)
	g.Observe(Signal{Key: "a", Throttled: true, RetryAfter: day})
	if n := logs.count(); n != 3 || logs.entry(2)["key"] != "a" {
		t.Errorf("logs = %v, want a third line for key a after it went clean", logs.entries)
	}
}

// Every cut is counted per key, logged or not.
func TestACutIsCounted(t *testing.T) {
	m := &clampCounter{}
	g, c := testGovernor(t, Options{MaxConcurrentReconciles: 4, Metrics: m})

	g.Observe(Signal{Key: "a", Throttled: true, RetryAfter: day})
	g.Observe(Signal{Key: "a", Throttled: true, ResetAt: c.now().AddDate(1, 0, 0)})
	g.Observe(Signal{Key: "a", Throttled: true, RetryAfter: time.Minute})
	g.Observe(Signal{Key: "b", Throttled: true, RetryAfter: day})
	// Advisory signals hold nothing, so there is nothing to cut.
	g.Observe(Signal{Key: "c", RetryAfter: day})

	if a, b, cc := m.count("a"), m.count("b"), m.count("c"); a != 2 || b != 1 || cc != 0 {
		t.Errorf("clamps counted a=%d b=%d c=%d, want 2, 1 and 0", a, b, cc)
	}
}

func TestNewRejectsANegativeMaxHold(t *testing.T) {
	if _, err := New(Options{MaxConcurrentReconciles: 1, MaxHold: new(-time.Second)}); err == nil {
		t.Error("New accepted a negative MaxHold")
	}
	for _, ok := range []*time.Duration{nil, new(time.Duration(0)), new(time.Minute)} {
		if _, err := New(Options{MaxConcurrentReconciles: 1, MaxHold: ok}); err != nil {
			t.Errorf("New rejected MaxHold %v: %v", ok, err)
		}
	}
}

// clampCounter counts the clamps it is told about, by key.
type clampCounter struct {
	mu   sync.Mutex
	keys map[string]int
}

func (*clampCounter) IncRateLimitDeferral(context.Context, string, DeferReason) {}
func (*clampCounter) IncRateLimitSignal(context.Context, string, SignalKind)    {}
func (*clampCounter) ObserveRateLimit(func() Snapshot)                          {}
func (c *clampCounter) IncRateLimitHoldClamped(_ context.Context, key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.keys == nil {
		c.keys = map[string]int{}
	}
	c.keys[key]++
}

func (c *clampCounter) count(key string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.keys[key]
}

// kvLog keeps the Info lines about a cut hold, with their key-value pairs.
type kvLog struct {
	mu      sync.Mutex
	entries []map[string]string
}

func (l *kvLog) Info(msg string, kv ...any) {
	if msg != "Server asked to hold a rate-limit key past the maximum hold: capping the hold" {
		return
	}
	e := map[string]string{}
	for i := 0; i+1 < len(kv); i += 2 {
		e[fmt.Sprint(kv[i])] = fmt.Sprint(kv[i+1])
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.entries = append(l.entries, e)
}
func (l *kvLog) Debug(string, ...any)             {}
func (l *kvLog) Warn(string, ...any)              {}
func (l *kvLog) Error(error, string, ...any)      {}
func (l *kvLog) WithName(string) logging.Logger   { return l }
func (l *kvLog) WithValues(...any) logging.Logger { return l }

func (l *kvLog) count() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.entries)
}

func (l *kvLog) entry(i int) map[string]string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.entries[i]
}
