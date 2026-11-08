package scaletozero

import (
	"testing"
	"time"
)

func TestDecide(t *testing.T) {
	now := time.Date(2026, 11, 7, 12, 0, 0, 0, time.UTC)
	const hold = 10 * time.Minute

	cases := []struct {
		name         string
		exists       bool
		lastSeen     time.Time
		holdDown     time.Duration
		wantReplicas int32
		wantRequeue  time.Duration
	}{
		{"an instance keeps it up", true, now.Add(-time.Hour), hold, 1, 0},
		{"an instance keeps it up with a zero lastSeen", true, time.Time{}, hold, 1, 0},
		{"an instance keeps it up even with a lastSeen far in the past", true, now.Add(-24 * time.Hour), hold, 1, 0},
		{"first sight of an empty Kind starts the hold-down", false, time.Time{}, hold, 1, hold},
		{"empty just after lastSeen waits the whole hold-down", false, now, hold, 1, hold},
		{"empty within the hold-down waits the rest of it", false, now.Add(-4 * time.Minute), hold, 1, 6 * time.Minute},
		{"empty a nanosecond before the end waits a nanosecond", false, now.Add(-hold + time.Nanosecond), hold, 1, time.Nanosecond},
		{"empty for exactly the hold-down scales to zero", false, now.Add(-hold), hold, 0, 0},
		{"empty past the hold-down scales to zero", false, now.Add(-time.Hour), hold, 0, 0},
		{"a clock gone backwards stays up for one hold-down", false, now.Add(time.Minute), hold, 1, hold},
		{"a lastSeen far in the future stays up and looks again within one hold-down", false, now.Add(365 * 24 * time.Hour), hold, 1, hold},
		{"a zero holdDown never scales down", false, now.Add(-time.Hour), 0, 1, 0},
		{"a negative holdDown never scales down", false, now.Add(-time.Hour), -time.Minute, 1, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gotReplicas, gotRequeue := Decide(tc.exists, tc.lastSeen, now, tc.holdDown)
			if gotReplicas != tc.wantReplicas || gotRequeue != tc.wantRequeue {
				t.Errorf("Decide(%v, %v, now, %v) = (%d, %v), want (%d, %v)",
					tc.exists, tc.lastSeen, tc.holdDown, gotReplicas, gotRequeue, tc.wantReplicas, tc.wantRequeue)
			}
		})
	}
}

func TestNextLastSeen(t *testing.T) {
	now := time.Date(2026, 11, 7, 12, 0, 0, 0, time.UTC)
	past := now.Add(-time.Hour)
	future := now.Add(time.Hour)

	cases := []struct {
		name     string
		exists   bool
		lastSeen time.Time
		want     time.Time
	}{
		{"an instance moves lastSeen to now", true, past, now},
		{"an instance moves a future lastSeen to now", true, future, now},
		{"first sight of an empty Kind starts the hold-down now", false, time.Time{}, now},
		{"an empty Kind keeps lastSeen", false, past, past},
		// Resetting a future lastSeen to now would restart the hold-down
		// from a clock that is behind; it is kept.
		{"an empty Kind keeps a future lastSeen", false, future, future},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := nextLastSeen(tc.exists, tc.lastSeen, now); !got.Equal(tc.want) {
				t.Errorf("nextLastSeen(%v, %v, now) = %v, want %v", tc.exists, tc.lastSeen, got, tc.want)
			}
		})
	}
}

func TestReasonFor(t *testing.T) {
	cases := []struct {
		exists   bool
		replicas int32
		want     Reason
	}{
		{true, 1, ReasonActive},
		{false, 1, ReasonIdleHoldDown},
		{false, 0, ReasonIdle},
	}
	for _, tc := range cases {
		if got := reasonFor(tc.exists, tc.replicas); got != tc.want {
			t.Errorf("reasonFor(%v, %d) = %q, want %q", tc.exists, tc.replicas, got, tc.want)
		}
	}
}

// The four reasons are a contract with the owners, which may surface them in
// their own status: the literals are pinned.
func TestReasonLiterals(t *testing.T) {
	for got, want := range map[Reason]string{
		ReasonActive:       "Active",
		ReasonIdleHoldDown: "IdleHoldDown",
		ReasonIdle:         "Idle",
		ReasonUnknown:      "Unknown",
	} {
		if string(got) != want {
			t.Errorf("reason %q, want %q", got, want)
		}
	}
}
