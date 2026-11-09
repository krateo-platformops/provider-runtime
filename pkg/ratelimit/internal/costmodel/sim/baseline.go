package sim

import (
	"math"

	"github.com/krateo-platformops/provider-runtime/pkg/ratelimit/internal/costmodel"
)

// Baseline is the utilization-feedback controller of design §17.6: a PI
// controller on the measured CPU against the limit, in velocity form, for the
// comparison only. It is not wired into the Governor, and it makes no claim of
// parity with Temporal's resource-based tuner, which gates slot issuance
// rather than setting a capacity.
type Baseline struct {
	// Kp and Ki are the gains, in slots per unit of error (a share of the
	// limit) and per unit of error-second.
	Kp, Ki float64
	// TauU is the first-order filter's time constant on U, in seconds; 0 is no
	// filter.
	TauU float64
	// Target is ρ, and PressureThreshold the block's threshold with no limit.
	Target, PressureThreshold float64
}

type baseline struct {
	cfg      Baseline
	prev     float64
	prevE    float64
	started  bool
	u        float64 // the filtered U
	carry    float64
	inRun    bool
	sOnset   float64
	capacity int
}

// NewBaseline returns the controller at the floor of one slot.
func NewBaseline(cfg Baseline) Controller {
	return &baseline{cfg: cfg, prev: 1, capacity: 1}
}

// Step folds in one sample (design §17.6).
func (b *baseline) Step(s costmodel.Sample, ceiling int) costmodel.Result {
	ceiling = max(ceiling, 1)
	dt := s.Dt.Seconds()
	if dt <= 0 {
		return costmodel.Result{Capacity: b.capacity, Slots: b.prev}
	}
	used := s.Usage.Seconds() / dt
	if !b.started || b.cfg.TauU <= 0 {
		b.u = used
	} else {
		b.u += dt / (b.cfg.TauU + dt) * (used - b.u)
	}
	var S float64
	stall := 100 * s.Stall.Seconds() / dt
	over := s.StallPresent && stall > b.cfg.PressureThreshold
	held := false
	if s.Limit == 0 {
		// With no CPU limit the CPU loop doesn't run; the slots rise to the
		// ceiling and the pressure block applies (design §5.1).
		S = float64(ceiling)
		if over {
			if !b.inRun {
				b.inRun, b.sOnset = true, b.prev
			}
			if S > b.sOnset {
				S, held = b.sOnset, true
			}
		} else {
			b.inRun = false
		}
		b.started = false
	} else {
		e := b.cfg.Target - b.u/s.Limit
		if !b.started {
			b.prevE, b.started = e, true
		}
		S = b.prev + b.cfg.Kp*(e-b.prevE) + b.cfg.Ki*e*dt
		b.prevE = e
	}
	clamped := false
	if S <= 1 {
		S, clamped = 1, true
	}
	if S >= float64(ceiling) {
		S, clamped = float64(ceiling), true
	}
	if clamped {
		b.carry = 0
	}
	whole := math.Floor(S + b.carry)
	b.carry = S + b.carry - whole
	b.capacity = min(max(int(whole), 1), ceiling)
	b.prev = S
	// CPU.Used is the filtered Ū the controller acts on, by which the gate
	// attributes its falls (plan §4.5).
	law := S
	if s.Limit == 0 {
		law = float64(ceiling)
	}
	return costmodel.Result{Capacity: b.capacity, Slots: S, Carry: b.carry, Held: held,
		CPU: costmodel.CPUResult{Slots: law, Used: b.u, Stall: stall, Blocked: over, InFlight: s.Books.SlotSeconds / dt}}
}
