package costmodel

import "math"

// Config configures the model.
type Config struct {
	CPU CPUConfig
}

// Result is one sample's outcome.
type Result struct {
	// Capacity is the whole number of slots to publish.
	Capacity int
	// Slots is S, the real value before rounding.
	Slots float64
	// Carry is the error-diffusion remainder after this sample, in [0, 1).
	Carry float64
	// Held reports that pressure held a rise (§6).
	Held bool
	CPU  CPUResult
}

// Model combines the inputs into slots (design §4.5, §6, §7). The sampler
// goroutine owns it.
type Model struct {
	cpu  *CPU
	prev float64
	// capacity is the whole number published by the last sample, in force
	// during the next.
	capacity int
	carry    float64
	inRun    bool
	sOnset   float64
}

// NewModel returns a model at the floor of one slot.
func NewModel(cfg Config) *Model {
	return &Model{cpu: NewCPU(cfg.CPU), prev: 1, capacity: 1}
}

// Slots reports S as of the last sample.
func (m *Model) Slots() float64 { return m.prev }

// Step folds in one sample. ceiling is the most slots there may be, at least 1.
func (m *Model) Step(s Sample, ceiling int) Result {
	ceiling = max(ceiling, 1)
	r := m.cpu.Step(s, m.prev, m.capacity, ceiling)
	S := r.Slots
	// §7: untrusted rises, and rises towards the ceiling when the CPU isn't
	// binding, are one slot per reconcile that ended in the sample and that
	// ĉ's denominator counts (§17.8, item 5).
	if r.Paced && S > m.prev {
		S = math.Min(S, m.prev+float64(r.PaceCompletions))
	}
	// §6: through a run of samples over the threshold, the slots are held at
	// min(S_onset, S_model).
	held := false
	if r.Blocked {
		if !m.inRun {
			m.inRun, m.sOnset = true, m.prev
		}
		if S > m.sOnset {
			S, held = m.sOnset, true
		}
	} else {
		m.inRun = false
	}
	clamped := false
	if S <= 1 {
		S, clamped = 1, true
	}
	if S >= float64(ceiling) {
		S, clamped = float64(ceiling), true
	}
	// §4.5: error diffusion. The carry resets on a limit change, a clamp and an
	// estimate's restart; the binding input's change joins with memory (PR 2).
	if clamped || r.LimitChanged || r.Restarted {
		m.carry = 0
	}
	whole := math.Floor(S + m.carry)
	m.carry = S + m.carry - whole
	capacity := int(whole)
	capacity = min(max(capacity, 1), ceiling)
	m.prev = S
	m.capacity = capacity
	return Result{Capacity: capacity, Slots: S, Carry: m.carry, Held: held, CPU: r}
}
