package credentialuse

import (
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/utils/clock"
)

// The kinds a release couldn't map (§6.2). A mapper that finds new kinds by
// itself looks for one again, which costs a discovery, whenever it is asked; and
// what a record names may be of any kind, or of none. So a miss is remembered for
// missedKindTime from when it was made, whatever asks for it meanwhile, and an
// object of its kind fails as it would have. Release batches alone ask it, so it
// makes no pass wait for a kind that is served again.
const (
	missedKindTime = time.Minute
	maxMissedKinds = 256 // the oldest goes first
)

// missedKinds is the memory, one for all the holder kinds of a Shared. Nil
// remembers nothing.
type missedKinds struct {
	mu sync.Mutex
	at map[schema.GroupKind]missedKind
}

type missedKind struct {
	err error
	at  time.Time
}

// known is the miss of gk that is remembered, if any. It reads c only for one
// that is there.
func (m *missedKinds) known(gk schema.GroupKind, c clock.PassiveClock) error {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	missed, ok := m.at[gk]
	m.mu.Unlock()
	if !ok || c.Since(missed.at) >= missedKindTime {
		return nil
	}
	return missed.err
}

// add remembers that gk couldn't be mapped at now, for err.
func (m *missedKinds) add(gk schema.GroupKind, err error, now time.Time) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.at == nil {
		m.at = map[schema.GroupKind]missedKind{}
	}
	m.at[gk] = missedKind{err, now}
	if len(m.at) <= maxMissedKinds {
		return
	}
	oldest := gk
	for k, missed := range m.at {
		if missed.at.Before(m.at[oldest].at) {
			oldest = k
		}
	}
	delete(m.at, oldest)
}
