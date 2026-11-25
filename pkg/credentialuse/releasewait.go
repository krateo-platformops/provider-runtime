package credentialuse

import (
	"math/rand/v2"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/types"
)

// The wait before the release of an object being deleted (§6.4, H1; decided
// 2026-11-24). The API server removes such an object in two steps: it
// tests R3's patch, and then removes what carries that name. Between them,
// another process's R3 of the same incarnation also passes its tests, and
// removes whatever carries the name by then, such as a Secret recreated at once.
// Two processes learn of a holder that went, or of a grace's end, at the same
// moment: each waits a random time of its own before it reads the object and
// releases it, so they seldom overlap. It is a mitigation, not a proof: §6.4 says
// what stays open.

// releaseWaitMax bounds the wait: a random time in [0, releaseWaitMax).
const releaseWaitMax = 2 * time.Second

// randomReleaseWait is the wait, in production.
func randomReleaseWait() time.Duration {
	//nolint:gosec // it spreads two processes of one provider in time; nothing here is secret, and the global source is seeded at random per process
	return rand.N(releaseWaitMax) // #nosec G404 -- it spreads two processes of one provider in time; nothing here is secret, and the global source is seeded at random per process
}

// releaseWaits are the waits of the batches' objects: when each ends. An entry
// goes when a batch decides its object, so that the next release of it waits
// again; a batch that fails keeps it, and its retry doesn't wait twice. It is of
// one incarnation: another of the name waits for itself. The timers are the
// queue's, so they hold no worker.
type releaseWaits struct {
	mu   sync.Mutex
	ends map[batchKey]map[ID]waitEnd
}

// waitEnd is when the wait of the object with uid ends.
type waitEnd struct {
	uid types.UID
	end time.Time
}

// over reports whether o's wait in k has ended by now. One that hasn't begun
// hasn't.
func (w *releaseWaits) over(k batchKey, o Waiting, now time.Time) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	e, ok := w.ends[k][o.ID]
	return ok && e.uid == o.UID && !now.Before(e.end)
}

// begun reports whether o's wait in k has begun.
func (w *releaseWaits) begun(k batchKey, o Waiting) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	e, ok := w.ends[k][o.ID]
	return ok && e.uid == o.UID
}

// begin begins o's wait in k, until end, in place of an earlier incarnation's.
func (w *releaseWaits) begin(k batchKey, o Waiting, end time.Time) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.ends == nil {
		w.ends = map[batchKey]map[ID]waitEnd{}
	}
	if w.ends[k] == nil {
		w.ends[k] = map[ID]waitEnd{}
	}
	w.ends[k][o.ID] = waitEnd{o.UID, end}
}

// settle ends what is kept of ids' waits in k, which a batch decided.
func (w *releaseWaits) settle(k batchKey, ids ...ID) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, id := range ids {
		delete(w.ends[k], id)
	}
	if len(w.ends[k]) == 0 {
		delete(w.ends, k)
	}
}

// clear drops every wait, when the write loops stop: the next ones wait again.
func (w *releaseWaits) clear() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.ends = nil
}

// mustWait reports whether the release of o in k must wait, or go on waiting.
func (s *Shared) mustWait(k batchKey, o Waiting) bool {
	return s.releaseWait != nil && !s.waits.over(k, o, s.clock.Now())
}

// wait begins the waits of objs, each of its own length, and queues each again
// when it ends. One that has begun goes on as it is.
func (s *Shared) wait(k batchKey, objs []Waiting) {
	for _, o := range objs {
		if s.waits.begun(k, o) {
			continue // the queue gives k to one worker at a time
		}
		d := s.releaseWait()
		s.waits.begin(k, o, s.clock.Now().Add(d))
		s.queue.AddAfter(k, d, o.ID)
	}
}
