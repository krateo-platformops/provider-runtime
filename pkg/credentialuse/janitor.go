package credentialuse

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/util/workqueue"
)

// The label janitor (§10.1, L4). R3 removes a kind's marker label with its
// last finalizer, and the held label with the last credential-use
// finalizer. If a third party strips the finalizers but leaves a label, a
// tracker whose informer sees the labelled object removes the label, with
// JanitorPatch, which tests the resourceVersion it read and so can't race a
// new A2.
//
// Its queue is client-go's rate-limited workqueue, unlike the (K, N) batch
// queue: an object needs a check, not a batch whose contents accumulate, so
// deduplication, delay and backoff are all it needs. NewShared creates it,
// so that what the informers see before the write loops start, as on a
// standby before election, waits for them.

const (
	janitorBackoffBase = time.Second
	janitorBackoffMax  = 5 * time.Minute
)

// janitorWake is never an object: the janitor's worker adds it to wake its
// own Get when the write loops' context ends, since a workqueue's Get takes
// no context.
var janitorWake = ID{Kind: "\x00wake"}

func newJanitorQueue(clk sharedClock) workqueue.TypedRateLimitingInterface[ID] {
	return workqueue.NewTypedRateLimitingQueueWithConfig(
		workqueue.NewTypedItemExponentialFailureRateLimiter[ID](janitorBackoffBase, janitorBackoffMax),
		workqueue.TypedRateLimitingQueueConfig[ID]{Name: "credential-use-janitor", Clock: clk},
	)
}

// janitorHandler queues every object protected informer p delivers with a
// label the janitor would remove.
type janitorHandler struct {
	s *Shared
	p *protectedInformer
}

func (h janitorHandler) check(obj any) {
	m, ok := metadataOf(obj)
	if !ok {
		return
	}
	if h.s.needsJanitor(m) {
		h.s.janitor.Add(ID{Group: h.p.gk.Group, Kind: h.p.gk.Kind, Namespace: m.Namespace, Name: m.Name})
	}
}

func (h janitorHandler) OnAdd(obj any, _ bool)  { h.check(obj) }
func (h janitorHandler) OnUpdate(_, newObj any) { h.check(newObj) }
func (janitorHandler) OnDelete(any)             {}

// needsJanitor reports whether some tracked kind's JanitorPatch would remove
// a label from m.
func (s *Shared) needsJanitor(m metav1.Object) bool {
	for _, t := range s.trackerList() {
		if _, err := JanitorPatch(m, t.gk); err == nil {
			return true
		}
	}
	return false
}

// janitorState remembers, for each object whose janitor patch found the
// state changed, the resourceVersion the patch was built from (§6.2 L1).
type janitorState struct {
	mu        sync.Mutex
	builtFrom map[ID]string
}

// runJanitor works the janitor's queue until ctx ends.
func (s *Shared) runJanitor(ctx context.Context) {
	q := s.janitor
	stop := context.AfterFunc(ctx, func() { q.Add(janitorWake) })
	defer stop()
	for {
		id, shutdown := q.Get()
		if shutdown {
			return
		}
		if id == janitorWake {
			q.Forget(id)
			q.Done(id)
			if ctx.Err() != nil {
				return
			}
			continue
		}
		if ctx.Err() != nil {
			// Leave it for the next write loops.
			q.Done(id)
			q.Add(id)
			return
		}
		if err := s.clean(ctx, id); err != nil {
			s.logJanitor(id, err)
			q.AddRateLimited(id)
		} else {
			q.Forget(id)
		}
		q.Done(id)
	}
}

// clean reads id live and removes every label the tracked kinds' janitor
// patches would. A patch whose test fails goes back to the read, as R3's
// does, at most maxReleaseRounds times; one that fails against the very
// resourceVersion it was built from is a defect, reported, and not retried
// as contention (§6.2 L1).
func (s *Shared) clean(ctx context.Context, id ID) error {
	srv := s.newServer(schema.GroupVersionKind{})
	for range maxReleaseRounds {
		m, err := srv.Get(ctx, id.GroupKind(), id.Namespace, id.Name)
		switch Classify(err) {
		case ClassNone:
		case ClassNotFound:
			s.janitorState.forget(id)
			return nil
		default:
			return fmt.Errorf("reading it: %w", err)
		}
		if rv, ok := s.janitorState.built(id); ok && rv == m.ResourceVersion {
			s.janitorState.forget(id)
			s.janitorRejected(id, rv)
			return fmt.Errorf("%w: the janitor's patch of %s at resourceVersion %s", ErrReleaseRejected, id, rv)
		}
		changed := false
		for _, t := range s.trackerList() {
			pt, err := JanitorPatch(m, t.gk)
			if errors.Is(err, ErrNothingToClean) {
				continue
			}
			if err != nil {
				return err
			}
			out, err := srv.Patch(ctx, id.GroupKind(), id.Namespace, id.Name, pt.Type, pt.Data)
			switch ClassifyJSONPatch(err) {
			case ClassNone:
				m = out
			case ClassStateChanged:
				s.janitorState.build(id, m.ResourceVersion)
				changed = true
			case ClassNotFound:
				s.janitorState.forget(id)
				return nil
			default:
				return fmt.Errorf("removing its labels: %w", err)
			}
			if changed {
				break
			}
		}
		if !changed {
			s.janitorState.forget(id)
			return nil
		}
	}
	return fmt.Errorf("its state kept changing under %d janitor patches", maxReleaseRounds)
}

func (j *janitorState) build(id ID, rv string) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.builtFrom == nil {
		j.builtFrom = map[ID]string{}
	}
	j.builtFrom[id] = rv
}

func (j *janitorState) built(id ID) (string, bool) {
	j.mu.Lock()
	defer j.mu.Unlock()
	rv, ok := j.builtFrom[id]
	return rv, ok
}

func (j *janitorState) forget(id ID) {
	j.mu.Lock()
	defer j.mu.Unlock()
	delete(j.builtFrom, id)
}

// janitorRejected reports a janitor patch rejected by an unchanged object,
// as R3's (§6.2 L1), on every tracked kind's metrics: the object's labels
// are shared, so no one kind owns the patch.
func (s *Shared) janitorRejected(id ID, rv string) {
	for _, t := range s.trackerList() {
		t.proto.reports.ReleaseRejected(t.gk, id, rv)
	}
}

func (s *Shared) logJanitor(id ID, err error) {
	for _, t := range s.trackerList() {
		t.warn("credential-use: the label janitor couldn't clean an object; it retries with backoff",
			"object", id.String(), "error", err.Error())
		return
	}
}
