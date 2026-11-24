package credentialuse

import (
	"context"
	"sync"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// asks is what a ref asks for that the first ref of the same object doesn't
// (§9.1, Ref.AsksMoreThan).
type asks struct{ hold, next bool }

// conflictSet remembers, in memory, what was reported of each holder's refs
// that conflict (§9.1), so that it is reported once. A nil set remembers
// nothing.
type conflictSet struct {
	mu sync.Mutex
	m  map[types.UID]map[ID]asks
}

func newConflictSet() *conflictSet { return &conflictSet{m: map[types.UID]map[ID]asks{}} }

// told is what was reported of holder's refs of id.
func (c *conflictSet) told(holder types.UID, id ID) asks {
	if c == nil {
		return asks{}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.m[holder][id]
}

// tell adds a to what was reported of holder's refs of id.
func (c *conflictSet) tell(holder types.UID, id ID, a asks) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.m[holder] == nil {
		c.m[holder] = map[ID]asks{}
	}
	t := c.m[holder][id]
	c.m[holder][id] = asks{t.hold || a.hold, t.next || a.next}
}

// keep forgets what was reported of holder and isn't in asked, what its refs
// ask for now. So a holder keeps no more than its refs ask for.
func (c *conflictSet) keep(holder types.UID, asked map[ID]asks) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for id, t := range c.m[holder] {
		a := asked[id]
		if a = (asks{t.hold && a.hold, t.next && a.next}); a == (asks{}) {
			delete(c.m[holder], id)
		} else {
			c.m[holder][id] = a
		}
	}
	if len(c.m[holder]) == 0 {
		delete(c.m, holder)
	}
}

// forget drops everything reported of holder.
func (c *conflictSet) forget(holder types.UID) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.m, holder)
}

// couldHold reports whether a pass of obj could hold id, as far as the Disown
// switch (§7.4) and consent (§11.3) go: a ref that asked to is worth a report
// only then (§9.1). In another namespace it reads the target's consent, unless
// the caches have it, and an unknown one, or a target that is gone, is no.
func (p *protocol) couldHold(ctx context.Context, obj metav1.Object, id ID) bool {
	if p.isDisowned(id.GroupKind()) {
		return false
	}
	if id.Namespace == obj.GetNamespace() {
		return true
	}
	g := p.gate(ctx, obj, id, true)
	return g == gateHoldLive || g == gateHoldCached
}
