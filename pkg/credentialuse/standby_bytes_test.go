package credentialuse

import (
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/runtime/schema"
	testingclock "k8s.io/utils/clock/testing"
)

// What bounds the queue of a process without write loops is its memory (§6.2):
// whatever the names are, whichever line they wait in, and however many keys they
// make.

// waitingBytes is how long the strings are of what waits in q.
func waitingBytes(q *batchQueue) int {
	q.mu.Lock()
	defer q.mu.Unlock()
	n := 0
	for k, set := range q.pending {
		n += len(k.Namespace)
		for id := range set {
			n += len(id.Group) + len(id.Kind) + len(id.Namespace) + len(id.Name)
		}
	}
	return n
}

// heap is how much of the heap is in use.
func heap() int {
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return int(m.HeapAlloc)
}

// 1,000 names as long as a record allows, 250 MB of them, don't stay: 32 MiB of names
// do, at most; and the key with the most bytes collapses, though another has more
// names.
func TestLongNamesAreBoundedByTheirBytes(t *testing.T) {
	q := newBatchQueue(testingclock.NewFakeClock(time.Now()))
	long, honest := batchKey{widgetGK, "long"}, batchKey{widgetGK, "honest"}
	before := heap()
	q.Add(honest, someIDs("honest", "s", 2000)...)
	most := 0
	for i := range 1000 {
		q.Add(long, secretID("long", fmt.Sprintf("%06d-%s", i, strings.Repeat("x", 250000))))
		most = max(most, waitingBytes(q))
	}
	if most > 33<<20 {
		t.Errorf("%d MiB of names waited at a time, want 33 at most", most>>20)
	}
	if grew := heap() - before; grew > 96<<20 {
		t.Errorf("the heap grew by %d MiB, want 96 at most", grew>>20)
	}
	if _, by, _ := waitingNames(q); by[honest] != 2000 || by[long] != 0 {
		t.Errorf("%d honest names wait, and %d long ones; want the 2,000, and none", by[honest], by[long])
	}
	runtime.KeepAlive(q)
}

// What is known to be held counts too: 200,000 objects that were held and went leave
// no more than the bound.
func TestWhatWasHeldIsBounded(t *testing.T) {
	q := newBatchQueue(testingclock.NewFakeClock(time.Now()))
	k := batchKey{widgetGK, "ns"}
	most := 0
	for i := range 200 {
		q.AddHeld(k, someIDs("ns", fmt.Sprintf("s%d-", i), 1000)...)
		_, _, all := waitingNames(q)
		most = max(most, all)
	}
	if most > 1<<16+1 {
		t.Errorf("%d objects waited at a time, want 65,536 at most, and the namespace's list", most)
	}
}

// 100,000 namespaces of one kind, as forged finalizers may name, leave no more than
// 16,384 keys, and no memory that grows with them; another kind's are kept.
func TestTheKeysAreBounded(t *testing.T) {
	q := newBatchQueue(testingclock.NewFakeClock(time.Now()))
	for i := range 100 {
		ns := fmt.Sprintf("honest%d", i)
		q.Add(batchKey{widgetGK, ns}, secretID(ns, "s"))
	}
	before, most := heap(), 0
	for i := range 100000 {
		ns := fmt.Sprintf("forged%d", i)
		q.AddHeld(batchKey{gadgetGK, ns}, secretID("ns", "s"))
		q.mu.Lock()
		most = max(most, len(q.pending), len(q.lines), len(q.queued), len(q.ready))
		q.mu.Unlock()
	}
	if most > 1<<14 {
		t.Errorf("%d keys waited at a time, want 16,384 at most", most)
	}
	if grew := heap() - before; grew > 48<<20 {
		t.Errorf("the heap grew by %d MiB, want 48 at most", grew>>20)
	}
	kinds := map[schema.GroupKind]int{}
	q.mu.Lock()
	for k := range q.pending {
		kinds[k.Holder]++
	}
	q.mu.Unlock()
	if kinds[widgetGK] != 100 || kinds[gadgetGK] != 0 {
		t.Errorf("%v keys wait, want the 100 honest ones alone", kinds)
	}
	runtime.KeepAlive(q)
}
