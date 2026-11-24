package credentialuse

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	testingclock "k8s.io/utils/clock/testing"
)

// The queue of a process whose write loops don't run (§6.2): nothing drains it,
// and the names that the holders' records have had since it started would be in it
// for as long as it waits for election. It keeps 65,536 names at most, and what the
// loops find at their start stands in for the others.

// waitingNames is how many names wait in q, in all and for each key, a namespace's
// list aside, and how many entries do in all.
func waitingNames(q *batchQueue) (names int, by map[batchKey]int, all int) {
	q.mu.Lock()
	defer q.mu.Unlock()
	by = map[batchKey]int{}
	for k, set := range q.pending {
		for id := range set {
			all++
			if id != fillAll {
				names++
				by[k]++
			}
		}
	}
	return names, by, all
}

// A million names of one holder, added while no write loops run, leave no more than
// 65,536 at any time, and then its namespace's list alone.
func TestAStandbysQueueIsBounded(t *testing.T) {
	q := newBatchQueue(testingclock.NewFakeClock(time.Now()))
	k := batchKey{widgetGK, "ns"}
	most := 0
	for i := range 250 {
		ids := make([]ID, 4000)
		for j := range ids {
			ids[j] = secretID("ns", fmt.Sprintf("s%d-%d", i, j))
		}
		q.Add(k, ids...)
		if _, _, all := waitingNames(q); all > most {
			most = all
		}
	}
	if most > 1<<16+1 {
		t.Errorf("%d objects waited at a time, want 65,536 at most, and the namespace's list", most)
	}
	if _, ids, _ := q.Get(context.Background()); !slices.Equal(ids, []ID{fillAll}) {
		t.Errorf("the batch is of %d objects, want the namespace's list alone", len(ids))
	}
}

// A flood collapses its own key, what is known to be held in it too, which the start's
// rescan finds again, and leaves the others': those of other namespaces, and what
// another kind's holders name in its own.
func TestAFloodCollapsesItsOwnNamespace(t *testing.T) {
	q := newBatchQueue(testingclock.NewFakeClock(time.Now()))
	flood := batchKey{widgetGK, "flood"}
	honest := []batchKey{{widgetGK, "ns1"}, {widgetGK, "ns2"}, {gadgetGK, "flood"}}
	for _, k := range honest {
		q.Add(k, someIDs(k.Namespace, "honest", 100)...)
	}
	q.AddHeld(flood, someIDs("flood", "held", 10)...)
	for i := range 40 {
		q.Add(flood, someIDs("flood", fmt.Sprintf("s%d-", i), 4000)...)
	}
	_, by, _ := waitingNames(q)
	if by[flood] != 0 {
		t.Errorf("%d names of the flood wait, want none", by[flood])
	}
	for _, k := range honest {
		if by[k] != 100 {
			t.Errorf("%v has %d names waiting, want its 100", k, by[k])
		}
	}
	q.Add(honest[0], secretID("ns1", "later"))
	if _, by, _ := waitingNames(q); by[honest[0]] != 101 {
		t.Error("a name of another namespace isn't kept after the collapse")
	}
	var got []ID
	for q.Len() > 0 {
		k, ids, _ := q.Get(context.Background())
		if k == flood {
			got = ids
		}
	}
	if !slices.Equal(got, []ID{fillAll}) {
		t.Errorf("the flood's batch is of %d objects, want the namespace's list alone", len(got))
	}
}
