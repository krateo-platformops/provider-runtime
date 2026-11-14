package credentialuse

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// Unit tests for the verified set (§6.3): the skip predicate, and the
// handlers that keep it.

var t0 = time.Date(2026, 11, 8, 16, 0, 0, 0, time.UTC)

func at(ms int) time.Time { return t0.Add(time.Duration(ms) * time.Millisecond) }

// holderAt is holder h1 at resourceVersion rv, with record (none if empty).
func holderAt(rv, record string) *metav1.ObjectMeta {
	m := &metav1.ObjectMeta{UID: "h1", Name: "h", Namespace: "ns", ResourceVersion: rv}
	if record != "" {
		m.Annotations = map[string]string{AnnotationRefs: record}
	}
	return m
}

func recordOf(t *testing.T, ids ...ID) string {
	t.Helper()
	v, err := EncodeRecord(ids)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func TestCanSkip(t *testing.T) {
	p := SecretRef("ns", "s").ID()
	rec := recordOf(t, p)
	cases := map[string]struct {
		setup func(v *verified)
		rv    string // the version the pass works from
		pUID  types.UID
		want  bool
	}{
		"nothing known":                     {func(*verified) {}, "2", "p1", false},
		"written, but no version delivered": {func(v *verified) { v.wrote("p1", "ns", at(5)) }, "2", "p1", false},
		"the version delivered, then written": {func(v *verified) {
			v.holderEvent("h1", holderAt("2", rec), at(1))
			v.wrote("p1", "ns", at(2))
		}, "2", "p1", true},
		// H1 of #59's review: the pass works from a version the informer
		// hasn't delivered, so the record may be a new run, whatever was
		// delivered before.
		"a later version than the delivered one": {func(v *verified) {
			v.holderEvent("h1", holderAt("2", rec), at(1))
			v.wrote("p1", "ns", at(2))
		}, "3", "p1", false},
		"an earlier version than the delivered one": {func(v *verified) {
			v.holderEvent("h1", holderAt("3", rec), at(1))
			v.wrote("p1", "ns", at(2))
		}, "2", "p1", false},
		// An A1 alone is no delivery: observed never makes a skip by itself.
		"observed by A1 only": {func(v *verified) {
			v.announced("h1", []ID{p}, nil, at(1))
			v.wrote("p1", "ns", at(2))
		}, "2", "p1", false},
		"written, then delivered: the write may predate the record": {func(v *verified) {
			v.wrote("p1", "ns", at(1))
			v.holderEvent("h1", holderAt("2", rec), at(2))
		}, "2", "p1", false},
		// recordSince is the first delivery of the run: a later version that
		// still names P (a status update, say) doesn't move it, so a restart
		// costs one A2, not one per update (§6.3).
		"a later version in the same run keeps the first time": {func(v *verified) {
			v.holderEvent("h1", holderAt("2", rec), at(1))
			v.wrote("p1", "ns", at(2))
			v.holderEvent("h1", holderAt("3", rec), at(3))
		}, "3", "p1", true},
		"the later of observed and recordSince counts": {func(v *verified) {
			v.holderEvent("h1", holderAt("2", rec), at(1))
			v.announced("h1", []ID{p}, nil, at(3))
			v.wrote("p1", "ns", at(2))
		}, "2", "p1", false},
		"the delivered version doesn't name P": {func(v *verified) {
			v.holderEvent("h1", holderAt("2", "[]"), at(1))
			v.wrote("p1", "ns", at(2))
		}, "2", "p1", false},
		// A run that ended and began again starts afresh: the old write
		// doesn't count for the new run.
		"away and back, both delivered": {func(v *verified) {
			v.holderEvent("h1", holderAt("2", rec), at(1))
			v.wrote("p1", "ns", at(2))
			v.holderEvent("h1", holderAt("3", "[]"), at(3))
			v.holderEvent("h1", holderAt("4", rec), at(4))
		}, "4", "p1", false},
		"another incarnation of P": {func(v *verified) {
			v.holderEvent("h1", holderAt("2", rec), at(1))
			v.wrote("p1", "ns", at(2))
		}, "2", "p2", false},
		"another holder namespace": {func(v *verified) {
			v.holderEvent("h1", holderAt("2", rec), at(1))
			v.wrote("p1", "other", at(2))
		}, "2", "p1", false},
		"no incarnation in the cache": {func(v *verified) {
			v.holderEvent("h1", holderAt("2", rec), at(1))
			v.wrote("p1", "ns", at(2))
		}, "2", "", false},
		"no resourceVersion": {func(v *verified) {
			v.holderEvent("h1", holderAt("", rec), at(1))
			v.wrote("p1", "ns", at(2))
		}, "", "p1", false},
		// §6.5 L2: a relist after the write means the record may have gone
		// and come back unseen.
		"a relist after the write": {func(v *verified) {
			v.holderEvent("h1", holderAt("2", rec), at(1))
			v.wrote("p1", "ns", at(2))
			v.listCompleted(at(3))
		}, "2", "p1", false},
		"a relist before the write": {func(v *verified) {
			v.listCompleted(at(1))
			v.holderEvent("h1", holderAt("2", rec), at(1))
			v.wrote("p1", "ns", at(2))
		}, "2", "p1", true},
		"released": {func(v *verified) {
			v.holderEvent("h1", holderAt("2", rec), at(1))
			v.wrote("p1", "ns", at(2))
			v.released("p1", "ns")
		}, "2", "p1", false},
		"released for another namespace": {func(v *verified) {
			v.holderEvent("h1", holderAt("2", rec), at(1))
			v.wrote("p1", "ns", at(2))
			v.released("p1", "other")
		}, "2", "p1", true},
		"an unparseable record names nothing": {func(v *verified) {
			v.holderEvent("h1", holderAt("2", "oops"), at(1))
			v.wrote("p1", "ns", at(2))
		}, "2", "p1", false},
		"an unparseable record ends the run": {func(v *verified) {
			v.holderEvent("h1", holderAt("2", rec), at(1))
			v.holderEvent("h1", holderAt("3", "oops"), at(2))
			v.holderEvent("h1", holderAt("4", rec), at(4))
			v.wrote("p1", "ns", at(3))
		}, "4", "p1", false},
		"the record dropped P (A1), delivered": {func(v *verified) {
			v.holderEvent("h1", holderAt("2", rec), at(1))
			v.announced("h1", nil, []ID{p}, at(2))
			v.holderEvent("h1", holderAt("3", "[]"), at(3))
			v.wrote("p1", "ns", at(4))
		}, "3", "p1", false},
		"a deleted holder": {func(v *verified) {
			v.holderEvent("h1", holderAt("2", rec), at(1))
			v.wrote("p1", "ns", at(2))
			v.holderEvent("h1", nil, at(3))
		}, "2", "p1", false},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			v := newVerified(widgetGK)
			tc.setup(v)
			if got := v.canSkip("h1", tc.rv, p, tc.pUID, "ns"); got != tc.want {
				t.Errorf("canSkip = %v, want %v", got, tc.want)
			}
		})
	}
}

// M1: away and back. Without the informer delivering the versions in
// between, the pass works from a version it hasn't delivered: no skip. Once
// it delivers them, the run starts again at the re-add's delivery, after the
// old write: no skip until a new write.
func TestCanSkipAwayAndBack(t *testing.T) {
	p := SecretRef("ns", "s").ID()
	rec := recordOf(t, p)
	v := newVerified(widgetGK)
	v.holderEvent("h1", holderAt("2", rec), at(1)) // the run starts
	v.wrote("p1", "ns", at(2))
	if !v.canSkip("h1", "2", p, "p1", "ns") {
		t.Fatal("the first run doesn't skip")
	}
	v.announced("h1", nil, []ID{p}, at(3)) // away, at version 3
	v.announced("h1", []ID{p}, nil, at(5)) // and back, at version 4
	if v.canSkip("h1", "4", p, "p1", "ns") {
		t.Error("a pass from the undelivered re-add skipped against a write from before it went away")
	}
	v.holderEvent("h1", holderAt("3", "[]"), at(6))
	v.holderEvent("h1", holderAt("4", rec), at(7))
	if v.canSkip("h1", "4", p, "p1", "ns") {
		t.Error("the delivered re-add skipped against a write from before it went away")
	}
	v.wrote("p1", "ns", at(8))
	if !v.canSkip("h1", "4", p, "p1", "ns") {
		t.Error("after a new write, the ref doesn't skip")
	}
}

// A version that ends P's run drops observed for P too (H1 of #59's
// review), so a later run can't lean on an A1 of an earlier one.
func TestHolderEventEndsObserved(t *testing.T) {
	p := SecretRef("ns", "s").ID()
	v := newVerified(widgetGK)
	v.announced("h1", []ID{p}, nil, at(1))
	v.holderEvent("h1", holderAt("2", recordOf(t, p)), at(2))
	if _, ok := v.observed["h1"][p]; !ok {
		t.Fatal("observed went while the run continued")
	}
	v.holderEvent("h1", holderAt("3", "[]"), at(3))
	if _, ok := v.observed["h1"][p]; ok {
		t.Error("a version without P left observed for P")
	}
	if _, ok := v.recordSince["h1"][p]; ok {
		t.Error("a version without P left recordSince for P")
	}
	// Forget, when the record is removed with the owner finalizer, drops
	// every observed entry of the holder.
	v.announced("h1", []ID{p}, nil, at(4))
	v.forgot("h1")
	if _, ok := v.observed["h1"]; ok {
		t.Error("forgot left observed")
	}
}

// The protected informer's handler only ever drops written entries (§6.3,
// §6.5 L6).
func TestProtectedEvent(t *testing.T) {
	f, _ := FinalizerName(widgetGK, "ns")
	fOther, _ := FinalizerName(widgetGK, "other")
	held := &metav1.ObjectMeta{UID: "p1", Finalizers: []string{f, fOther}}
	cases := map[string]struct {
		current       metav1.Object
		keepNS, keepO bool
	}{
		"unchanged":                   {held, true, true},
		"deleted or left the cache":   {nil, false, false},
		"recreated":                   {&metav1.ObjectMeta{UID: "p2", Finalizers: []string{f, fOther}}, false, false},
		"lost ns's finalizer":         {&metav1.ObjectMeta{UID: "p1", Finalizers: []string{fOther}}, false, true},
		"stripped of every finalizer": {&metav1.ObjectMeta{UID: "p1"}, false, false},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			v := newVerified(widgetGK)
			v.wrote("p1", "ns", at(1))
			v.wrote("p1", "other", at(1))
			v.wrote("p9", "ns", at(1))
			v.protectedEvent(held, tc.current)
			if _, ok := v.written["p1"]["ns"]; ok != tc.keepNS {
				t.Errorf("entry for ns kept = %v, want %v", ok, tc.keepNS)
			}
			if _, ok := v.written["p1"]["other"]; ok != tc.keepO {
				t.Errorf("entry for other kept = %v, want %v", ok, tc.keepO)
			}
			if _, ok := v.written["p9"]["ns"]; !ok {
				t.Error("another object's entry was dropped")
			}
		})
	}
	// It never creates one.
	v := newVerified(widgetGK)
	v.protectedEvent(held, held)
	if len(v.written) != 0 {
		t.Errorf("the handler created entries: %v", v.written)
	}
}
