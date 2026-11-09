package credentialuse

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const owner = "finalizer.managedresource.krateo.io"

var secretX = ID{Kind: "Secret", Namespace: "ns", Name: "x"}

func holder(record string, deleting bool, finalizers ...string) *metav1.ObjectMeta {
	m := &metav1.ObjectMeta{Name: "h", Namespace: "ns", Finalizers: finalizers}
	if record != "" {
		m.Annotations = map[string]string{AnnotationRefs: record}
	}
	if deleting {
		now := metav1.Now()
		m.DeletionTimestamp = &now
	}
	return m
}

func TestUsers(t *testing.T) {
	names := `[{"group":"","kind":"Secret","namespace":"ns","name":"x"}]`
	other := `[{"group":"","kind":"Secret","namespace":"ns","name":"y"}]`
	owners := []string{"old.example.com/owner", owner} // both names, as during provider-runtime#46
	cases := map[string]struct {
		h                  *metav1.ObjectMeta
		user, deletingUser bool
		established        bool
	}{
		"live, names it":                        {holder(names, false), true, false, false},
		"live, names it, established":           {holder(names, false, owner), true, false, true},
		"live, names another":                   {holder(other, false, owner), false, false, true},
		"live, no record":                       {holder("", false, owner), false, false, true},
		"live, unparseable":                     {holder("oops", false), true, false, false},
		"deleting with the owner finalizer":     {holder(names, true, owner), true, true, true},
		"deleting with the old owner finalizer": {holder(names, true, "old.example.com/owner"), true, true, true},
		// §6.1: excluded, whatever else keeps it around (§11.8).
		"deleting without the owner finalizer":              {holder(names, true), false, false, false},
		"deleting, only foregroundDeletion":                 {holder(names, true, "foregroundDeletion"), false, false, false},
		"deleting without the owner finalizer, unparseable": {holder("oops", true), false, false, false},
		"deleting, unparseable, with the owner finalizer":   {holder("oops", true, owner), true, true, true},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if got := IsUser(tc.h, secretX, owners); got != tc.user {
				t.Errorf("IsUser = %v, want %v", got, tc.user)
			}
			if got := IsDeletingUser(tc.h, secretX, owners); got != tc.deletingUser {
				t.Errorf("IsDeletingUser = %v, want %v", got, tc.deletingUser)
			}
			if got := IsEstablished(tc.h, owners); got != tc.established {
				t.Errorf("IsEstablished = %v, want %v", got, tc.established)
			}
		})
	}
}

func TestDecide(t *testing.T) {
	cases := map[string]struct {
		in   ReleaseInput
		want Decision
	}{
		// Row 1: not being deleted.
		"live, a user":  {ReleaseInput{Users: 1}, Keep},
		"live, no user": {ReleaseInput{}, Release},
		// Row 2: being deleted, no user at all.
		"deleting, no user, namespace live":     {ReleaseInput{Deleting: true}, Release},
		"deleting, no user, namespace deleting": {ReleaseInput{Deleting: true, NamespaceDeleting: true}, Release},
		"deleting, no user, in the grace":       {ReleaseInput{Deleting: true, GraceOver: false}, Release},
		// Row 3: its namespace is being deleted.
		"namespace deleting, a live user":           {ReleaseInput{Deleting: true, NamespaceDeleting: true, Users: 1, GraceOver: true}, Keep},
		"namespace deleting, a live user, in grace": {ReleaseInput{Deleting: true, NamespaceDeleting: true, Users: 1}, Keep},
		// Row 4: its namespace is live.
		"namespace live, in the grace":                     {ReleaseInput{Deleting: true, Users: 2, DeletingUsers: 1}, WaitForGrace},
		"namespace live, after the grace, a deleting user": {ReleaseInput{Deleting: true, Users: 2, DeletingUsers: 1, GraceOver: true}, Keep},
		"namespace live, after the grace, only live users": {ReleaseInput{Deleting: true, Users: 2, GraceOver: true}, Release},
		// A cluster-scoped P always takes row 4.
		"cluster-scoped, in the grace":                   {ReleaseInput{Deleting: true, ClusterScoped: true, NamespaceDeleting: true, Users: 1}, WaitForGrace},
		"cluster-scoped, after the grace, live users":    {ReleaseInput{Deleting: true, ClusterScoped: true, NamespaceDeleting: true, Users: 1, GraceOver: true}, Release},
		"cluster-scoped, after the grace, deleting user": {ReleaseInput{Deleting: true, ClusterScoped: true, Users: 1, DeletingUsers: 1, GraceOver: true}, Keep},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if got := Decide(tc.in); got != tc.want {
				t.Errorf("Decide(%+v) = %v, want %v", tc.in, got, tc.want)
			}
		})
	}
}

func TestGraceEnd(t *testing.T) {
	grace := 60 * time.Second
	dt := time.Date(2026, 11, 8, 16, 27, 34, 0, time.UTC)
	cases := map[string]struct {
		firstSeen time.Time
		want      time.Time
	}{
		// A late delivery: counted from first sight.
		"seen late": {dt.Add(20 * time.Second), dt.Add(80 * time.Second)},
		// A server clock ahead of the local one: counted from the timestamp.
		"seen before the timestamp": {dt.Add(-30 * time.Second), dt.Add(60 * time.Second)},
		"seen at once":              {dt, dt.Add(60 * time.Second)},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if got := GraceEnd(dt, tc.firstSeen, grace); !got.Equal(tc.want) {
				t.Errorf("GraceEnd = %v, want %v", got, tc.want)
			}
		})
	}
	// A local reading with a monotonic clock compares on the wall clock.
	now := time.Now()
	if got := GraceEnd(now.Add(-time.Hour).Round(0), now, grace); !got.Equal(now.Add(grace)) {
		t.Errorf("GraceEnd with a monotonic first sight = %v, want %v", got, now.Add(grace))
	}
}

func TestDecisionString(t *testing.T) {
	for _, d := range []Decision{Keep, Release, WaitForGrace} {
		if d.String() == "Decision(?)" {
			t.Errorf("Decision %d has no name", d)
		}
	}
}
