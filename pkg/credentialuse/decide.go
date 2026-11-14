package credentialuse

import (
	"slices"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// IsEstablished reports whether holder carries any of the owner finalizers:
// the runtime's own finalizer, which it writes only after the first Connect
// and Observe succeed (§2, §7.1). An established holder fails open on an
// Acquire error; a new one fails closed (§8).
func IsEstablished(holder metav1.Object, ownerFinalizers []string) bool {
	return slices.ContainsFunc(holder.GetFinalizers(), func(f string) bool {
		return slices.Contains(ownerFinalizers, f)
	})
}

// IsUser reports whether holder, which exists, uses protected object p
// (§6.1): its record names p, or can't be parsed, and it is not being
// deleted without an owner finalizer. A holder that is being deleted and
// lacks every owner finalizer is excluded: the runtime removes the record in
// the same write as its owner finalizer, and the exclusion covers every other
// way a stale record is left behind, and breaks foreground-deletion cycles
// (§11.8).
func IsUser(holder metav1.Object, p ID, ownerFinalizers []string) bool {
	if holder.GetDeletionTimestamp() != nil && !IsEstablished(holder, ownerFinalizers) {
		return false
	}
	return ReadRecord(holder).Names(p)
}

// IsDeletingUser reports whether holder is a user of p that is being deleted:
// it has a deletionTimestamp and still carries an owner finalizer (§6.1).
func IsDeletingUser(holder metav1.Object, p ID, ownerFinalizers []string) bool {
	return holder.GetDeletionTimestamp() != nil && IsUser(holder, p, ownerFinalizers)
}

// A Decision is what §5's table says to do with one finalizer of K for N on a
// protected object.
type Decision int

const (
	// Keep the finalizer.
	Keep Decision = iota
	// Release it: R3.
	Release
	// WaitForGrace: P is being deleted in a live namespace, or is
	// cluster-scoped, and the grace hasn't run out. Requeue the batch at its
	// end.
	WaitForGrace
)

func (d Decision) String() string {
	switch d {
	case Keep:
		return "Keep"
	case Release:
		return "Release"
	case WaitForGrace:
		return "WaitForGrace"
	}
	return "Decision(?)"
}

// ReleaseInput is what §5's table reads for one protected object P and one
// (K, N). Every field comes from the last reads before the release patch: R1's
// live GET of P, R2's live GET of P's Namespace and R2's consistent list of K
// in N (§5, §6.2).
type ReleaseInput struct {
	// Deleting: P has a deletionTimestamp.
	Deleting bool
	// ClusterScoped: P has no namespace, so it always takes the last row.
	ClusterScoped bool
	// NamespaceDeleting: P's Namespace has a deletionTimestamp, read live.
	NamespaceDeleting bool
	// GraceOver: the grace has run out (GraceEnd).
	GraceOver bool
	// Users and DeletingUsers count the holders of K in N that use P, and
	// those of them being deleted (§6.1).
	Users, DeletingUsers int
}

// Decide applies §5's table (U5):
//
//	P not being deleted:                       kept while any user exists
//	P being deleted, no user at all:           released at once
//	P being deleted, its namespace deleting:   kept while any user exists
//	P being deleted, its namespace live:       the grace runs; after it, kept
//	                                           while any user is being deleted
//
// A cluster-scoped P has no namespace and takes the last row.
func Decide(in ReleaseInput) Decision {
	switch {
	case in.Users == 0:
		return Release
	case !in.Deleting:
		return Keep
	case in.NamespaceDeleting && !in.ClusterScoped:
		return Keep
	case !in.GraceOver:
		return WaitForGrace
	case in.DeletingUsers > 0:
		return Keep
	}
	return Release
}

// GraceEnd returns when the grace for a protected object being deleted runs
// out: grace after the later of its deletionTimestamp and the time this
// process first saw that timestamp (§5, L4). So neither a server clock behind
// the local one nor a late delivery can shorten it.
//
// firstSeen is a local time.Now() reading. The server's timestamp is compared
// with it once, on the wall clock, and the result is an offset from firstSeen,
// so it keeps firstSeen's monotonic reading: a step of the local wall clock
// afterwards can neither shorten nor stretch the grace.
func GraceEnd(deletionTimestamp, firstSeen time.Time, grace time.Duration) time.Time {
	ahead := deletionTimestamp.Sub(firstSeen.Round(0)) // wall clock only
	if ahead < 0 {
		ahead = 0
	}
	return firstSeen.Add(ahead + grace)
}
