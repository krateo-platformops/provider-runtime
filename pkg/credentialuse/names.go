package credentialuse

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation"
)

// Domain is the prefix of every label and annotation key this package owns.
const Domain = "credential-use.krateo.io"

// Keys on holders.
const (
	// AnnotationRefs is the record: the protected objects a holder reads
	// (§4).
	AnnotationRefs = Domain + "/refs.v1"
	// AnnotationUnheld lists the refs a holder reads that aren't held, with
	// why (§11.3).
	AnnotationUnheld = Domain + "/unheld"
)

// Keys on protected objects and their Namespaces.
const (
	// LabelHeld scopes the protected-object caches: an object carries it
	// while any kind holds it (§10.1).
	LabelHeld = Domain + "/held"
	// AnnotationAllowHoldersFrom is a target's consent to be held by holders
	// in other namespaces (§11.3).
	AnnotationAllowHoldersFrom = Domain + "/allow-holders-from"
)

// Keys on a holder kind's CustomResourceDefinition.
const (
	// AnnotationDisown is the Disown switch (§7.4).
	AnnotationDisown = Domain + "/disown"
	// AnnotationHeldCount is Disown's live count (§7.4).
	AnnotationHeldCount = Domain + "/held-count"
	// AnnotationDisownSettled is set once Disown has finished (§7.4).
	AnnotationDisownSettled = Domain + "/disown-settled"
	// AnnotationConfig holds the settings every process of a kind shares
	// (§7.3).
	AnnotationConfig = Domain + "/config"
	// AnnotationIndex locates a kind's index in PerName and PerNamePoll
	// modes (§10.6.1).
	AnnotationIndex = Domain + "/index"
)

// ClusterScopedName is the name part of a cluster-scoped holder kind's
// finalizer (§3). A GroupKind is either namespaced or cluster-scoped, so it
// can't collide with a namespace of that name.
const ClusterScopedName = "cluster-scoped"

// finalizerFamily starts every credential-use finalizer, of any holder kind.
const finalizerFamily = "credential-use."

// markerPrefix starts every holder kind's marker label key.
const markerPrefix = Domain + "/k-"

// FinalizerPrefix returns the prefix every finalizer of holder kind K
// carries, credential-use.<lowercase kind>.<group>/ (§3).
func FinalizerPrefix(holder schema.GroupKind) (string, error) {
	if holder.Kind == "" || holder.Group == "" {
		return "", fmt.Errorf("credential-use: a holder kind needs a kind and a group, got %q", holder.String())
	}
	prefix := finalizerFamily + strings.ToLower(holder.Kind) + "." + holder.Group
	if errs := validation.IsDNS1123Subdomain(prefix); len(errs) > 0 {
		return "", fmt.Errorf("credential-use: holder kind %s gives the invalid finalizer prefix %q: %s", holder, prefix, strings.Join(errs, "; "))
	}
	return prefix + "/", nil
}

// FinalizerName returns the finalizer of holder kind K in holder namespace N,
// credential-use.<lowercase kind>.<group>/<namespace> (§3, U6). An empty
// namespace means a cluster-scoped holder kind, whose name part is
// ClusterScopedName. The same string is the key of the epoch annotation.
//
// Only a pathological group makes the name too long; setup refuses to start
// then (§7.3).
func FinalizerName(holder schema.GroupKind, namespace string) (string, error) {
	prefix, err := FinalizerPrefix(holder)
	if err != nil {
		return "", err
	}
	part := namespace
	if part == "" {
		part = ClusterScopedName
	} else if errs := validation.IsDNS1123Label(namespace); len(errs) > 0 {
		return "", fmt.Errorf("credential-use: %q is not a namespace name: %s", namespace, strings.Join(errs, "; "))
	}
	name := prefix + part
	if errs := validation.IsQualifiedName(name); len(errs) > 0 {
		return "", fmt.Errorf("credential-use: holder kind %s gives the invalid finalizer %q: %s", holder, name, strings.Join(errs, "; "))
	}
	return name, nil
}

// IsCredentialUseFinalizer reports whether f is a credential-use finalizer of
// any holder kind. The held label goes with the last of them (§6.2 R3).
func IsCredentialUseFinalizer(f string) bool {
	prefix, _, ok := strings.Cut(f, "/")
	return ok && strings.HasPrefix(prefix, finalizerFamily)
}

// MarkerLabel returns holder kind K's marker label key,
// credential-use.krateo.io/k-<hash>, where <hash> is the first 16 hex digits
// of the SHA-256 of <lowercase kind>.<group> (§10.1, L3). It lets one kind's
// holds be selected. A collision is harmless: every reader also checks the
// finalizer prefix.
func MarkerLabel(holder schema.GroupKind) string {
	sum := sha256.Sum256([]byte(strings.ToLower(holder.Kind) + "." + holder.Group))
	return markerPrefix + hex.EncodeToString(sum[:])[:16]
}

// IsMarkerLabel reports whether key is some holder kind's marker label.
func IsMarkerLabel(key string) bool {
	return strings.HasPrefix(key, markerPrefix)
}

// NewEpoch returns a fresh random 128-bit token, hex-encoded. Every acquiring
// write sets one (§3, §6.2 A2), and R3 tests the one it read.
func NewEpoch() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("credential-use: reading a random epoch: %w", err)
	}
	return hex.EncodeToString(b[:]), nil
}
