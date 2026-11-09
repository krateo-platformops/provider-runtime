package credentialuse

import (
	"regexp"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation"
)

var repoGK = schema.GroupKind{Group: "git.krateo.io", Kind: "Repo"}

// groupOfLen returns a valid DNS subdomain of n characters, in labels of at
// most 63.
func groupOfLen(n int) string {
	var labels []string
	for n > 0 {
		l := min(n, 63)
		if n-l == 1 {
			l-- // don't leave a dot with nothing after it
		}
		labels = append(labels, strings.Repeat("g", l))
		n -= l
		if n > 0 {
			n-- // the dot
		}
	}
	return strings.Join(labels, ".")
}

func TestFinalizerName(t *testing.T) {
	cases := map[string]struct {
		holder    schema.GroupKind
		namespace string
		want      string
		wantErr   bool
	}{
		"the design's example":       {repoGK, "rlgov-probe", "credential-use.repo.git.krateo.io/rlgov-probe", false},
		"a cluster-scoped kind":      {repoGK, "", "credential-use.repo.git.krateo.io/cluster-scoped", false},
		"a kind is lowercased":       {schema.GroupKind{Group: "core.krateo.io", Kind: "CompositionDefinition"}, "a", "credential-use.compositiondefinition.core.krateo.io/a", false},
		"no group":                   {schema.GroupKind{Kind: "Pod"}, "a", "", true},
		"no kind":                    {schema.GroupKind{Group: "g.io"}, "a", "", true},
		"a namespace that isn't one": {repoGK, "Not_A_Namespace", "", true},
		"a namespace of 64":          {repoGK, strings.Repeat("a", 64), "", true},
		"a namespace of 63":          {repoGK, strings.Repeat("a", 63), "credential-use.repo.git.krateo.io/" + strings.Repeat("a", 63), false},
		// 15 + len(kind) + 1 + len(group) <= 253 is the only limit a real
		// kind can hit (§3).
		"a prefix of 254": {schema.GroupKind{Group: groupOfLen(254 - len("credential-use.k.")), Kind: "K"}, "a", "", true},
		"a prefix of 253": {schema.GroupKind{Group: groupOfLen(253 - len("credential-use.k.")), Kind: "K"}, "a", "credential-use.k." + groupOfLen(253-len("credential-use.k.")) + "/a", false},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got, err := FinalizerName(tc.holder, tc.namespace)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("FinalizerName = %q, want an error", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("FinalizerName: %v", err)
			}
			if got != tc.want {
				t.Errorf("FinalizerName = %q, want %q", got, tc.want)
			}
			// The API server validates finalizer names and annotation keys
			// the same way (§2), and warns on a finalizer without a /.
			if errs := validation.IsQualifiedName(got); len(errs) > 0 {
				t.Errorf("%q is not a qualified name: %v", got, errs)
			}
			if !strings.Contains(got, "/") {
				t.Errorf("%q has no /, so the API server warns on it", got)
			}
			if !IsCredentialUseFinalizer(got) {
				t.Errorf("IsCredentialUseFinalizer(%q) = false", got)
			}
			prefix, err := FinalizerPrefix(tc.holder)
			if err != nil || !strings.HasPrefix(got, prefix) {
				t.Errorf("FinalizerPrefix = %q, %v; not a prefix of %q", prefix, err, got)
			}
		})
	}
}

// FinalizerPrefix is used on its own, by the release and the janitor, so it
// checks the prefix itself.
func TestFinalizerPrefix(t *testing.T) {
	got, err := FinalizerPrefix(repoGK)
	if err != nil || got != "credential-use.repo.git.krateo.io/" {
		t.Errorf("FinalizerPrefix(Repo) = %q, %v", got, err)
	}
	cases := map[string]struct {
		holder schema.GroupKind
		want   string
	}{
		"no group":        {schema.GroupKind{Kind: "Pod"}, "needs a kind and a group"},
		"no kind":         {schema.GroupKind{Group: "g.io"}, "needs a kind and a group"},
		"a prefix of 254": {schema.GroupKind{Group: groupOfLen(254 - len("credential-use.k.")), Kind: "K"}, "invalid finalizer prefix"},
		"an invalid kind": {schema.GroupKind{Group: "g.io", Kind: "Not_A_Kind"}, "invalid finalizer prefix"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got, err := FinalizerPrefix(tc.holder)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("FinalizerPrefix = %q, %v; want an error containing %q", got, err, tc.want)
			}
		})
	}
}

func TestIsCredentialUseFinalizer(t *testing.T) {
	cases := map[string]bool{
		"credential-use.repo.git.krateo.io/ns":         true,
		"credential-use.thing.other.io/cluster-scoped": true,
		"finalizer.managedresource.krateo.io":          false,
		"credential-use.krateo.io":                     false, // no /
		"example.com/credential-use.repo":              false,
		"kubernetes":                                   false,
		"foregroundDeletion":                           false,
	}
	for f, want := range cases {
		if got := IsCredentialUseFinalizer(f); got != want {
			t.Errorf("IsCredentialUseFinalizer(%q) = %v, want %v", f, got, want)
		}
	}
}

func TestMarkerLabel(t *testing.T) {
	got := MarkerLabel(repoGK)
	if !regexp.MustCompile(`^credential-use\.krateo\.io/k-[0-9a-f]{16}$`).MatchString(got) {
		t.Fatalf("MarkerLabel = %q, want credential-use.krateo.io/k-<16 hex digits>", got)
	}
	if errs := validation.IsQualifiedName(got); len(errs) > 0 {
		t.Errorf("%q is not a valid label key: %v", got, errs)
	}
	if !IsMarkerLabel(got) || IsMarkerLabel(LabelHeld) {
		t.Error("IsMarkerLabel doesn't tell a marker from the held label")
	}
	// The hash is of <lowercase kind>.<group>, so a kind's case doesn't
	// matter, and another kind gets another marker.
	if MarkerLabel(schema.GroupKind{Group: "git.krateo.io", Kind: "REPO"}) != got {
		t.Error("MarkerLabel depends on the kind's case")
	}
	if MarkerLabel(schema.GroupKind{Group: "git.krateo.io", Kind: "LocalResource"}) == got {
		t.Error("two kinds share a marker")
	}
	// sha256("repo.git.krateo.io") starts with this (checked with shasum -a 256);
	// pinned so the key never changes under existing holds.
	if want := "credential-use.krateo.io/k-" + "65e903d2efc70d5f"; got != want {
		t.Errorf("MarkerLabel(Repo.git.krateo.io) = %q, want %q", got, want)
	}
}

func TestNewEpoch(t *testing.T) {
	seen := map[string]bool{}
	for range 100 {
		e, err := NewEpoch()
		if err != nil {
			t.Fatal(err)
		}
		if !regexp.MustCompile(`^[0-9a-f]{32}$`).MatchString(e) {
			t.Fatalf("NewEpoch = %q, want 32 hex digits (128 bits)", e)
		}
		if seen[e] {
			t.Fatalf("NewEpoch repeated %q", e)
		}
		seen[e] = true
	}
}

func TestGroupOfLen(t *testing.T) {
	for _, n := range []int{1, 63, 64, 65, 127, 236, 237} {
		g := groupOfLen(n)
		if len(g) != n {
			t.Errorf("groupOfLen(%d) has length %d", n, len(g))
		}
		if errs := validation.IsDNS1123Subdomain(g); len(errs) > 0 {
			t.Errorf("groupOfLen(%d) = %q is not a DNS subdomain: %v", n, g, errs)
		}
	}
}
