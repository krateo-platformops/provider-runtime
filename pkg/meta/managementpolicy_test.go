package meta

import (
	"fmt"
	"testing"

	"github.com/krateo-platformops/plumbing/managementpolicy"
	corev1 "k8s.io/api/core/v1"
)

// The policy inputs of the reconciler's truth table, in its order
// (pkg/reconciler/management_policy_table_test.go), which are
// unstructured-runtime's (#116). "" is the annotation unset.
var (
	testManagementPolicies = []string{
		"", ManagementPolicyDefault, ManagementPolicyObserveCreateUpdate, ManagementPolicyObserveDelete,
		ManagementPolicyObserve, "observe-create", "observe,create", "create",
	}
	testDeletionPolicies = []string{"", DeletionPolicyDelete, DeletionPolicyOrphan, "orphaned"}
)

func policyObject(mp, dp string) *corev1.Pod {
	p := &corev1.Pod{}
	a := map[string]string{}
	if mp != "" {
		a[AnnotationKeyManagementPolicy] = mp
	}
	if dp != "" {
		a[AnnotationKeyDeletionPolicy] = dp
	}
	p.SetAnnotations(a)
	return p
}

// The predicates as they were before plumbing's managementpolicy (v1.20.1),
// kept as the reference the wrappers are compared with.
func oldPolicy(mp string) string {
	if mp == "" {
		return ManagementPolicyDefault
	}
	return mp
}

func oldShouldCreate(mp string) bool {
	mp = oldPolicy(mp)
	return mp == ManagementPolicyDefault || mp == ManagementPolicyObserveCreateUpdate
}

func oldShouldOnlyObserve(mp string) bool { return oldPolicy(mp) == ManagementPolicyObserve }

func oldShouldDelete(mp, dp string) bool {
	mp = oldPolicy(mp)
	if dp == "" {
		dp = DeletionPolicyDelete
	}
	return (dp == DeletionPolicyDelete && mp == ManagementPolicyDefault) || mp == ManagementPolicyObserveDelete
}

func oldIsActionAllowed(mp, action string) bool {
	mp = oldPolicy(mp)
	if action == ActionCreate || action == ActionUpdate {
		return mp == ManagementPolicyDefault || mp == ManagementPolicyObserveCreateUpdate
	}
	return mp == ManagementPolicyDefault || mp == ManagementPolicyObserveDelete
}

// TestShouldWrappersFollowManagementDecision pins each deprecated predicate
// to ManagementDecision, the one the reconciler consults, on every input of
// the truth table.
func TestShouldWrappersFollowManagementDecision(t *testing.T) {
	for _, mp := range testManagementPolicies {
		for _, dp := range testDeletionPolicies {
			o := policyObject(mp, dp)
			d := ManagementDecision(o)
			p := d.Policy
			only := !p.Allows(managementpolicy.Create) && !p.Allows(managementpolicy.Update) && !p.Allows(managementpolicy.Delete)
			for name, c := range map[string][2]bool{
				"ShouldCreate":      {ShouldCreate(o), d.May(managementpolicy.Create)},
				"ShouldUpdate":      {ShouldUpdate(o), d.May(managementpolicy.Update)},
				"ShouldDelete":      {ShouldDelete(o), d.May(managementpolicy.Delete)},
				"ShouldOnlyObserve": {ShouldOnlyObserve(o), only},
			} {
				if c[0] != c[1] {
					t.Errorf("%s(policy %q, deletion %q) = %v, ManagementDecision says %v", name, mp, dp, c[0], c[1])
				}
			}
		}
	}
}

// TestPredicatesChangeOnlyWhereDecided compares each predicate with its
// answer before managementpolicy, on every input of the truth table. They
// differ only where unstructured-runtime#110 decided they should:
//
//   - D1/D3: "observe,create" and "create" are verb sets. Before, they were
//     unknown values, and every predicate was false.
//   - D2: a value that does not parse ("observe-create") acts on nothing, so
//     it is observe-only. Before, ShouldOnlyObserve was false for it.
//   - D5: orphan always wins, and a deletion policy that is neither delete
//     nor orphan keeps the resource. Before, observe-delete deleted whatever
//     the deletion policy said.
func TestPredicatesChangeOnlyWhereDecided(t *testing.T) {
	changed := 0
	for _, mp := range testManagementPolicies {
		for _, dp := range testDeletionPolicies {
			o := policyObject(mp, dp)
			set := mp == "observe,create" || mp == "create"
			for _, c := range []struct {
				name     string
				got, old bool
				decided  bool
			}{
				{"ShouldCreate", ShouldCreate(o), oldShouldCreate(mp), set},
				{"ShouldUpdate", ShouldUpdate(o), oldShouldCreate(mp), false},
				{"ShouldOnlyObserve", ShouldOnlyObserve(o), oldShouldOnlyObserve(mp), mp == "observe-create"},
				{"ShouldDelete", ShouldDelete(o), oldShouldDelete(mp, dp), mp == ManagementPolicyObserveDelete && (dp == DeletionPolicyOrphan || dp == "orphaned")},
			} {
				t.Run(fmt.Sprintf("%s/%q/%q", c.name, mp, dp), func(t *testing.T) {
					if c.got != c.old && !c.decided {
						t.Errorf("= %v, before managementpolicy %v, and no decision changes it", c.got, c.old)
					}
					if c.got == c.old && c.decided {
						t.Errorf("= %v, as before managementpolicy, but a decision changes it", c.got)
					}
				})
				if c.got != c.old {
					changed++
				}
			}
		}
	}
	// "observe,create" and "create" × 4 deletion policies for ShouldCreate (8),
	// "observe-create" × 4 for ShouldOnlyObserve (4), observe-delete × orphan
	// and orphaned for ShouldDelete (2).
	if changed != 14 {
		t.Errorf("%d answers changed, want 14", changed)
	}
}

// TestIsActionAllowedKeepsItsAnswers: for every value that parsed before
// (unset, the four presets) and for one that never did, IsActionAllowed gives
// the answers it always gave, for the three actions callers pass
// (oasgen-provider's RestDefinition controller passes ActionCreate,
// ActionUpdate and ActionDelete). For delete it still ignores the deletion
// policy. A verb set is now understood.
func TestIsActionAllowedKeepsItsAnswers(t *testing.T) {
	for _, mp := range testManagementPolicies {
		for _, dp := range testDeletionPolicies {
			for _, action := range []string{ActionCreate, ActionUpdate, ActionDelete} {
				got := IsActionAllowed(policyObject(mp, dp), action)
				want := oldIsActionAllowed(mp, action)
				if mp == "observe,create" || mp == "create" {
					want = action == ActionCreate
				}
				if got != want {
					t.Errorf("IsActionAllowed(policy %q, deletion %q, %s) = %v, want %v", mp, dp, action, got, want)
				}
			}
		}
	}
	if !IsActionAllowed(policyObject(ManagementPolicyDefault, DeletionPolicyOrphan), ActionDelete) {
		t.Error("IsActionAllowed(default, orphan, delete) = false: it ignores the deletion policy, as it always has")
	}
}

// TestPaddedValuesMeanTheirTrimmedValue pins the third behaviour change of
// adopting managementpolicy, which the user decided to keep on 2026-11-21:
// whitespace around either annotation, or around a set's entries, is ignored.
// A padded value decides exactly as its trimmed value does. Before, a padded
// value matched no preset, so it acted on nothing and kept the external
// resource: deletion-policy "delete\n" (a Helm block scalar's trailing
// newline) kept it, and now deletes it, as "delete" does.
func TestPaddedValuesMeanTheirTrimmedValue(t *testing.T) {
	cases := []struct{ mp, dp, trimmedMP, trimmedDP string }{
		{"", "delete\n", "", DeletionPolicyDelete},
		{"", " delete", "", DeletionPolicyDelete},
		{" default", "", ManagementPolicyDefault, ""},
		{"default\n", "delete\n", ManagementPolicyDefault, DeletionPolicyDelete},
		{" observe-create-update", "", ManagementPolicyObserveCreateUpdate, ""},
		{"observe-delete", " orphan", ManagementPolicyObserveDelete, DeletionPolicyOrphan},
		{" observe , create ", "", "observe,create", ""},
		{"\tobserve\n", "", ManagementPolicyObserve, ""},
	}
	differsFromBefore := 0
	for _, tc := range cases {
		o, trimmed := policyObject(tc.mp, tc.dp), policyObject(tc.trimmedMP, tc.trimmedDP)
		for _, c := range []struct {
			name           string
			got, want, old bool
		}{
			{"ShouldCreate", ShouldCreate(o), ShouldCreate(trimmed), oldShouldCreate(tc.mp)},
			{"ShouldUpdate", ShouldUpdate(o), ShouldUpdate(trimmed), oldShouldCreate(tc.mp)},
			{"ShouldDelete", ShouldDelete(o), ShouldDelete(trimmed), oldShouldDelete(tc.mp, tc.dp)},
			{"ShouldOnlyObserve", ShouldOnlyObserve(o), ShouldOnlyObserve(trimmed), oldShouldOnlyObserve(tc.mp)},
		} {
			if c.got != c.want {
				t.Errorf("%s(policy %q, deletion %q) = %v, but its trimmed value gives %v", c.name, tc.mp, tc.dp, c.got, c.want)
			}
			if c.got != c.old {
				differsFromBefore++
			}
		}
		if p := ManagementDecision(o).Policy; p.Invalid != "" {
			t.Errorf("policy %q is invalid (%s); padding must not make a value invalid", tc.mp, p.Invalid)
		}
	}
	// The delete direction, named: these kept the resource before.
	for _, dp := range []string{"delete\n", " delete"} {
		if oldShouldDelete("", dp) || !ShouldDelete(policyObject("", dp)) {
			t.Errorf("deletion-policy %q: before %v, now %v; want kept before and deleted now", dp, oldShouldDelete("", dp), ShouldDelete(policyObject("", dp)))
		}
	}
	if differsFromBefore == 0 {
		t.Error("no padded input answers differently from before: the comparison can't fail")
	}
}
