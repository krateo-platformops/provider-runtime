package reconciler

// TestManagementPolicyTruthTable measures how the reconciler behaves today for
// every combination of krateo.io/management-policy, krateo.io/deletion-policy
// and the state of the external resource, and pins the result in
// testdata/management-policy-table.tsv. It documents behaviour; it does not
// judge it. The same table is run on unstructured-runtime and the two are
// diffed (unstructured-runtime#110), so the columns and their order are fixed.
//
// Harness. One Reconcile call of the real Reconciler per row, against the tracing
// tests' fake API server (newTraceServer): controller-runtime's fake client
// with the Widget status subresource. The reconciler keeps its default
// finalizer (resource.APIFinalizer) and critical annotation updater, so every
// finalizer and annotation write goes through that client. The fake client
// gives the deletion semantics the table needs: an object can only be stored
// with a deletionTimestamp while it has a finalizer, the deletionTimestamp is
// immutable, and an update that removes the last finalizer of an object being
// deleted deletes it, as the API server does. It also enforces resourceVersion
// preconditions. envtest would add a real API server and nothing this table
// reads, so it isn't used.
//
// Every object starts with the runtime's finalizer, as after its first pass,
// and with no conditions. Rows in a deleting state also carry a
// deletionTimestamp. Every row is exactly one Reconcile call, with no
// follow-up pass: a deletion that takes two passes (Delete, then the finalizer
// once the resource is gone) shows its first pass in deleting-present and its
// second in deleting-absent. The scripted ExternalClient records the calls it gets.
// Its Observe sets Ready as the provider contract requires (Available when the
// resource exists, Unavailable when it does not) and returns ResourceExists
// and ResourceUpToDate for the row's state; Create, Update and Delete succeed
// and change nothing.
//
// The layout is the one core-provider's unstructured-runtime table uses, so
// the two files diff line by line. Columns, tab-separated, in order:
//
//	management-policy  unset (no annotation), default, observe-create-update,
//	                   observe-delete, observe, observe-create (a value that
//	                   does not parse: D2), observe,create and create (verb
//	                   sets: D1, with observe implied: D3)
//	deletion-policy    unset (no annotation), delete, orphan, orphaned (a
//	                   value that is neither, which keeps the resource)
//	state              absent, up-to-date, drifted, deleting-present,
//	                   deleting-absent
//	observe, create,   how many times each ExternalClient method was called
//	update, delete
//	finalizer-removed  true or false. true also when the object was deleted,
//	                   and then every condition cell reads (gone)
//	ready-*, synced-*  status, reason and message of the stored condition
//	                   after the pass
//	events             the reasons of the events recorded, sorted and
//	                   comma-joined
//	error              the text of the error Reconcile returned
//	requeue            rate-limited when Reconcile returned an error (the
//	                   controller requeues it with backoff) or asked for
//	                   Requeue; after=<d> for RequeueAfter; otherwise none
//
// An empty cell is -. Rows are management-policy outer, then deletion-policy,
// then state inner: 8 × 4 × 5 = 160 rows and the header. The two lists, and
// their order, are unstructured-runtime's (#116). The API server writes of each row
// go to t.Log only.
//
// Regenerate the golden file with MGMT_TABLE_WRITE=1.

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/krateo-platformops/plumbing/managementpolicy"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	prv1 "github.com/krateo-platformops/provider-runtime/apis/common/v1"
	"github.com/krateo-platformops/provider-runtime/pkg/meta"
	"github.com/krateo-platformops/provider-runtime/pkg/resource"
	"github.com/krateo-platformops/provider-runtime/pkg/resource/fake"
)

const mgmtTableGolden = "testdata/management-policy-table.tsv"

var mgmtTableColumns = []string{
	"management-policy", "deletion-policy", "state",
	"observe", "create", "update", "delete",
	"finalizer-removed",
	"ready-status", "ready-reason", "ready-message",
	"synced-status", "synced-reason", "synced-message",
	"events", "error", "requeue",
}

// mgmtTableState is the state of the external resource, and whether the
// managed object is being deleted.
type mgmtTableState struct {
	name     string
	exists   bool
	upToDate bool
	deleting bool
}

var mgmtTableStates = []mgmtTableState{
	{name: "absent"},
	{name: "up-to-date", exists: true, upToDate: true},
	{name: "drifted", exists: true},
	{name: "deleting-present", exists: true, upToDate: true, deleting: true},
	{name: "deleting-absent", deleting: true},
}

// The policy values; "" is the annotation unset. observe-create does not
// parse (D2); observe,create and create are verb sets (D1, D3); orphaned is a
// deletion policy that is neither delete nor orphan (Diego, 2026-11-19).
var (
	mgmtTableManagementPolicies = []string{
		"",
		meta.ManagementPolicyDefault,
		meta.ManagementPolicyObserveCreateUpdate,
		meta.ManagementPolicyObserveDelete,
		meta.ManagementPolicyObserve,
		"observe-create",
		"observe,create",
		"create",
	}
	mgmtTableDeletionPolicies = []string{
		"",
		meta.DeletionPolicyDelete,
		meta.DeletionPolicyOrphan,
		"orphaned",
	}
)

// mgmtTableCalls counts the calls to each ExternalClient method.
type mgmtTableCalls struct{ observe, create, update, delete int }

// mgmtTableClient is the scripted ExternalClient. It counts each call.
func mgmtTableClient(st mgmtTableState, calls *mgmtTableCalls) ExternalClient {
	return &ExternalClientFns{
		ObserveFn: func(_ context.Context, mg resource.Managed) (ExternalObservation, error) {
			calls.observe++
			if st.exists {
				mg.SetConditions(prv1.Available())
			} else {
				mg.SetConditions(prv1.Unavailable())
			}
			return ExternalObservation{ResourceExists: st.exists, ResourceUpToDate: st.upToDate}, nil
		},
		CreateFn: func(context.Context, resource.Managed) error {
			calls.create++
			return nil
		},
		UpdateFn: func(context.Context, resource.Managed) error {
			calls.update++
			return nil
		},
		DeleteFn: func(context.Context, resource.Managed) error {
			calls.delete++
			return nil
		},
	}
}

func TestManagementPolicyTruthTable(t *testing.T) {
	rows := [][]string{mgmtTableColumns}
	var writes strings.Builder
	for _, mp := range mgmtTableManagementPolicies {
		for _, dp := range mgmtTableDeletionPolicies {
			for _, st := range mgmtTableStates {
				row, w := mgmtTableRow(t, mp, dp, st)
				rows = append(rows, row)
				fmt.Fprintf(&writes, "%s\t%s\t%s\t%s\n", row[0], row[1], row[2], w)
			}
		}
	}
	if want := 1 + len(mgmtTableManagementPolicies)*len(mgmtTableDeletionPolicies)*len(mgmtTableStates); len(rows) != want || want != 161 {
		t.Fatalf("the table has %d lines, header included; want 161 (8 × 4 × 5 cases and the header)", len(rows))
	}
	for i, r := range rows {
		if len(r) != len(mgmtTableColumns) {
			t.Fatalf("line %d has %d cells, want %d: %q", i, len(r), len(mgmtTableColumns), r)
		}
	}

	checkMgmtTableContracts(t, rows[1:])

	var b strings.Builder
	for _, r := range rows {
		b.WriteString(strings.Join(r, "\t"))
		b.WriteByte('\n')
	}
	got := b.String()
	t.Logf("management-policy truth table, %d cases:\n%s", len(rows)-1, got)
	t.Logf("API server writes per row (not in the golden file):\n%s", writes.String())

	if os.Getenv("MGMT_TABLE_WRITE") == "1" {
		if err := os.MkdirAll(filepath.Dir(mgmtTableGolden), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(mgmtTableGolden, []byte(got), 0o644); err != nil {
			t.Fatal(err)
		}
		t.Logf("wrote %s", mgmtTableGolden)
		return
	}
	want, err := os.ReadFile(mgmtTableGolden)
	if err != nil {
		t.Fatalf("read %s (regenerate with MGMT_TABLE_WRITE=1): %v", mgmtTableGolden, err)
	}
	if diff := cmp.Diff(strings.Split(string(want), "\n"), strings.Split(got, "\n")); diff != "" {
		t.Errorf("the truth table differs from %s (-want, +got):\n%s", mgmtTableGolden, diff)
	}
}

// mgmtTableRow runs one Reconcile call for the row. It returns the row's
// cells, and the writes the API server got, for the log.
func mgmtTableRow(t *testing.T, mp, dp string, st mgmtTableState) ([]string, string) {
	t.Helper()
	ann := map[string]string{}
	if mp != "" {
		ann[meta.AnnotationKeyManagementPolicy] = mp
	}
	if dp != "" {
		ann[meta.AnnotationKeyDeletionPolicy] = dp
	}
	w := newWidget("w", ann)
	w.SetFinalizers([]string{FinalizerName})
	if st.deleting {
		ts := metav1.NewTime(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
		w.SetDeletionTimestamp(&ts)
	}
	s := newTraceServer(t, w)

	var calls mgmtTableCalls
	rec := &eventCapture{}
	r := NewReconciler(&fake.Manager{Client: s.c, Scheme: s.scheme}, resource.ManagedKind(widgetGVK),
		WithExternalConnecter(ExternalConnectorFn(func(context.Context, resource.Managed) (ExternalClient, error) {
			return mgmtTableClient(st, &calls), nil
		})),
		WithRecorder(rec),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	res, err := r.Reconcile(ctx, reqFor("w"))

	cells := []string{
		mgmtOr(mp, "unset"), mgmtOr(dp, "unset"), st.name,
		fmt.Sprint(calls.observe), fmt.Sprint(calls.create), fmt.Sprint(calls.update), fmt.Sprint(calls.delete),
	}

	stored := &Widget{}
	gerr := s.raw.Get(context.Background(), reqFor("w").NamespacedName, stored)
	switch {
	case kerrors.IsNotFound(gerr):
		cells = append(cells, "true", "(gone)", "(gone)", "(gone)", "(gone)", "(gone)", "(gone)")
	case gerr != nil:
		t.Fatalf("get the stored widget: %v", gerr)
	default:
		cells = append(cells, fmt.Sprint(!meta.FinalizerExists(stored, FinalizerName)))
		cells = append(cells, mgmtConditionCells(stored.GetCondition(prv1.TypeReady))...)
		cells = append(cells, mgmtConditionCells(stored.GetCondition(prv1.TypeSynced))...)
	}

	reasons := make([]string, 0, len(rec.events))
	for _, e := range rec.events {
		reasons = append(reasons, string(e.Reason))
	}
	sort.Strings(reasons)
	cells = append(cells, mgmtJoin(reasons, ","))

	errCell := "-"
	if err != nil {
		errCell = err.Error()
	}
	cells = append(cells, mgmtOr(errCell, "-"), mgmtRequeueCell(res, err))

	for i, c := range cells {
		cells[i] = mgmtOr(mgmtTSVSafe(c), "-")
	}
	return cells, mgmtJoin(s.ops.list(), ",")
}

func mgmtConditionCells(c prv1.Condition) []string {
	if c.Type == "" {
		return []string{"-", "-", "-"}
	}
	return []string{string(c.Status), string(c.Reason), mgmtOr(c.Message, "-")}
}

func mgmtRequeueCell(res reconcile.Result, err error) string {
	switch {
	case err != nil, isRateLimitedRequeue(res):
		return "rate-limited"
	case res.RequeueAfter > 0:
		return "after=" + res.RequeueAfter.String()
	case res == reconcile.Result{}:
		return "none"
	default:
		return "other"
	}
}

func mgmtOr(s, empty string) string {
	if s == "" {
		return empty
	}
	return s
}

func mgmtJoin(ss []string, sep string) string {
	if len(ss) == 0 {
		return "-"
	}
	return strings.Join(ss, sep)
}

func mgmtTSVSafe(s string) string {
	return strings.NewReplacer("\t", " ", "\r", " ", "\n", " ").Replace(s)
}

// checkMgmtTableContracts asserts what unstructured-runtime#110 decided, on
// the rows themselves, so a golden file regenerated without reading its diff
// can't change it unseen:
//
//   - delete-on-absent (Diego, 2026-11-21): when Observe reports the external
//     resource absent during a deletion, this runtime removes the finalizer
//     without calling Delete. unstructured-runtime calls Delete there; each
//     runtime keeps and documents its own contract.
//   - D5: a deletion policy that keeps the resource (orphan, or a value that
//     is neither delete nor orphan) is never overridden by the management
//     policy: no Delete call, whatever the policy.
//   - D2: a value that does not parse acts on nothing: no Create, Update or
//     Delete call.
//   - D3: "create" is "observe,create": the two rows agree on every cell.
//   - D2 reporting: outside a deletion, an invalid management policy reads
//     Synced=ManagementPolicyInvalid, and else an invalid deletion policy
//     DeletionPolicyInvalid, whatever the pass concluded.
//   - D4: an absent resource with create withheld reads
//     Ready=CreateWithheldByPolicy, and a drifted one with update withheld
//     (the harness's Observe reports it healthy) True/UpdateWithheldByPolicy.
func checkMgmtTableContracts(t *testing.T, rows [][]string) {
	t.Helper()
	col := map[string]int{}
	for i, c := range mgmtTableColumns {
		col[c] = i
	}
	byKey := map[[3]string][]string{}
	for _, r := range rows {
		byKey[[3]string{r[0], r[1], r[2]}] = r
		key := fmt.Sprintf("%s/%s/%s", r[0], r[1], r[2])
		if r[2] == "deleting-absent" && (r[col["delete"]] != "0" || r[col["finalizer-removed"]] != "true") {
			t.Errorf("%s: delete=%s finalizer-removed=%s; this runtime removes the finalizer without a Delete when the resource is absent", key, r[col["delete"]], r[col["finalizer-removed"]])
		}
		if (r[1] == meta.DeletionPolicyOrphan || r[1] == "orphaned") && r[col["delete"]] != "0" {
			t.Errorf("%s: delete=%s; a deletion policy that keeps the resource always wins (D5)", key, r[col["delete"]])
		}
		if r[0] == "observe-create" && (r[col["create"]] != "0" || r[col["update"]] != "0" || r[col["delete"]] != "0") {
			t.Errorf("%s: create=%s update=%s delete=%s; a policy that does not parse acts on nothing (D2)", key, r[col["create"]], r[col["update"]], r[col["delete"]])
		}
		if r[2] == "deleting-present" || r[2] == "deleting-absent" {
			continue
		}
		d := meta.ManagementDecision(mgmtTableObjectAnnotations(r[0], r[1]))
		switch {
		case d.Policy.Invalid != "":
			if r[col["synced-reason"]] != string(prv1.ReasonManagementPolicyInvalid) {
				t.Errorf("%s: Synced reason %s; an invalid management policy wins over the pass's outcome", key, r[col["synced-reason"]])
			}
		case d.DeletionInvalid != "":
			if r[col["synced-reason"]] != string(prv1.ReasonDeletionPolicyInvalid) {
				t.Errorf("%s: Synced reason %s; an invalid deletion policy is reported on every pass", key, r[col["synced-reason"]])
			}
		}
		if r[2] == "absent" && !d.May(managementpolicy.Create) && r[col["ready-reason"]] != string(prv1.ReasonCreateWithheldByPolicy) {
			t.Errorf("%s: Ready reason %s; an absent resource with create withheld is CreateWithheldByPolicy (D4)", key, r[col["ready-reason"]])
		}
		if r[2] == "drifted" && !d.May(managementpolicy.Update) && (r[col["ready-status"]] != "True" || r[col["ready-reason"]] != string(prv1.ReasonUpdateWithheldByPolicy)) {
			t.Errorf("%s: Ready %s/%s; a healthy drifted resource with update withheld is True/UpdateWithheldByPolicy (D4)", key, r[col["ready-status"]], r[col["ready-reason"]])
		}
	}
	n := 0
	for k, r := range byKey {
		if k[0] != "create" {
			continue
		}
		n++
		o := byKey[[3]string{"observe,create", k[1], k[2]}]
		if !cmp.Equal(r[3:], o[3:]) {
			t.Errorf("create/%s/%s differs from observe,create (D3: observe is implied):\n%q\n%q", k[1], k[2], r[3:], o[3:])
		}
	}
	if n != len(mgmtTableDeletionPolicies)*len(mgmtTableStates) {
		t.Errorf("compared %d create rows, want %d", n, len(mgmtTableDeletionPolicies)*len(mgmtTableStates))
	}
}

// TestEveryVerbSetCallsOnlyWhatItAllows runs every subset of {create, update,
// delete}, written as a set (observe is implied), against every deletion
// policy and state, with the truth table's harness. The table pins only the
// inputs it shares with unstructured-runtime; this test covers the sets it
// doesn't, such as "observe,update", which no preset could express. A verb
// must be called exactly when the policy allows it and the state calls for
// it: create when absent, update when drifted, delete when deleting a present
// resource whose deletion policy keeps nothing. In particular an absent
// resource is never updated.
func TestEveryVerbSetCallsOnlyWhatItAllows(t *testing.T) {
	optional := []string{"create", "update", "delete"}
	for mask := 0; mask < 1<<len(optional); mask++ {
		set := []string{"observe"}
		for i, v := range optional {
			if mask&(1<<i) != 0 {
				set = append(set, v)
			}
		}
		mp := strings.Join(set, ",")
		allows := func(v string) bool {
			for _, s := range set {
				if s == v {
					return true
				}
			}
			return false
		}
		for _, dp := range mgmtTableDeletionPolicies {
			keeps := dp == meta.DeletionPolicyOrphan || dp == "orphaned"
			for _, st := range mgmtTableStates {
				row, _ := mgmtTableRow(t, mp, dp, st)
				want := map[string]string{"create": "0", "update": "0", "delete": "0"}
				switch {
				case st.deleting:
					if st.exists && allows("delete") && !keeps {
						want["delete"] = "1"
					}
				case !st.exists:
					if allows("create") {
						want["create"] = "1"
					}
				case !st.upToDate:
					if allows("update") {
						want["update"] = "1"
					}
				}
				got := map[string]string{"create": row[4], "update": row[5], "delete": row[6]}
				if !cmp.Equal(want, got) {
					t.Errorf("%q / deletion %q / %s: calls %v, want %v", mp, dp, st.name, got, want)
				}
			}
		}
	}
}

// TestTheDeleteGateRereadsThePolicyAfterConnect pins the second delete gate,
// the one just before Delete. The first gate, before Connect, removes the
// finalizer of an object whose policy withholds delete, so the second decides
// only when the policy changed in between: a connecter whose Connect writes
// the object's annotations. It must read the policy as it is then.
func TestTheDeleteGateRereadsThePolicyAfterConnect(t *testing.T) {
	cases := []struct {
		name       string
		set        map[string]string // what Connect writes onto the object
		wantDelete int
	}{
		{"control: nothing changes", nil, 1},
		{"Connect sets deletion-policy orphan", map[string]string{meta.AnnotationKeyDeletionPolicy: meta.DeletionPolicyOrphan}, 0},
		{"Connect sets an unrecognised deletion-policy", map[string]string{meta.AnnotationKeyDeletionPolicy: "orphaned"}, 0},
		{"Connect sets management-policy observe-create-update", map[string]string{meta.AnnotationKeyManagementPolicy: meta.ManagementPolicyObserveCreateUpdate}, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := newWidget("w", map[string]string{})
			w.SetFinalizers([]string{FinalizerName})
			ts := metav1.NewTime(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
			w.SetDeletionTimestamp(&ts)
			s := newTraceServer(t, w)
			var calls mgmtTableCalls
			st := mgmtTableState{name: "deleting-present", exists: true, upToDate: true, deleting: true}
			r := NewReconciler(&fake.Manager{Client: s.c, Scheme: s.scheme}, resource.ManagedKind(widgetGVK),
				WithExternalConnecter(ExternalConnectorFn(func(_ context.Context, mg resource.Managed) (ExternalClient, error) {
					if tc.set != nil {
						meta.AddAnnotations(mg, tc.set)
					}
					return mgmtTableClient(st, &calls), nil
				})),
				WithRecorder(&eventCapture{}),
			)
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			if _, err := r.Reconcile(ctx, reqFor("w")); err != nil {
				t.Logf("Reconcile: %v", err)
			}
			if calls.observe != 1 {
				t.Fatalf("observe called %d times, want 1: the pass didn't reach the second gate", calls.observe)
			}
			if calls.delete != tc.wantDelete {
				t.Errorf("Delete called %d times, want %d", calls.delete, tc.wantDelete)
			}
		})
	}
}

// mgmtTableObjectAnnotations is an object carrying a row's two policy cells
// ("unset" is the annotation absent).
func mgmtTableObjectAnnotations(mp, dp string) *fake.Managed {
	m := &fake.Managed{}
	a := map[string]string{}
	if mp != "unset" {
		a[meta.AnnotationKeyManagementPolicy] = mp
	}
	if dp != "unset" {
		a[meta.AnnotationKeyDeletionPolicy] = dp
	}
	m.SetAnnotations(a)
	return m
}
