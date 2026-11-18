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
//	                   observe-delete, observe, observe-create (a value the
//	                   runtime does not know)
//	deletion-policy    unset (no annotation), delete, orphan
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
// then state inner: 90 rows and the header. The API server writes of each row
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

// The policy values; "" is the annotation unset. observe-create is not a
// value the runtime knows.
var (
	mgmtTableManagementPolicies = []string{
		"",
		meta.ManagementPolicyDefault,
		meta.ManagementPolicyObserveCreateUpdate,
		meta.ManagementPolicyObserveDelete,
		meta.ManagementPolicyObserve,
		"observe-create",
	}
	mgmtTableDeletionPolicies = []string{
		"",
		meta.DeletionPolicyDelete,
		meta.DeletionPolicyOrphan,
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
	if len(rows) != 91 {
		t.Fatalf("the table has %d lines, header included; want 91 (90 cases and the header)", len(rows))
	}
	for i, r := range rows {
		if len(r) != len(mgmtTableColumns) {
			t.Fatalf("line %d has %d cells, want %d: %q", i, len(r), len(mgmtTableColumns), r)
		}
	}

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
