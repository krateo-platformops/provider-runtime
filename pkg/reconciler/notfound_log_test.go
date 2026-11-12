package reconciler

import (
	"bytes"
	"context"
	"log/slog"
	"testing"

	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/krateo-platformops/provider-runtime/pkg/logging"
)

// A reconcile of an object that is gone, the normal end of a deletion, logs
// nothing at ERROR, returns no error and doesn't requeue. Reported from a live
// cluster, where every normal deletion logged "Cannot get managed resource" at
// ERROR level.
func TestNotFoundIsNotAnError(t *testing.T) {
	var buf bytes.Buffer
	log := logging.NewSlogLogger(*slog.New(logging.NewOTelJSONHandler(slog.LevelDebug, &buf)))
	s := newTraceServer(t)
	r := newTraceReconciler(s, &widgetProvider{ops: s.ops}, WithLogger(log))

	got, err := r.Reconcile(context.Background(), reqFor("gone"))
	if err != nil {
		t.Errorf("Reconcile of a missing object returned %v, want nil", err)
	}
	if got != (reconcile.Result{}) {
		t.Errorf("Reconcile of a missing object returned %+v, want an empty Result", got)
	}
	gone := false
	for _, rec := range jsonRecords(t, &buf) {
		if lvl, _ := rec["level"].(string); lvl == "ERROR" {
			t.Errorf("an ERROR record on NotFound: %v", rec)
		}
		if rec["msg"] == "Managed resource is gone" {
			gone = true
		}
	}
	if !gone {
		t.Errorf("records = %s, want a Debug \"Managed resource is gone\"", buf.String())
	}
}
