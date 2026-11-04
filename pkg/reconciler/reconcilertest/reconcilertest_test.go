package reconcilertest

import (
	"context"
	"fmt"
	"strings"
	"testing"

	prv1 "github.com/krateo-platformops/provider-runtime/apis/common/v1"
	"github.com/krateo-platformops/provider-runtime/pkg/errors"
	"github.com/krateo-platformops/provider-runtime/pkg/reconciler"
	"github.com/krateo-platformops/provider-runtime/pkg/resource"
	"github.com/krateo-platformops/provider-runtime/pkg/resource/fake"
)

// recordingTB is a testing.TB double that records failures instead of failing the real test.
// Embedding testing.TB satisfies its unexported method; only what the helper calls is overridden.
type recordingTB struct {
	testing.TB
	failures []string
}

func (r *recordingTB) Helper() {}

func (r *recordingTB) Errorf(format string, args ...any) {
	r.failures = append(r.failures, fmt.Sprintf(format, args...))
}

func observer(fn func(mg resource.Managed) (reconciler.ExternalObservation, error)) reconciler.ExternalClient {
	return &reconciler.ExternalClientFns{
		ObserveFn: func(_ context.Context, mg resource.Managed) (reconciler.ExternalObservation, error) { return fn(mg) },
	}
}

func TestAssertObserveSetsReady(t *testing.T) {
	errWrite := errors.New("cannot update status")

	cases := map[string]struct {
		client reconciler.ExternalClient
		// start is the Ready the fixture carries. The helper must replace it with NotObserved,
		// or a client that never sets Ready would pass on a fixture that already had one.
		start       prv1.Condition
		wantFail    bool
		wantMessage string
	}{
		"SetsReady": {
			client: observer(func(mg resource.Managed) (reconciler.ExternalObservation, error) {
				mg.SetConditions(prv1.Available())
				return reconciler.ExternalObservation{ResourceExists: true, ResourceUpToDate: true}, nil
			}),
		},
		"LeavesReady": {
			client: observer(func(resource.Managed) (reconciler.ExternalObservation, error) {
				return reconciler.ExternalObservation{ResourceExists: true, ResourceUpToDate: true}, nil
			}),
			wantFail:    true,
			wantMessage: "every successful Observe must set Ready",
		},
		"LeavesReadyOnAFixtureThatHadOne": {
			client: observer(func(resource.Managed) (reconciler.ExternalObservation, error) {
				return reconciler.ExternalObservation{ResourceExists: true, ResourceUpToDate: true}, nil
			}),
			start:       prv1.Available(),
			wantFail:    true,
			wantMessage: "every successful Observe must set Ready",
		},
		"ReadyAssessedAfterSettingReady": {
			client: observer(func(mg resource.Managed) (reconciler.ExternalObservation, error) {
				mg.SetConditions(prv1.Available())
				return reconciler.ExternalObservation{}, reconciler.ReadyAssessed(errWrite)
			}),
		},
		"ReadyAssessedWithoutSettingReady": {
			client: observer(func(resource.Managed) (reconciler.ExternalObservation, error) {
				return reconciler.ExternalObservation{}, reconciler.ReadyAssessed(errWrite)
			}),
			wantFail:    true,
			wantMessage: "ReadyAssessed promises that Observe set Ready",
		},
		"PlainError": {
			client: observer(func(mg resource.Managed) (reconciler.ExternalObservation, error) {
				mg.SetConditions(prv1.Available())
				return reconciler.ExternalObservation{}, errWrite
			}),
			wantFail:    true,
			wantMessage: "not a path on which Observe owns Ready",
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			mg := &fake.Managed{}
			if tc.start.Type != "" {
				mg.SetConditions(tc.start)
			}
			tb := &recordingTB{TB: t}

			AssertObserveSetsReady(tb, context.Background(), tc.client, mg)

			if failed := len(tb.failures) > 0; failed != tc.wantFail {
				t.Fatalf("want failure %t, got failures %q", tc.wantFail, tb.failures)
			}
			if tc.wantFail && !strings.Contains(tb.failures[0], tc.wantMessage) {
				t.Errorf("want a failure naming %q, got %q", tc.wantMessage, tb.failures[0])
			}
		})
	}
}
