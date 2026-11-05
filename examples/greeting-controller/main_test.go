package main

import (
	"context"
	"testing"

	"github.com/krateo-platformops/provider-runtime/pkg/logging"
	"github.com/krateo-platformops/provider-runtime/pkg/meta"
	"github.com/krateo-platformops/provider-runtime/pkg/reconciler/reconcilertest"
)

// Every success path of Observe must set Ready (the provider-runtime Ready
// contract). One case per path, so a new path without a case stands out in
// review and a path that stops setting Ready fails here.
func TestObserveSetsReady(t *testing.T) {
	cases := map[string]struct {
		store        externalStore
		externalName string
		message      string
	}{
		"NotCreatedYet":  {store: externalStore{}},
		"GoneExternally": {store: externalStore{}, externalName: "hello", message: "hi"},
		"OutOfDate":      {store: externalStore{"hello": "old"}, externalName: "hello", message: "hi"},
		"UpToDate":       {store: externalStore{"hello": "hi"}, externalName: "hello", message: "hi"},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			g := &Greeting{Spec: GreetingSpec{Message: tc.message}}
			if tc.externalName != "" {
				meta.SetExternalName(g, tc.externalName)
			}
			e := &external{log: logging.NewNopLogger(), store: tc.store}

			_, _ = reconcilertest.AssertObserveSetsReady(t, context.Background(), e, g)
		})
	}
}
