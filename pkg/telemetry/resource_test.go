package telemetry

import (
	"context"
	"strings"
	"testing"
)

func TestNewResourceServiceName(t *testing.T) {
	cases := map[string]struct {
		field, envName, envAttrs string
		want                     string
	}{
		"the field applies":                     {field: "git-provider", want: "git-provider"},
		"OTEL_SERVICE_NAME wins":                {field: "git-provider", envName: "from-env", want: "from-env"},
		"OTEL_RESOURCE_ATTRIBUTES wins":         {field: "git-provider", envAttrs: "service.name=from-attrs", want: "from-attrs"},
		"other resource attributes don't block": {field: "git-provider", envAttrs: "team=platform", want: "git-provider"},
		"OTEL_SERVICE_NAME alone is unchanged":  {envName: "from-env", want: "from-env"},
		// Nothing set: the SDK default as before, never a shared library name.
		"nothing set keeps the SDK default": {want: "unknown_service:"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Setenv("OTEL_SERVICE_NAME", tc.envName)
			t.Setenv("OTEL_RESOURCE_ATTRIBUTES", tc.envAttrs)
			res, err := newResource(context.Background(), tc.field)
			if err != nil {
				t.Fatal(err)
			}
			v, ok := res.Set().Value(serviceNameKey)
			if !ok {
				t.Fatal("the resource has no service.name")
			}
			if got := v.AsString(); !strings.HasPrefix(got, tc.want) {
				t.Errorf("service.name = %q, want it to start with %q", got, tc.want)
			}
		})
	}
}

// A malformed OTEL_RESOURCE_ATTRIBUTES must not stop metrics, as it doesn't with
// resource.Default alone.
func TestNewResourceToleratesMalformedEnv(t *testing.T) {
	t.Setenv("OTEL_SERVICE_NAME", "")
	t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "not-a-pair")
	res, err := newResource(context.Background(), "git-provider")
	if err != nil {
		t.Fatalf("newResource: %v", err)
	}
	if v, _ := res.Set().Value(serviceNameKey); v.AsString() != "git-provider" {
		t.Errorf("service.name = %q, want git-provider", v.AsString())
	}
}
