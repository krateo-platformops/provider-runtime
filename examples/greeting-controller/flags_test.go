package main

import (
	"flag"
	"io"
	"os"
	"testing"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/manager"

	"github.com/krateo-platformops/provider-runtime/pkg/controller"
	"github.com/krateo-platformops/provider-runtime/pkg/ratelimit"
)

const maxHoldEnv = "GREETING_PROVIDER_RATE_LIMIT_MAX_HOLD"

// --rate-limit-max-hold defaults to its env, then to 1h; the flag wins over
// the env, and 0 from either is no cap.
func TestRateLimitMaxHoldFlag(t *testing.T) {
	cases := map[string]struct {
		env  *string
		args []string
		want time.Duration
	}{
		"unset is 1h":          {want: ratelimit.DefaultMaxHold},
		"env sets it":          {env: new("10m"), want: 10 * time.Minute},
		"env 0 is no cap":      {env: new("0"), want: 0},
		"flag sets it":         {args: []string{"--rate-limit-max-hold=15m"}, want: 15 * time.Minute},
		"flag 0 is no cap":     {args: []string{"--rate-limit-max-hold=0"}, want: 0},
		"flag wins over env":   {env: new("10m"), args: []string{"--rate-limit-max-hold=2h"}, want: 2 * time.Hour},
		"flag 0 wins over env": {env: new("10m"), args: []string{"--rate-limit-max-hold=0s"}, want: 0},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Setenv(maxHoldEnv, "") // restores the caller's value after the test
			if tc.env != nil {
				t.Setenv(maxHoldEnv, *tc.env)
			} else if err := os.Unsetenv(maxHoldEnv); err != nil {
				t.Fatal(err)
			}
			var o controller.RateLimitOptions
			fs := flag.NewFlagSet("greeting-controller", flag.ContinueOnError)
			rateLimitFlags(fs, &o)
			if err := fs.Parse(tc.args); err != nil {
				t.Fatal(err)
			}
			if o.MaxHold == nil {
				t.Fatal("MaxHold is nil after parsing: the flag's value never reaches the options")
			}
			if *o.MaxHold != tc.want {
				t.Errorf("MaxHold = %v, want %v", *o.MaxHold, tc.want)
			}
		})
	}
}

// A negative --rate-limit-max-hold parses, and SetupRateLimitGovernor rejects
// it, so the provider fails at start rather than running uncapped.
func TestNegativeRateLimitMaxHoldIsRejected(t *testing.T) {
	o := controller.DefaultOptions()
	fs := flag.NewFlagSet("greeting-controller", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	rateLimitFlags(fs, &o.RateLimit)
	if err := fs.Parse([]string{"--rate-limit-governor", "--rate-limit-max-hold=-1m"}); err != nil {
		t.Fatal(err)
	}
	if err := o.SetupRateLimitGovernor(nopAdder{}); err == nil {
		t.Error("SetupRateLimitGovernor accepted --rate-limit-max-hold=-1m")
	}
}

type nopAdder struct{}

func (nopAdder) Add(manager.Runnable) error { return nil }
