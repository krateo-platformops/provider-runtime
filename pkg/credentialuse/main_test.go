package credentialuse_test

import (
	"fmt"
	"os"
	"testing"

	"github.com/krateo-platformops/provider-runtime/pkg/credentialuse/internal/cutest"
)

// env is the envtest API server, or nil when KUBEBUILDER_ASSETS is unset.
var env *cutest.Env

func TestMain(m *testing.M) {
	e, err := cutest.Start()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	env = e
	code := m.Run()
	if err := env.Stop(); err != nil {
		fmt.Fprintln(os.Stderr, "stopping envtest:", err)
	}
	os.Exit(code)
}
