package credentialuse

import (
	"os"
	"os/exec"
	"strings"
	"testing"
)

// TestDependsOnClientGoOnly checks that the package, without its tests,
// imports nothing from controller-runtime: runtimes without a manager use it
// (§7.1).
func TestDependsOnClientGoOnly(t *testing.T) {
	gobin, err := exec.LookPath("go")
	if err != nil {
		if os.Getenv("CI") != "" {
			t.Fatal("the go command isn't on PATH")
		}
		t.Skip("the go command isn't on PATH")
	}
	out, err := exec.Command(gobin, "list", "-deps", "-f", "{{.ImportPath}}", ".").CombinedOutput()
	if err != nil {
		t.Fatalf("go list -deps: %v\n%s", err, out)
	}
	deps := strings.Fields(string(out))
	if len(deps) < 10 {
		t.Fatalf("go list -deps listed only %d packages: %q", len(deps), deps)
	}
	for _, d := range deps {
		if strings.HasPrefix(d, "sigs.k8s.io/controller-runtime") {
			t.Errorf("pkg/credentialuse depends on %s", d)
		}
	}
}
