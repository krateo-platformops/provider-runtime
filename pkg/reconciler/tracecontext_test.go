package reconciler

import (
	"testing"
	"time"
)

// The default window is tracecontext.DefaultContinueWindow. Pin its value, so
// a change in plumbing is a deliberate change here too (docs/design-tracing.md
// §6.2, "The window").
func TestDefaultTraceContinueWindow(t *testing.T) {
	if defaultTraceContinueWindow != 10*time.Minute {
		t.Errorf("defaultTraceContinueWindow = %v, want 10m", defaultTraceContinueWindow)
	}
}
