package tracing

import "testing"

// The credentialuse.acquire event and its keys are a contract with the
// collector's queries (docs/design-tracing.md §4.5): pinned here, so a
// rename shows up as a contract change.
func TestCredentialUseEventNames(t *testing.T) {
	pins := map[string]string{
		EventCredentialUseAcquire:        "credentialuse.acquire",
		EventCredentialUseRelease:        "credentialuse.release",
		string(AttrCredentialUseRefs):    "provider_runtime.credentialuse.refs",
		string(AttrCredentialUseHeld):    "provider_runtime.credentialuse.held",
		string(AttrCredentialUseOutcome): "provider_runtime.credentialuse.outcome",
		AttrCredentialUseUnheldPrefix:    "provider_runtime.credentialuse.unheld.",
		CredentialUseOK:                  "ok",
		CredentialUseDegraded:            "degraded",
		CredentialUseMustNotConnect:      "must_not_connect",
		CredentialUseConflict:            "conflict",
		string(StepCredentialUse):        "credential_use",
	}
	for got, want := range pins {
		if got != want {
			t.Errorf("%q, want %q", got, want)
		}
	}
}
