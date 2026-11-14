package credentialuse

import (
	"time"

	"k8s.io/apimachinery/pkg/types"
)

// Resets for the model's in-place restart (harness_test.go): a process's
// memory goes back to a fresh process's, in the same objects. The access
// hooks stay.

func (v *verified) resetForTest() {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.written = map[types.UID]map[string]time.Time{}
	v.delivered = map[types.UID]string{}
	v.recordSince = map[types.UID]map[ID]time.Time{}
	v.observed = map[types.UID]map[ID]time.Time{}
	v.lastList = time.Time{}
}

func (f *firstSight) resetForTest() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.at = map[types.UID]time.Time{}
}
