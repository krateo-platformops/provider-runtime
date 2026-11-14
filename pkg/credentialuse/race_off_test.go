//go:build !race

package credentialuse

// raceEnabled: the tests run under the race detector (see race_on_test.go).
const raceEnabled = false
