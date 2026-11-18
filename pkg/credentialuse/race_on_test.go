//go:build race

package credentialuse

// raceEnabled: the tests run under the race detector, which makes each
// schedule several times slower, so the model explores less (model_test.go).
const raceEnabled = true
