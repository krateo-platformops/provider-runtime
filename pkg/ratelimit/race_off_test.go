//go:build !race

package ratelimit

// defaultModelRuns is TestGatedQueueMatchesTheModel's run count when
// RLGOV_MODEL_RUNS is unset.
const defaultModelRuns = 200
