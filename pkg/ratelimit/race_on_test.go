//go:build race

package ratelimit

// defaultModelRuns is TestGatedQueueMatchesTheModel's run count when
// RLGOV_MODEL_RUNS is unset. The race detector makes each run several hundred
// times slower, so a longer race run sets RLGOV_MODEL_RUNS and a longer
// -timeout explicitly.
const defaultModelRuns = 20
