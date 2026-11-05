package controller

import (
	"time"

	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/krateo-platformops/provider-runtime/pkg/logging"
	"github.com/krateo-platformops/provider-runtime/pkg/ratelimit"
	"github.com/krateo-platformops/provider-runtime/pkg/ratelimiter"
)

// DefaultOptions returns a functional set of options with conservative
// defaults.
func DefaultOptions() Options {
	return Options{
		Logger:                  logging.NewNopLogger(),
		GlobalRateLimiter:       ratelimiter.NewGlobal(1),
		PollInterval:            1 * time.Minute,
		MaxConcurrentReconciles: 1,
	}
}

// Options frequently used by most controllers.
type Options struct {
	// The Logger controllers should use.
	Logger logging.Logger

	// The GlobalRateLimiter used by this controller manager. The rate of
	// reconciles across all controllers will be subject to this limit.
	GlobalRateLimiter workqueue.TypedRateLimiter[any]

	// PollInterval at which each controller should speculatively poll to
	// determine whether it has work to do.
	PollInterval time.Duration

	// MaxConcurrentReconciles for each controller.
	MaxConcurrentReconciles int

	// UsePriorityQueue controls whether controller-runtime should use its
	// priority queue implementation when queue wait telemetry is enabled.
	// Defaults to true.
	UsePriorityQueue *bool

	// QueueWaitRecorder records how long requests spend waiting in the
	// controller queue before a worker starts processing them.
	QueueWaitRecorder QueueWaitRecorder

	// RateLimit configures automatic slow-down under rate limits and
	// controller saturation (docs/design-rate-limit-governor.md). The zero
	// value is off, and every field is safe to leave unset.
	RateLimit RateLimitOptions

	// RateLimitGovernor is the governor SetupRateLimitGovernor built, or nil
	// when RateLimit enables nothing. Controllers pass it to their reconciler
	// with reconciler.WithRateLimitGovernor, which is a no-op for nil.
	// ForControllerRuntime does not read it: the governor gates calls inside
	// the reconciler, not the queue.
	RateLimitGovernor *ratelimit.Governor
}

// RateLimitOptions configures the rate-limit governor. Every field is safe to
// leave unset: the zero value is off, and a provider turns the governor on
// with Enabled alone.
type RateLimitOptions struct {
	// Enabled turns on the rate-limit input: calls of a connecter or client
	// that implements reconciler.RateLimitKeyer are gated per key, and the
	// signals the provider reports slow that key down. Default false.
	Enabled bool

	// RecoveryInterval is how long a key, or the local capacity, must go clean
	// before its capacity rises by one. Default 30s.
	RecoveryInterval time.Duration

	// Saturation turns on and tunes the local input, the controller's own CPU
	// saturation, independently of Enabled. Default off, and its threshold
	// defaults to a provisional value. Memory is a separate, experimental
	// option in it, also off by default.
	Saturation ratelimit.SaturationOptions

	// Metrics records the governor's metrics, for example *telemetry.Metrics.
	// Default none.
	Metrics ratelimit.MetricsRecorder
}

// RunnableAdder is the part of a controller-runtime manager.Manager that
// SetupRateLimitGovernor uses.
type RunnableAdder interface {
	Add(manager.Runnable) error
}

// SetupRateLimitGovernor builds the governor o.RateLimit describes, adds it to
// mgr so its saturation sampler runs, and stores it in o.RateLimitGovernor.
// When neither RateLimit.Enabled nor RateLimit.Saturation.Enabled is set it
// does nothing and leaves RateLimitGovernor nil. Call it once, before the
// controllers are set up, and share o with them.
//
// The governor's ceiling comes from o.MaxConcurrentReconciles (at least 1),
// times the reconcilers that register with it.
func (o *Options) SetupRateLimitGovernor(mgr RunnableAdder) error {
	rl := o.RateLimit
	if !rl.Enabled && !rl.Saturation.Enabled {
		o.RateLimitGovernor = nil
		return nil
	}
	logger := o.Logger
	if logger == nil {
		logger = logging.NewNopLogger()
	}
	g, err := ratelimit.New(ratelimit.Options{
		MaxConcurrentReconciles: max(1, o.MaxConcurrentReconciles),
		IgnoreSignals:           !rl.Enabled,
		RecoveryInterval:        rl.RecoveryInterval,
		Saturation:              rl.Saturation,
		Logger:                  logger.WithName("ratelimit"),
		Metrics:                 rl.Metrics,
	})
	if err != nil {
		return err
	}
	if err := mgr.Add(g); err != nil {
		return err
	}
	o.RateLimitGovernor = g
	return nil
}

// ForControllerRuntime extracts options for controller-runtime.
func (o Options) ForControllerRuntime() controller.Options {
	controllerOptions := controller.TypedOptions[reconcile.Request]{
		MaxConcurrentReconciles: o.MaxConcurrentReconciles,
		RateLimiter:             workqueue.NewTypedItemExponentialFailureRateLimiter[reconcile.Request](1*time.Second, 60*time.Second),
	}

	if o.QueueWaitRecorder != nil {
		usePriorityQueue := true
		if o.UsePriorityQueue != nil {
			usePriorityQueue = *o.UsePriorityQueue
		}

		controllerOptions.NewQueue = func(controllerName string, rateLimiter workqueue.TypedRateLimiter[reconcile.Request]) workqueue.TypedRateLimitingInterface[reconcile.Request] {
			return newQueueWithWaitMetrics(controllerName, rateLimiter, usePriorityQueue, o.QueueWaitRecorder)
		}
	}

	return controllerOptions
}
