// Command measure is examples/greeting-controller with a synthetic-load Observe,
// used to measure the saturation thresholds (design open question 5, and §11
// items 14 to 16). It is its own module, so it is not part of the library's
// build, tests or lint.
//
// Env:
//
//	WORKERS             MaxConcurrentReconciles (default 16)
//	POLL_MS             poll interval in ms (default 2000)
//	LOAD_ALLOC_MIB      MiB each Observe allocates and touches (default 0)
//	LOAD_HOLD_MS        how long Observe then waits, as a call to an external
//	                    API would, holding its allocation if any (default 0)
//	LOAD_CPU_MS         ms of CPU time each Observe spends, measured on its
//	                    own thread (burn.go) (default 0)
//	SATURATION          "true" enables the saturation input
//	EXPERIMENTAL_MEMORY "true" enables the experimental memory input
//	SAT_CPU             CPU pressure threshold (default: the library's)
//	SAT_SOFT, SAT_HARD  working-set ratios (default: the library's)
//	SAT_PERIOD_MS       sample period (default: the library's)
//	SAMPLER_OVERHEAD    "true" measures the sampler's cost and nothing else
//	PHASE_S             length of each overhead phase (default 60)
//	GOGC_PERCENT        GOGC (default 100)
//
// opts_new.go reads the settings the control build doesn't have.
package main

import (
	"context"
	"fmt"
	"os"
	"runtime/debug"
	gometrics "runtime/metrics"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	prv1 "github.com/krateo-platformops/provider-runtime/apis/common/v1"
	"github.com/krateo-platformops/provider-runtime/pkg/controller"
	"github.com/krateo-platformops/provider-runtime/pkg/logging"
	"github.com/krateo-platformops/provider-runtime/pkg/meta"
	"github.com/krateo-platformops/provider-runtime/pkg/ratelimit"
	"github.com/krateo-platformops/provider-runtime/pkg/reconciler"
	"github.com/krateo-platformops/provider-runtime/pkg/resource"
)

var GroupVersion = schema.GroupVersion{Group: "examples.krateo.io", Version: "v1"}

type GreetingSpec struct {
	Message string `json:"message,omitempty"`
}
type GreetingStatus struct {
	prv1.ConditionedStatus `json:",inline"`
}
type Greeting struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              GreetingSpec   `json:"spec,omitempty"`
	Status            GreetingStatus `json:"status,omitempty"`
}

func (g *Greeting) GetCondition(ct prv1.ConditionType) prv1.Condition {
	return g.Status.GetCondition(ct)
}
func (g *Greeting) SetConditions(c ...prv1.Condition) { g.Status.SetConditions(c...) }
func (g *Greeting) DeepCopyObject() runtime.Object {
	out := &Greeting{TypeMeta: g.TypeMeta, Spec: g.Spec}
	g.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	if cs := g.Status.ConditionedStatus.DeepCopy(); cs != nil {
		out.Status.ConditionedStatus = *cs
	}
	return out
}

type GreetingList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Greeting `json:"items"`
}

func (gl *GreetingList) DeepCopyObject() runtime.Object {
	out := &GreetingList{TypeMeta: gl.TypeMeta}
	gl.ListMeta.DeepCopyInto(&out.ListMeta)
	out.Items = make([]Greeting, len(gl.Items))
	for i := range gl.Items {
		out.Items[i] = *gl.Items[i].DeepCopyObject().(*Greeting)
	}
	return out
}

type store struct {
	mu sync.Mutex
	m  map[string]string
}

type load struct{ allocMiB, holdMS, cpuMS int }

var (
	observes atomic.Int64
	inFlight atomic.Int64
	sink     []byte // defeats escape analysis
	// burns, burnCPUNs and burnWallNs total the CPU work: calls, the CPU time
	// they measured, and the wall time they took, which throttling stretches.
	burns, burnCPUNs, burnWallNs atomic.Int64
)

type connector struct {
	s *store
	l load
}

func (c *connector) Connect(context.Context, resource.Managed) (reconciler.ExternalClient, error) {
	return &external{s: c.s, l: c.l}, nil
}
func (c *connector) RateLimitKey(resource.Managed) (string, error) { return "greeting-store", nil }

type external struct {
	s *store
	l load
}

// burn spends ms of CPU time (burn.go) and adds it to the totals STAT reports.
func burn(ms int) {
	start := time.Now()
	used := burnCPU(time.Duration(ms) * time.Millisecond)
	burnWallNs.Add(int64(time.Since(start)))
	burnCPUNs.Add(int64(used))
	burns.Add(1)
}

func (e *external) Observe(_ context.Context, mg resource.Managed) (reconciler.ExternalObservation, error) {
	inFlight.Add(1)
	defer inFlight.Add(-1)
	defer observes.Add(1)
	if e.l.allocMiB > 0 {
		b := make([]byte, e.l.allocMiB<<20)
		for i := 0; i < len(b); i += 4096 {
			b[i] = 1
		}
		if e.l.cpuMS > 0 {
			burn(e.l.cpuMS)
		}
		time.Sleep(time.Duration(e.l.holdMS) * time.Millisecond)
		sink = b[:1]
	} else {
		if e.l.cpuMS > 0 {
			burn(e.l.cpuMS)
		}
		time.Sleep(time.Duration(e.l.holdMS) * time.Millisecond)
	}
	g := mg.(*Greeting)
	name := meta.GetExternalName(g)
	e.s.mu.Lock()
	got, ok := e.s.m[name]
	e.s.mu.Unlock()
	if name == "" || !ok {
		g.SetConditions(prv1.Unavailable())
		return reconciler.ExternalObservation{}, nil
	}
	g.SetConditions(prv1.Available())
	return reconciler.ExternalObservation{ResourceExists: true, ResourceUpToDate: got == g.Spec.Message}, nil
}
func (e *external) Create(_ context.Context, mg resource.Managed) error {
	g := mg.(*Greeting)
	meta.SetExternalName(g, g.GetName())
	e.s.mu.Lock()
	e.s.m[g.GetName()] = g.Spec.Message
	e.s.mu.Unlock()
	return nil
}
func (e *external) Update(_ context.Context, mg resource.Managed) error {
	g := mg.(*Greeting)
	e.s.mu.Lock()
	e.s.m[meta.GetExternalName(g)] = g.Spec.Message
	e.s.mu.Unlock()
	return nil
}
func (e *external) Delete(_ context.Context, mg resource.Managed) error {
	g := mg.(*Greeting)
	e.s.mu.Lock()
	delete(e.s.m, meta.GetExternalName(g))
	e.s.mu.Unlock()
	return nil
}

// recorder counts deferrals by cause and keeps the snapshot source.
type recorder struct {
	mu        sync.Mutex
	deferrals map[string]int64
	snap      func() ratelimit.Snapshot
}

func (r *recorder) IncRateLimitDeferral(_ context.Context, _ string, reason ratelimit.DeferReason) {
	r.mu.Lock()
	r.deferrals[reason.Cause()]++
	r.mu.Unlock()
}
func (r *recorder) IncRateLimitSignal(context.Context, string, ratelimit.SignalKind) {}

// IncRateLimitHoldClamped is MetricsRecorder's since #45's MaxHold. The control
// build, v1.10.0, doesn't ask for it, and an extra method is harmless there.
func (r *recorder) IncRateLimitHoldClamped(context.Context, string) {}
func (r *recorder) ObserveRateLimit(s func() ratelimit.Snapshot)    { r.snap = s }

func envInt(k string, d int) int {
	if v, err := strconv.Atoi(os.Getenv(k)); err == nil {
		return v
	}
	return d
}
func envFloat(k string) float64 {
	v, _ := strconv.ParseFloat(os.Getenv(k), 64)
	return v
}

// readFile reads a cgroup file through an os.Root opened at the cgroup mount,
// so name can't reach outside it (gosec G304), as pkg/ratelimit's sampler does.
func readFile(name string) string {
	root, err := os.OpenRoot("/sys/fs/cgroup")
	if err != nil {
		return ""
	}
	defer func() { _ = root.Close() }()
	b, _ := root.ReadFile(name)
	return strings.TrimSpace(string(b))
}

func avg10(file, line string) string {
	for _, l := range strings.Split(readFile(file), "\n") {
		if strings.HasPrefix(l, line+" ") {
			for _, f := range strings.Fields(l) {
				if v, ok := strings.CutPrefix(f, "avg10="); ok {
					return v
				}
			}
		}
	}
	return "na"
}

func statValue(key string) uint64 {
	for _, l := range strings.Split(readFile("memory.stat"), "\n") {
		if k, v, ok := strings.Cut(l, " "); ok && k == key {
			n, _ := strconv.ParseUint(v, 10, 64)
			return n
		}
	}
	return 0
}

// nodeReport records what the node is: its kernel, the cgroup version, and
// whether cpu.pressure exists. The kernel and cgroup setup decide what a run
// can be compared with.
func nodeReport() {
	version, _ := os.ReadFile("/proc/version")
	cgroupV2 := "false"
	if _, err := os.Stat("/sys/fs/cgroup/cgroup.controllers"); err == nil {
		cgroupV2 = "true"
	}
	_, pressureErr := os.Stat("/sys/fs/cgroup/cpu.pressure")
	_, memPressureErr := os.Stat("/sys/fs/cgroup/memory.pressure")
	fmt.Printf("LOG node kernel=%q cgroup_v2=%s cpu_pressure_file=%t memory_pressure_file=%t memory_max=%s cpu_max=%q\n",
		strings.TrimSpace(string(version)), cgroupV2, pressureErr == nil, memPressureErr == nil, readFile("memory.max"), readFile("cpu.max"))
}

// nodeLoad is the node's 1-minute load average: /proc/loadavg isn't namespaced.
func nodeLoad() string {
	b, _ := os.ReadFile("/proc/loadavg")
	if f := strings.Fields(string(b)); len(f) > 0 {
		return f[0]
	}
	return "na"
}

func main() {
	debug.SetGCPercent(envInt("GOGC_PERCENT", 100))
	nodeReport()
	if os.Getenv("SAMPLER_OVERHEAD") == "true" {
		samplerOverhead()
		return
	}
	workers := envInt("WORKERS", 16)
	poll := time.Duration(envInt("POLL_MS", 2000)) * time.Millisecond
	l := load{allocMiB: envInt("LOAD_ALLOC_MIB", 0), holdMS: envInt("LOAD_HOLD_MS", 0), cpuMS: envInt("LOAD_CPU_MS", 0)}

	scheme := runtime.NewScheme()
	scheme.AddKnownTypes(GroupVersion, &Greeting{}, &GreetingList{})
	metav1.AddToGroupVersion(scheme, GroupVersion)

	var cacheOpts cache.Options
	if ns := os.Getenv("WATCH_NAMESPACE"); ns != "" {
		cacheOpts.DefaultNamespaces = map[string]cache.Config{ns: {}}
	}
	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Cache:                  cacheOpts,
		Scheme:                 scheme,
		HealthProbeBindAddress: ":8081",
		Metrics:                metricsserver.Options{BindAddress: "0"},
	})
	if err != nil {
		panic(err)
	}
	if err := mgr.AddHealthzCheck("ping", healthz.Ping); err != nil {
		panic(err)
	}

	rec := &recorder{deferrals: map[string]int64{}}
	o := controller.DefaultOptions()
	o.MaxConcurrentReconciles = workers
	o.RateLimit.Saturation.Enabled = os.Getenv("SATURATION") == "true"
	o.RateLimit.Saturation.CPUPressureThreshold = envFloat("SAT_CPU")
	o.RateLimit.Saturation.WorkingSetSoftRatio = envFloat("SAT_SOFT")
	o.RateLimit.Saturation.WorkingSetHardRatio = envFloat("SAT_HARD")
	o.RateLimit.Saturation.SamplePeriod = time.Duration(envInt("SAT_PERIOD_MS", 0)) * time.Millisecond
	applyExperimental(&o.RateLimit.Saturation)
	o.RateLimit.Metrics = rec
	o.Logger = stdoutLogger{}
	if err := o.SetupRateLimitGovernor(mgr); err != nil {
		panic(err)
	}

	r := reconciler.NewReconciler(mgr,
		resource.ManagedKind(GroupVersion.WithKind("Greeting")),
		reconciler.WithExternalConnecter(&connector{s: &store{m: map[string]string{}}, l: l}),
		reconciler.WithPollInterval(poll),
		reconciler.WithRateLimitGovernor(o.RateLimitGovernor),
	)
	if err := ctrl.NewControllerManagedBy(mgr).Named("greeting").For(&Greeting{}).
		WithOptions(o.ForControllerRuntime()).Complete(r); err != nil {
		panic(err)
	}

	go func() {
		start := time.Now()
		var last int64
		var lastUsage uint64
		var lastBurns, lastBurnCPU, lastBurnWall int64
		for range time.Tick(time.Second) {
			n := observes.Load()
			// The CPU work of the calls that finished in this second, per call:
			// the CPU time measured on their threads, and the wall time it took.
			nb, bc, bw := burns.Load(), burnCPUNs.Load(), burnWallNs.Load()
			burnCPU, burnWall := "na", "na"
			if d := nb - lastBurns; d > 0 {
				burnCPU = fmt.Sprintf("%.1f", float64(bc-lastBurnCPU)/float64(d)/1e6)
				burnWall = fmt.Sprintf("%.1f", float64(bw-lastBurnWall)/float64(d)/1e6)
			}
			lastBurns, lastBurnCPU, lastBurnWall = nb, bc, bw
			usage, _ := strconv.ParseUint(statField("cpu.stat", "usage_usec"), 10, 64)
			cpuMs := float64(usage-lastUsage) / 1000
			lastUsage = usage
			cur, _ := strconv.ParseUint(readFile("memory.current"), 10, 64)
			inactive := statValue("inactive_file")
			limit := readFile("memory.max")
			ws := cur - min(cur, inactive)
			local, sat := "-", "-"
			rec.mu.Lock()
			def := fmt.Sprint(rec.deferrals)
			rec.mu.Unlock()
			if rec.snap != nil {
				s := rec.snap()
				local = strconv.Itoa(s.LocalCapacity)
				var parts []string
				for _, in := range s.Inputs {
					if in.Enabled {
						parts = append(parts, fmt.Sprintf("%s=%.1f", in.Input, in.Percent))
					} else {
						parts = append(parts, in.Input+"=off")
					}
				}
				sat = strings.Join(parts, ",")
			}
			fmt.Printf("STAT t=%.0f cpu_ms/s=%.0f obs/s=%d burn_cpu_ms/call=%s burn_wall_ms/call=%s inflight=%d local=%s cpu_some=%s mem_full=%s mem_some=%s ws_mib=%d max=%s throttled_usec=%s node_load=%s %s deferrals=%s inputs=%s\n",
				time.Since(start).Seconds(), cpuMs, n-last, burnCPU, burnWall, inFlight.Load(), local,
				avg10("cpu.pressure", "some"), avg10("memory.pressure", "full"), avg10("memory.pressure", "some"),
				ws>>20, limit, statField("cpu.stat", "throttled_usec"), nodeLoad(), goStats(), def, sat)
			last = n
		}
	}()

	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		panic(err)
	}
}

// goStats is the harness's own read of the Go runtime, for diagnosis only.
func goStats() string {
	names := []string{"/gc/gomemlimit:bytes", "/gc/heap/live:bytes", "/gc/heap/goal:bytes", "/memory/classes/total:bytes", "/memory/classes/heap/released:bytes", "/gc/cycles/total:gc-cycles", "/cpu/classes/gc/total:cpu-seconds"}
	ss := make([]gometrics.Sample, len(names))
	for i, n := range names {
		ss[i].Name = n
	}
	gometrics.Read(ss)
	lim := ss[0].Value.Uint64()
	limit := "off"
	if lim < 1<<62 {
		limit = strconv.FormatUint(lim>>20, 10)
	}
	return fmt.Sprintf("golimit_mib=%s live_mib=%d goal_mib=%d mapped_mib=%d gc_cycles=%d gc_cpu_s=%.1f",
		limit, ss[1].Value.Uint64()>>20, ss[2].Value.Uint64()>>20, (ss[3].Value.Uint64()-ss[4].Value.Uint64())>>20, ss[5].Value.Uint64(), ss[6].Value.Float64())
}

// stdoutLogger prints what the library logs at Info.
type stdoutLogger struct{}

func (stdoutLogger) Info(msg string, kv ...any)           { fmt.Println("LOG", msg, kv) }
func (stdoutLogger) Debug(string, ...any)                 {}
func (stdoutLogger) Warn(msg string, kv ...any)           { fmt.Println("LOG", msg, kv) }
func (stdoutLogger) Error(err error, m string, kv ...any) { fmt.Println("LOG", err, m, kv) }
func (l stdoutLogger) WithName(string) logging.Logger     { return l }
func (l stdoutLogger) WithValues(...any) logging.Logger   { return l }

func statField(file, key string) string {
	for _, l := range strings.Split(readFile(file), "\n") {
		if k, v, ok := strings.Cut(l, " "); ok && k == key {
			return v
		}
	}
	return "na"
}

// samplerOverhead measures the saturation sampler's CPU cost in this
// container: process CPU (user+sys) over PHASE_S seconds idle, then over the
// same time with only the governor's sampler running at SAT_PERIOD_MS.
func samplerOverhead() {
	phase := time.Duration(envInt("PHASE_S", 60)) * time.Second
	cpu := func() time.Duration {
		var ru syscall.Rusage
		_ = syscall.Getrusage(syscall.RUSAGE_SELF, &ru)
		return time.Duration(ru.Utime.Nano() + ru.Stime.Nano())
	}
	c0 := cpu()
	time.Sleep(phase)
	idle := cpu() - c0

	period := time.Duration(envInt("SAT_PERIOD_MS", 250)) * time.Millisecond
	sat := ratelimit.SaturationOptions{Enabled: true, SamplePeriod: period}
	applyExperimental(&sat)
	g, err := ratelimit.New(ratelimit.Options{MaxConcurrentReconciles: 16, Logger: stdoutLogger{}, Saturation: sat})
	if err != nil {
		panic(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	c1 := cpu()
	go func() { _ = g.Start(ctx) }()
	time.Sleep(phase)
	cancel()
	sampling := cpu() - c1
	samples := float64(phase / period)
	perSecond := float64((sampling - idle).Microseconds()) / phase.Seconds()
	fmt.Printf("OVERHEAD period=%s phase=%s idle_cpu=%s sampling_cpu=%s net_cpu_per_s=%.0fus net_per_sample=%.0fus share_of_one_core=%.4f%% inputs=%v\n",
		period, phase, idle, sampling, perSecond, float64((sampling-idle).Microseconds())/samples, perSecond/1e6*100, g.Snapshot().Inputs)
	time.Sleep(24 * time.Hour)
}
