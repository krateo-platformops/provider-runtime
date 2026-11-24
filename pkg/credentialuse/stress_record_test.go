package credentialuse_test

// The stress test's recorder: what stress_history_test.go's audits rest on.
// It watches every version of the objects, sees every call the processes make
// through their metadata clients, with its answer, follows the passes, and
// makes the test's own creates and DELETEs, so that it has their answers.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/metadata"

	cu "github.com/krateo-platformops/provider-runtime/pkg/credentialuse"
	"github.com/krateo-platformops/provider-runtime/pkg/credentialuse/internal/cutest"
	"github.com/krateo-platformops/provider-runtime/pkg/credentialuse/internal/driver"
)

// recordedKinds are the kinds the recorder watches.
var recordedKinds = map[string]schema.GroupVersionResource{
	"Widget": cutest.WidgetGVR, "Target": cutest.TargetGVR, "Secret": cutest.SecretGVR, "Namespace": {Version: "v1", Resource: "namespaces"},
}

// kindOfResource is the kind of a resource whose calls are kept: the holder
// kinds' CRDs too, which are called, and not watched.
func kindOfResource(gvr schema.GroupVersionResource) string {
	if gvr.Resource == "customresourcedefinitions" {
		return calledCRD
	}
	for kind, r := range recordedKinds {
		if r == gvr {
			return kind
		}
	}
	return ""
}

// stressRecorder records one stress run.
type stressRecorder struct {
	mode    stressMode
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	passSeq atomic.Int64
	// beforeList are, by namespace, functions that run before each list of its
	// Widgets that a process makes.
	beforeList sync.Map

	mu       sync.Mutex
	versions []stVersion
	calls    []stCall
	passes   []*stPass
	reads    []stRead
	writes   []stWrite
	events   []stEvent
	gaps     []string
	synced   map[int]time.Time
	stopped  time.Time
	frozen   time.Duration
	outside  bool
	ended    map[string]bool // by kind and namespace: its watch has delivered the recording's last object
}

// recordingEnd starts the name of the namespace the run's last objects live
// in (stressRecorder.finish).
const recordingEnd = "stressend-"

// startRecorder lists every object, and watches from there: so the recording
// has every object's state at its start, and every version since.
func startRecorder(t *testing.T, mode stressMode) *stressRecorder {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	r := &stressRecorder{mode: mode, cancel: cancel, ended: map[string]bool{}, synced: map[int]time.Time{}}
	t.Cleanup(func() {
		cancel()
		r.wg.Wait()
	})
	for kind, gvr := range recordedKinds {
		l, err := env.Dynamic.Resource(gvr).List(ctx, metav1.ListOptions{})
		if err != nil {
			t.Fatal(err)
		}
		r.mu.Lock()
		for i := range l.Items {
			r.versions = append(r.versions, versionOf(kind, &l.Items[i]))
		}
		r.mu.Unlock()
		day := int64(24 * 60 * 60) // the API server ends a watch without a timeout after 30 to 60 minutes
		w, err := env.Dynamic.Resource(gvr).Watch(ctx, metav1.ListOptions{ResourceVersion: l.GetResourceVersion(), TimeoutSeconds: &day})
		if err != nil {
			t.Fatal(err)
		}
		r.wg.Add(1)
		go func() {
			defer r.wg.Done()
			defer w.Stop()
			r.watch(ctx, kind, w)
		}()
	}
	r.wg.Go(func() { r.beat(ctx) })
	return r
}

// beat keeps the longest gap between two beats of 100ms, less those: how long
// the machine, or this process, stood still.
func (r *stressRecorder) beat(ctx context.Context) {
	const every = 100 * time.Millisecond
	tick := time.NewTicker(every)
	defer tick.Stop()
	for last := time.Now(); ; {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		now := time.Now()
		r.mu.Lock()
		r.frozen = max(r.frozen, now.Sub(last)-every)
		r.mu.Unlock()
		last = now
	}
}

func (r *stressRecorder) watch(ctx context.Context, kind string, w watch.Interface) {
	for ev := range w.ResultChan() {
		u, ok := ev.Object.(*unstructured.Unstructured)
		if !ok || ev.Type != watch.Added && ev.Type != watch.Modified && ev.Type != watch.Deleted {
			// Ending ctx ends the stream, which the client may report as an
			// ERROR event: that is the recording's end, not a gap in it.
			if ctx.Err() == nil {
				r.gap(fmt.Sprintf("the watch of %s delivered a %s event: %v", kind, ev.Type, ev.Object))
			}
			continue
		}
		v := versionOf(kind, u)
		v.gone = ev.Type == watch.Deleted
		ns := v.ns
		if kind == "Namespace" {
			ns = v.name
		}
		r.mu.Lock()
		if strings.HasPrefix(ns, recordingEnd) {
			r.ended[kind+"/"+ns] = true
		} else {
			r.versions = append(r.versions, v)
		}
		r.mu.Unlock()
	}
	if ctx.Err() == nil {
		r.gap("the watch of " + kind + " ended")
	}
}

// hasSynced and stopping tell that process proc has synced, and that the
// processes are about to be stopped.
func (r *stressRecorder) hasSynced(proc int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.synced[proc] = time.Now()
}

func (r *stressRecorder) stopping() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.stopped = time.Now()
}

func (r *stressRecorder) gap(s string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.gaps = append(r.gaps, s)
}

// finish ends the recording, once the processes have stopped, and returns
// it. It is complete: each watch has delivered an object made after the last
// write, and a watch delivers its kind's events in order.
func (r *stressRecorder) finish(t *testing.T) *stHistory {
	t.Helper()
	ctx := context.Background()
	ns := env.Namespace(t, strings.TrimSuffix(recordingEnd, "-"))
	for _, kind := range []string{"Widget", "Target", "Secret"} {
		u := &unstructured.Unstructured{Object: map[string]any{}}
		u.SetGroupVersionKind(recordedKinds[kind].GroupVersion().WithKind(kind))
		u.SetNamespace(ns)
		u.SetName("end")
		c := env.Dynamic.Resource(recordedKinds[kind]).Namespace(ns)
		if _, err := c.Create(ctx, u, metav1.CreateOptions{}); err != nil {
			r.gap(fmt.Sprintf("creating the run's last %s: %v", kind, err))
			continue
		}
		defer func() { _ = c.Delete(ctx, "end", metav1.DeleteOptions{}) }()
	}
	for kind := range recordedKinds {
		delivered := false
		for end := time.Now().Add(30 * time.Second); !delivered && time.Now().Before(end); time.Sleep(10 * time.Millisecond) {
			r.mu.Lock()
			delivered = r.ended[kind+"/"+ns]
			r.mu.Unlock()
		}
		if !delivered {
			r.gap("the watch of " + kind + " didn't deliver the run's last object")
		}
	}
	r.cancel()
	r.wg.Wait()
	r.mu.Lock()
	defer r.mu.Unlock()
	return newStHistory(stRecording{mode: r.mode, grace: testGrace, versions: r.versions, calls: r.calls, passes: r.passes, reads: r.reads, writes: r.writes, events: r.events,
		gaps: r.gaps, synced: r.synced, stopped: r.stopped, frozen: r.frozen, outside: r.outside})
}

// running reports whether a pass of holder o has begun and hasn't ended.
func (r *stressRecorder) running(o stObj) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.ContainsFunc(r.passes, func(p *stPass) bool { return p.holder == o && p.done.IsZero() })
}

func parseRV(s string) uint64 {
	n, _ := strconv.ParseUint(s, 10, 64)
	return n
}

// versionOf keeps what the audits need of one version of an object.
func versionOf(kind string, u *unstructured.Unstructured) stVersion {
	v := stVersion{stObj: stObj{kind, u.GetNamespace(), u.GetName()}, rv: parseRV(u.GetResourceVersion()), uid: string(u.GetUID()), at: time.Now(),
		fins: u.GetFinalizers(), epochs: epochsOf(u.GetFinalizers(), u.GetAnnotations()), consent: u.GetAnnotations()[cu.AnnotationAllowHoldersFrom]}
	if d := u.GetDeletionTimestamp(); d != nil {
		v.deleting, v.deletedAt = true, d.Time
	}
	v.rec, v.recBad = parseRecord(u.GetAnnotations())
	v.spec = specOf(u)
	v.badLabels = badLabels(u.GetFinalizers(), u.GetLabels())
	return v
}

// specOf is what a holder or a Target names.
func specOf(u *unstructured.Unstructured) string {
	s, _, _ := unstructured.NestedString(u.Object, "spec", "target")
	if s == "" {
		s, _, _ = unstructured.NestedString(u.Object, "spec", "secret")
	}
	return s
}

func epochsOf(fins []string, annotations map[string]string) map[string]string {
	out := map[string]string{}
	for _, f := range credentialUse(fins) {
		out[f] = annotations[f]
	}
	return out
}

// goroutine is the calling goroutine's number, from its stack's header: a
// release batch's calls all run on one, and nothing else tells them apart.
func goroutine() string {
	var buf [40]byte
	fs := strings.Fields(string(buf[:runtime.Stack(buf[:], false)])) // "goroutine 18 [running]:"
	if len(fs) < 2 {
		return "?"
	}
	return fs[1]
}

type stressPassKey struct{}

func passID(ctx context.Context) int64 {
	if p, ok := ctx.Value(stressPassKey{}).(*stPass); ok {
		return p.id
	}
	return 0
}

// follow records c's passes, whose driver is already set, as those of a
// controller of holder kind kind in process proc.
func (r *stressRecorder) follow(proc int, kind string, c *controller) {
	c.beginPass = func(ctx context.Context, nn types.NamespacedName) context.Context {
		p := &stPass{id: r.passSeq.Add(1), proc: proc, holder: stObj{kind, nn.Namespace, nn.Name}, began: time.Now()}
		r.mu.Lock()
		r.passes = append(r.passes, p)
		r.mu.Unlock()
		return context.WithValue(ctx, stressPassKey{}, p)
	}
	c.endPass = func(ctx context.Context, res driver.Result, err error) {
		p := ctx.Value(stressPassKey{}).(*stPass)
		r.mu.Lock()
		defer r.mu.Unlock()
		p.done, p.cut = time.Now(), ctx.Err() != nil
		p.unheld = map[stObj]string{}
		a := res.Acquired
		for _, ref := range a.Held {
			p.held = append(p.held, stObj{ref.GroupKind.Kind, ref.Namespace, ref.Name})
		}
		for _, u := range a.Unheld {
			p.unheld[stObj{u.Ref.GroupKind.Kind, u.Ref.Namespace, u.Ref.Name}] = string(u.Reason)
		}
		p.mustNot, p.degraded = a.MustNotConnect, a.Degraded
		p.conflict = res.Requeue && !res.Connected && !a.MustNotConnect
		if a.Err != nil {
			p.acqErr = a.Err.Error()
		}
		if err != nil {
			p.err = err.Error()
			if p.acqErr == "" && !res.Connected && !strings.HasPrefix(p.err, "connect: ") {
				p.acqErr = p.err
			}
		}
	}
	c.readFailed = func(ctx context.Context, holder types.NamespacedName, ref cu.Ref, start time.Time, err error) {
		if ctx.Err() != nil {
			return // the controller is stopping: the read was cut, and nothing is learnt from it
		}
		rd := stRead{pass: passID(ctx), holder: stObj{kind, holder.Namespace, holder.Name}, obj: stObj{ref.GroupKind.Kind, ref.Namespace, ref.Name},
			start: start, end: time.Now(), err: err.Error(), notFound: apierrors.IsNotFound(err)}
		r.mu.Lock()
		defer r.mu.Unlock()
		r.reads = append(r.reads, rd)
	}
	c.d.OnRead = func(ctx context.Context, obj *unstructured.Unstructured) {
		p := ctx.Value(stressPassKey{}).(*stPass)
		r.mu.Lock()
		defer r.mu.Unlock()
		p.read = true
		p.rv, p.uid, p.deleting = parseRV(obj.GetResourceVersion()), string(obj.GetUID()), obj.GetDeletionTimestamp() != nil
		p.refs = refsOf(r.mode, obj.GetNamespace(), specOf(obj))
	}
}

// listen records what process proc's informers deliver to its handlers, just
// before they act on it. Call it before Track.
func (r *stressRecorder) listen(proc int, s *cu.Shared) {
	event := func(kind, what, uid, rv string) {
		e := stEvent{stFeed: stFeed{proc, kind, what}, uid: uid, rv: parseRV(rv), at: time.Now()}
		r.mu.Lock()
		defer r.mu.Unlock()
		r.events = append(r.events, e)
	}
	s.OnHolderEvent(func(t *cu.Tracker, m *metav1.PartialObjectMetadata) {
		event(t.HolderKind().Kind, "holder", string(m.UID), m.ResourceVersion)
	})
	s.OnProtectedEvent(func(t *cu.Tracker, m *metav1.PartialObjectMetadata) {
		event(t.HolderKind().Kind, "protected", string(m.UID), m.ResourceVersion)
	})
	s.OnNamespaceAct(func(n *metav1.PartialObjectMetadata) { event("", "namespace", n.Name, n.ResourceVersion) })
}

// calledBy is md, recording process proc's calls.
func (r *stressRecorder) calledBy(proc int, md metadata.Interface) metadata.Interface {
	return recordingMetadata{md, r, proc}
}

type recordingMetadata struct {
	metadata.Interface
	r    *stressRecorder
	proc int
}

func (m recordingMetadata) Resource(gvr schema.GroupVersionResource) metadata.Getter {
	g := m.Interface.Resource(gvr)
	kind := kindOfResource(gvr)
	if kind == "" {
		return g
	}
	return recordingGetter{recordingResource{g, m.r, m.proc, stObj{kind: kind}}, g}
}

type recordingGetter struct {
	recordingResource
	g metadata.Getter
}

func (g recordingGetter) Namespace(ns string) metadata.ResourceInterface {
	c := g.recordingResource
	c.ResourceInterface, c.obj.ns = g.g.Namespace(ns), ns
	return c
}

type recordingResource struct {
	metadata.ResourceInterface
	r    *stressRecorder
	proc int
	obj  stObj
}

// call records f, a call of op on the object name.
func (c recordingResource) call(ctx context.Context, sc stCall, name string, f func() (*metav1.PartialObjectMetadata, error)) (*metav1.PartialObjectMetadata, error) {
	sc.proc, sc.g, sc.pass, sc.obj = c.proc, goroutine(), passID(ctx), c.obj
	sc.obj.name = name
	sc.start = time.Now()
	out, err := f()
	sc.end = time.Now()
	if err != nil {
		sc.err, sc.notFound, sc.unknown, sc.code = err.Error(), apierrors.IsNotFound(err), !answered(err), codeOf(err)
		sc.ours = endedByUs(ctx, err)
		sc.refused = sc.op == "a2" && sc.code == http.StatusUnprocessableEntity && strings.Contains(sc.err, "no new finalizers can be added if the object is being deleted")
	} else {
		sc.rv, sc.uid, sc.deleting, sc.fins = parseRV(out.ResourceVersion), string(out.UID), out.DeletionTimestamp != nil, slices.Clone(out.Finalizers)
		sc.epochs, sc.consent = epochsOf(out.Finalizers, out.Annotations), out.Annotations[cu.AnnotationAllowHoldersFrom]
	}
	c.r.add(sc)
	return out, err
}

func (r *stressRecorder) add(c stCall) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, c)
}

// endedByUs reports whether err, a call's with ctx, isn't the API server's
// answer, and comes of the call's own context: it has ended, or, by the
// client's rate limiter, would have. A 504 whose text speaks of a deadline has
// a code, and is the API server's.
func endedByUs(ctx context.Context, err error) bool {
	if codeOf(err) != 0 {
		return false
	}
	return ctx.Err() != nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || strings.Contains(err.Error(), "would exceed context deadline")
}

// answered reports whether err is the API server's refusal: the call did
// nothing. A 5xx, a timeout, or no status at all, may have landed.
func answered(err error) bool {
	code := codeOf(err)
	return code >= 400 && code < http.StatusInternalServerError
}

// codeOf is the HTTP code err carries, or 0.
func codeOf(err error) int {
	var st apierrors.APIStatus
	if !errors.As(err, &st) {
		return 0
	}
	return int(st.Status().Code)
}

// Get records a live GET as such: one with a resourceVersion may be answered
// from a cache, and is no R1, and no read of a Namespace or of a consent.
func (c recordingResource) Get(ctx context.Context, name string, o metav1.GetOptions, sub ...string) (*metav1.PartialObjectMetadata, error) {
	op := "get"
	if o.ResourceVersion != "" {
		op = "get, not live"
	}
	return c.call(ctx, stCall{op: op}, name, func() (*metav1.PartialObjectMetadata, error) {
		return c.ResourceInterface.Get(ctx, name, o, sub...)
	})
}

func (c recordingResource) Patch(ctx context.Context, name string, pt types.PatchType, data []byte, o metav1.PatchOptions, sub ...string) (*metav1.PartialObjectMetadata, error) {
	return c.call(ctx, patchOf(pt, data), name, func() (*metav1.PartialObjectMetadata, error) {
		return c.ResourceInterface.Patch(ctx, name, pt, data, o, sub...)
	})
}

// List records a list's revision: that of the state it answers, in etcd's
// order, when it is a consistent read, as R2's is; and, of a namespace's
// holders, its items.
func (c recordingResource) List(ctx context.Context, o metav1.ListOptions) (*metav1.PartialObjectMetadataList, error) {
	if f, ok := c.r.beforeList.Load(c.obj.ns); ok && c.obj.kind == "Widget" {
		f.(func())()
	}
	sc := stCall{op: "list", proc: c.proc, g: goroutine(), pass: passID(ctx), obj: c.obj, start: time.Now(), continued: o.Continue != ""}
	out, err := c.ResourceInterface.List(ctx, o)
	sc.end = time.Now()
	if err != nil {
		sc.err, sc.unknown, sc.code, sc.ours = err.Error(), !answered(err), codeOf(err), endedByUs(ctx, err)
	} else {
		sc.rv, sc.more = parseRV(out.ResourceVersion), out.Continue != ""
		if c.obj.ns != "" && (c.obj.kind == "Widget" || c.obj.kind == "Target") {
			for i := range out.Items {
				m := &out.Items[i]
				it := stItem{name: m.Name, uid: string(m.UID), rv: parseRV(m.ResourceVersion), deleting: m.DeletionTimestamp != nil, owner: slices.Contains(m.Finalizers, ownerFinalizer)}
				it.rec, it.recBad = parseRecord(m.Annotations)
				sc.items = append(sc.items, it)
			}
		}
	}
	if o.ResourceVersion != "" || o.LabelSelector != "" || o.FieldSelector != "" {
		sc.op = "list, not consistent or not whole"
	}
	c.r.add(sc)
	return out, err
}

// Delete and DeleteCollection aren't the package's to make: nothing here would
// account for what they do.
func (c recordingResource) Delete(ctx context.Context, name string, o metav1.DeleteOptions, sub ...string) error {
	c.r.gap(fmt.Sprintf("p%d deletes %s/%s of %s", c.proc, c.obj.ns, name, c.obj.kind))
	return c.ResourceInterface.Delete(ctx, name, o, sub...)
}

func (c recordingResource) DeleteCollection(ctx context.Context, o metav1.DeleteOptions, l metav1.ListOptions) error {
	c.r.gap(fmt.Sprintf("p%d deletes the %s of %s", c.proc, c.obj.kind, c.obj.ns))
	return c.ResourceInterface.DeleteCollection(ctx, o, l)
}

// patchOf reads what a patch is, by the paths it writes: an A1, which writes
// a holder's record; an A2, which writes a finalizer and its epoch; or an R3,
// which removes them.
func patchOf(pt types.PatchType, data []byte) stCall {
	c := stCall{op: string(pt)}
	if pt == types.JSONPatchType {
		var ops []struct {
			Op, Path string
			Value    any
		}
		if err := json.Unmarshal(data, &ops); err != nil {
			return c
		}
		const annotations, finalizers = "/metadata/annotations/", "/metadata/finalizers/"
		for _, o := range ops {
			value, _ := o.Value.(string)
			key := strings.NewReplacer("~1", "/", "~0", "~").Replace(strings.TrimPrefix(o.Path, annotations))
			switch {
			case o.Op == "test" && o.Path == "/metadata/uid":
				c.testedUID = value
			case o.Op == "test" && strings.HasPrefix(o.Path, annotations):
				c.tested = value
			case o.Op == "remove" && strings.HasPrefix(o.Path, annotations):
				c.fin = key
			case o.Op == "remove" && strings.HasPrefix(o.Path+"/", finalizers):
				c.op = "r3" // one entry of the list, or all of it
			}
		}
		return c
	}
	var body struct {
		Metadata struct {
			Finalizers  []string
			Annotations map[string]*string
		}
	}
	if err := json.Unmarshal(data, &body); err != nil {
		return c
	}
	if body.Metadata.Finalizers == nil {
		if len(body.Metadata.Annotations) > 0 {
			c.op = "a1"
		}
		return c
	}
	c.op = "a2"
	for k, v := range body.Metadata.Annotations {
		if _, _, ok := holderOfFinalizer(k); ok && v != nil {
			c.fin, c.wrote = k, *v
		}
	}
	return c
}

// deleteObject makes one of the test's DELETEs, through a client of the
// test's, and records what the API server answered: the typed clients drop
// it. Every DELETE of a Secret or a Target in the stress is made here. An
// answer that is lost names no incarnation, so it exempts nothing: its
// outcome is unknown, which fails the run; and one that client-go retried
// by itself is answered NotFound, which leaves the removal unexplained.
func (r *stressRecorder) deleteObject(ctx context.Context, o stObj) error {
	gvr := recordedKinds[o.kind]
	path := "/api/" + gvr.Version
	if gvr.Group != "" {
		path = "/apis/" + gvr.Group + "/" + gvr.Version
	}
	path += "/namespaces/" + o.ns + "/" + gvr.Resource + "/" + o.name
	w := stWrite{op: "DELETE", obj: o, start: time.Now()}
	data, err := env.Kube.CoreV1().RESTClient().Delete().AbsPath(path).SetHeader("Accept", "application/json").Do(ctx).Raw()
	w.end = time.Now()
	switch {
	case apierrors.IsNotFound(err):
		w.answer, w.err = answerNotFound, err.Error()
	case err != nil && answered(err):
		w.answer, w.err = answerRefused, err.Error()
	case err != nil:
		w.answer, w.err = answerUnknown, err.Error()
	default:
		w.answer, w.uid, w.rv, w.err = deleteAnswer(data)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.writes = append(r.writes, w)
	return err
}

// deleteAnswer reads a DELETE's answer: a Status whose details name the
// incarnation it removed, or the object, being deleted. Anything else is
// unknown, which fails the run.
func deleteAnswer(data []byte) (answer, uid string, rv uint64, problem string) {
	var kind struct{ Kind string }
	if err := json.Unmarshal(data, &kind); err != nil {
		return answerUnknown, "", 0, fmt.Sprintf("%v: %s", err, data)
	}
	if kind.Kind == "Status" {
		var st metav1.Status
		if err := json.Unmarshal(data, &st); err == nil && st.Status == metav1.StatusSuccess && st.Details != nil && st.Details.UID != "" {
			return answerStatus, string(st.Details.UID), 0, ""
		}
		return answerUnknown, "", 0, string(data)
	}
	var m metav1.PartialObjectMetadata
	if err := json.Unmarshal(data, &m); err != nil || m.UID == "" || m.DeletionTimestamp == nil {
		return answerUnknown, "", 0, string(data)
	}
	return answerMarked, string(m.UID), parseRV(m.ResourceVersion), ""
}

// createSecret and createTarget make the test's creates.
func (r *stressRecorder) createSecret(ctx context.Context, ns, name string) error {
	return r.create(stObj{"Secret", ns, name}, func() (metav1.Object, error) {
		return env.Kube.CoreV1().Secrets(ns).Create(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: name}}, metav1.CreateOptions{})
	})
}

func (r *stressRecorder) createTarget(ctx context.Context, ns, name, secret string) error {
	u := &unstructured.Unstructured{Object: map[string]any{"spec": map[string]any{"secret": secret}}}
	u.SetGroupVersionKind(cutest.TargetGVK)
	u.SetNamespace(ns)
	u.SetName(name)
	return r.create(stObj{"Target", ns, name}, func() (metav1.Object, error) {
		return env.Dynamic.Resource(cutest.TargetGVR).Namespace(ns).Create(ctx, u, metav1.CreateOptions{})
	})
}

func (r *stressRecorder) create(o stObj, f func() (metav1.Object, error)) error {
	w := stWrite{op: "create", obj: o, start: time.Now()}
	out, err := f()
	w.end = time.Now()
	switch {
	case err == nil:
		w.answer, w.uid, w.rv = answerCreated, string(out.GetUID()), parseRV(out.GetResourceVersion())
	case answered(err):
		w.answer, w.err = answerRefused, err.Error()
	default:
		w.answer, w.err = answerUnknown, err.Error()
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.writes = append(r.writes, w)
	return err
}
