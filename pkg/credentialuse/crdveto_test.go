package credentialuse

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/client-go/metadata"
	"k8s.io/client-go/rest"
)

// The discovery's veto: the CRD that is gone must still be the kind's (§6.4).

// fakeDiscovery is the API server's discovery, in its unaggregated form, of
// the test group, whose resources a test changes.
type fakeDiscovery struct {
	mu sync.Mutex
	// versions are the group's, in order; each has resources, by plural. With
	// no resource anywhere the group isn't served.
	versions  []string
	resources map[string]map[string]string
	// answer, if set, answers instead when it returns true.
	answer func(path string, req *http.Request) (*http.Response, error, bool)
	asked  []string // the paths read
}

func newFakeDiscovery() *fakeDiscovery {
	return &fakeDiscovery{versions: []string{"v1"}, resources: map[string]map[string]string{"v1": {"widgets": "Widget", "gadgets": "Gadget"}}}
}

// client is a REST client of d, as a discovery client's RESTClient() is.
func (d *fakeDiscovery) client() rest.Interface {
	c, err := rest.UnversionedRESTClientForConfigAndClient(&rest.Config{
		Host: "https://localhost", QPS: -1,
		ContentConfig: rest.ContentConfig{NegotiatedSerializer: serializer.NewCodecFactory(runtime.NewScheme()).WithoutConversion()},
	}, &http.Client{Transport: roundTrip(d.do)})
	must(err)
	return c
}

// serve has the group serve plural as kind at version, or, with no kind, not
// serve it.
func (d *fakeDiscovery) serve(version, plural, kind string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.resources[version] == nil {
		d.versions = append(d.versions, version)
		d.resources[version] = map[string]string{}
	}
	if kind == "" {
		delete(d.resources[version], plural)
	} else {
		d.resources[version][plural] = kind
	}
}

func (d *fakeDiscovery) reads() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return slices.Clone(d.asked)
}

func httpAnswer(code int, contentType, body string) *http.Response {
	return &http.Response{StatusCode: code, Header: http.Header{"Content-Type": []string{contentType}}, Body: io.NopCloser(strings.NewReader(body))}
}

func (d *fakeDiscovery) do(req *http.Request) (*http.Response, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	path := req.URL.Path
	d.asked = append(d.asked, path)
	if d.answer != nil {
		if resp, err, ok := d.answer(path, req); ok {
			return resp, err
		}
	}
	notFound := httpAnswer(404, "text/plain", "404 page not found\n")
	group := metav1.APIGroup{TypeMeta: metav1.TypeMeta{Kind: "APIGroup", APIVersion: "v1"}, Name: widgetGK.Group}
	for _, v := range d.versions {
		if len(d.resources[v]) == 0 {
			continue
		}
		gv := widgetGK.Group + "/" + v
		group.Versions = append(group.Versions, metav1.GroupVersionForDiscovery{GroupVersion: gv, Version: v})
		if path == "/apis/"+gv {
			list := metav1.APIResourceList{TypeMeta: metav1.TypeMeta{Kind: "APIResourceList", APIVersion: "v1"}, GroupVersion: gv}
			for plural, kind := range d.resources[v] {
				list.APIResources = append(list.APIResources, metav1.APIResource{Name: plural, Kind: kind, Namespaced: true})
			}
			return jsonAnswer(list), nil
		}
	}
	if path != "/apis/"+widgetGK.Group || len(group.Versions) == 0 {
		return notFound, nil
	}
	return jsonAnswer(group), nil
}

func jsonAnswer(v any) *http.Response {
	b, err := json.Marshal(v)
	must(err)
	return httpAnswer(200, "application/json", string(b))
}

// What the discovery must say before a kind whose CRD is gone has no holders:
// that the group is read, every version of it, and none serves the kind.
func TestTheDiscoverysVeto(t *testing.T) {
	const group, v1, v2 = "/apis/credentialuse.test.krateo.io", "/apis/credentialuse.test.krateo.io/v1", "/apis/credentialuse.test.krateo.io/v2"
	answer := func(at string, resp func() (*http.Response, error)) func(d *fakeDiscovery) {
		return func(d *fakeDiscovery) {
			d.serve("v2", "gizmos", "Gizmo")
			d.answer = func(path string, _ *http.Request) (*http.Response, error, bool) {
				if path != at {
					return nil, nil, false
				}
				r, err := resp()
				return r, err, true
			}
		}
	}
	code := func(at string, c int) func(d *fakeDiscovery) {
		return answer(at, func() (*http.Response, error) { return httpAnswer(c, "text/plain", "test"), nil })
	}
	body := func(at, b string) func(d *fakeDiscovery) {
		return answer(at, func() (*http.Response, error) { return httpAnswer(200, "application/json", b), nil })
	}
	for name, c := range map[string]struct {
		with func(d *fakeDiscovery)
		gone bool
		want string
		asks []string
	}{
		"the group serves other kinds":               {with: func(*fakeDiscovery) {}, gone: true, asks: []string{group, v1}},
		"the group isn't served":                     {with: func(d *fakeDiscovery) { d.serve("v1", "gadgets", "") }, gone: true, asks: []string{group}},
		"two versions, of other kinds":               {with: func(d *fakeDiscovery) { d.serve("v2", "gizmos", "Gizmo") }, gone: true, asks: []string{group, v1, v2}},
		"another plural":                             {with: func(d *fakeDiscovery) { d.serve("v1", "widgetries", "Widget") }, want: "widgetries in credentialuse.test.krateo.io/v1 serves the kind now", asks: []string{group, v1}},
		"another plural, at a later version":         {with: func(d *fakeDiscovery) { d.serve("v2", "widgetries", "Widget") }, want: "restart the process", asks: []string{group, v1, v2}},
		"the same finalizer, by its case":            {with: func(d *fakeDiscovery) { d.serve("v1", "wIDGETs", "WIDGET") }, want: "restart the process"},
		"a subresource of the kind":                  {with: func(d *fakeDiscovery) { d.serve("v1", "things/widget", "Widget") }, want: "restart the process"},
		"its own resource, by a server that lags":    {with: func(d *fakeDiscovery) { d.serve("v1", "widgets", "Widget") }, want: "discovery still shows widgets in credentialuse.test.krateo.io/v1"},
		"its own subresource, by a server that lags": {with: func(d *fakeDiscovery) { d.serve("v1", "widgets/status", "Widget") }, want: "discovery still shows widgets/status in"},
		"the kind in another group": {gone: true, with: body(v1, `{"kind":"APIResourceList","groupVersion":"credentialuse.test.krateo.io/v1",`+
			`"resources":[{"name":"gadgets/scale","group":"autoscaling","version":"v1","kind":"Widget"}]}`)},
		"the kind in its group, named": {want: "restart the process", with: body(v1, `{"kind":"APIResourceList","groupVersion":"credentialuse.test.krateo.io/v1",`+
			`"resources":[{"name":"widgetries","group":"credentialuse.test.krateo.io","kind":"Widget"}]}`)},
		"the group: 403": {with: code(group, 403), want: "grant get on the URLs /apis/*, as system:discovery does"},
		"a version: 403": {with: code(v1, 403), want: "grant get on the URLs /apis/*, as system:discovery does"},
		"the group: 500": {with: code(group, 500), want: "whether another resource serves it"},
		"a version: 404": {with: code(v1, 404), want: "whether another resource serves it"},
		"a later version: 503, its server being down": {with: code(v2, 503), want: "whether another resource serves it"},
		"the group: unreachable":                      {with: answer(group, func() (*http.Response, error) { return nil, errors.New("connection refused") }), want: "connection refused"},
		"the group: not JSON":                         {with: body(group, "<html>"), want: "whether another resource serves it"},
		"the group: something else":                   {with: body(group, `{"kind":"Status","status":"Success"}`), want: "isn't the group's discovery"},
		"the group: another one":                      {with: body(group, `{"kind":"APIGroup","name":"x.io","versions":[{"groupVersion":"x.io/v1","version":"v1"}]}`), want: "isn't the group's discovery"},
		"the group: with no versions":                 {with: body(group, `{"kind":"APIGroup","name":"credentialuse.test.krateo.io"}`), want: "isn't the group's discovery"},
		"a version: something else":                   {with: body(v1, `{}`), want: "isn't the version's discovery"},
		"a version: another one":                      {with: body(v1, `{"kind":"APIResourceList","groupVersion":"x.io/v1"}`), want: "isn't the version's discovery"},
	} {
		t.Run(name, func(t *testing.T) {
			r := newFakeRig(t)
			tr := r.track(t, widgetGK)
			r.removeCRD(t, widgetGK)
			c.with(r.disc)
			err := tr.noHolders(context.Background(), errTest404)
			if (err == nil) != c.gone || err != nil && !strings.Contains(err.Error(), c.want) {
				t.Errorf("noHolders = %v, want gone %v %q", err, c.gone, c.want)
			}
			if err != nil && !errors.Is(err, errTest404) {
				t.Errorf("noHolders = %v: it lost the list's error", err)
			}
			if err != nil && strings.HasPrefix(c.want, "discovery still") && strings.Contains(err.Error(), "restart") {
				t.Errorf("noHolders = %v: a restarted process couldn't track the kind", err)
			}
			if err != nil && !strings.Contains(c.want, "grant") && strings.Contains(err.Error(), "grant") {
				t.Errorf("noHolders = %v: no grant is missing", err)
			}
			if c.asks != nil && !slices.Equal(r.disc.reads(), c.asks) {
				t.Errorf("read %v, want %v", r.disc.reads(), c.asks)
			}
		})
	}

	// It is read after the GET of the CRD, and only when that says it is gone.
	t.Run("after the GET", func(t *testing.T) {
		r := newFakeRig(t)
		tr := r.track(t, widgetGK)
		r.removeCRD(t, widgetGK)
		r.failCRDGet(func() error {
			if n := len(r.disc.reads()); n != 0 {
				t.Errorf("%d discovery reads before the GET", n)
			}
			return nil
		})
		if err := tr.noHolders(context.Background(), errTest404); err != nil || len(r.disc.reads()) == 0 {
			t.Errorf("noHolders = %v, after %d discovery reads", err, len(r.disc.reads()))
		}
	})
	t.Run("not for a CRD that is there, or can't be read", func(t *testing.T) {
		for _, err := range []error{nil, errTest403, errTest404} {
			r := newFakeRig(t)
			tr := r.track(t, widgetGK)
			r.failCRDGet(func() error { return err })
			if got := tr.noHolders(context.Background(), errTest404); got == nil || len(r.disc.reads()) != 0 {
				t.Errorf("the GET = %v: noHolders = %v, after %d discovery reads", err, got, len(r.disc.reads()))
			}
		}
	})
	t.Run("without a client", func(t *testing.T) {
		r := newFakeRig(t)
		tr := r.track(t, widgetGK)
		r.removeCRD(t, widgetGK)
		r.s.c.Discovery = nil
		if err := tr.noHolders(context.Background(), errTest404); err == nil || !strings.Contains(err.Error(), "Clients.Discovery isn't set") {
			t.Errorf("noHolders = %v, want the hold kept, and why", err)
		}
	})
	// A server that hangs doesn't hold a release worker for long.
	t.Run("bounded", func(t *testing.T) {
		r := newFakeRig(t)
		tr := r.track(t, widgetGK)
		r.removeCRD(t, widgetGK)
		r.disc.answer = func(_ string, req *http.Request) (*http.Response, error, bool) {
			if dl, ok := req.Context().Deadline(); !ok || time.Until(dl) > discoveryTimeout {
				t.Errorf("the read's deadline: %v, %v", dl, ok)
			}
			return nil, nil, false
		}
		if err := tr.noHolders(context.Background(), errTest404); err != nil {
			t.Error(err)
		}
	})
}

// A Track without Clients.Discovery says what that costs, once, and one with it
// nothing.
func TestTrackWarnsWithoutDiscovery(t *testing.T) {
	for _, set := range []bool{true, false} {
		r := newFakeRig(t)
		if !set {
			r.s.c.Discovery = nil
		}
		sink := &sinkLines{}
		opts := r.options(widgetGK)
		opts.Logger = sink
		r.run(t)
		r.denied.Store("watch widgets", true)
		if _, err := r.s.Track(context.Background(), opts); err == nil {
			t.Fatal("Track succeeded without its grant")
		}
		r.denied.Delete("watch widgets")
		if n := sink.warned("Clients.Discovery isn't set"); n != 0 {
			t.Errorf("set %v: a Track that failed warned", set)
		}
		r.trackLogged(t, sink)
		r.trackLogged(t, sink) // the same Options: the same tracker
		if n := sink.warned("Clients.Discovery isn't set"); (n == 1) == set {
			t.Errorf("set %v: warned %d times", set, n)
		}
	}
}

// Two processes, without leader election (§6.8). This one, O, tracks Widgets
// as widgets. That CRD goes, and another, widgetries, brings the kind back; a
// new process, N, tracks it, and its holder holds the Secret again and again,
// with the same finalizer. O's every batch finds its list and its CRD gone;
// but the kind is served, so it strips nothing, also while the Secret and its
// namespace are being deleted. Once widgetries is gone too, O releases.
//
// Without the veto O strips each of N's holds, which N's holder then puts
// back: the control.
func TestAnotherPluralsHoldsArentStripped(t *testing.T) {
	for name, c := range map[string]struct{ deleting, veto bool }{
		"a live Secret":                      {veto: true},
		"its namespace being deleted":        {deleting: true, veto: true},
		"the control, a discovery that lags": {},
	} {
		t.Run(name, func(t *testing.T) {
			ns := namespaceObject("ns")
			s := heldObject(secretGVK, "ns", "s", "u1", widgetGK, "ns")
			s.Finalizers = append(s.Finalizers, "example.com/other") // the fake API server removes nothing by itself
			if c.deleting {
				deleting(ns, time.Now().Add(-time.Hour))
				deleting(s, time.Now().Add(-time.Hour))
			}
			r := newFakeRig(t, ns, s)
			o, sink := r.breakable("widgets"), &sinkLines{}
			tr := r.trackLogged(t, sink)
			r.run(t)
			o.fail(errTest404)
			r.removeCRD(t, widgetGK)
			if c.veto {
				r.disc.serve("v1", "widgetries", "Widget")
			}
			eventually(t, "O's informer to see its CRD go", tr.crdAbsent)
			r.runWrites(t)
			stripped := 0
			for turn := range 4 {
				// N's A2.
				m := r.secretMeta(t, "ns", "s").DeepCopy()
				if !hasFinalizer(m, fWidgetNS) {
					m.Finalizers = append(m.Finalizers, fWidgetNS)
				}
				m.Annotations = map[string]string{fWidgetNS: fmt.Sprintf("n%d", turn)}
				m.Labels = map[string]string{LabelHeld: "true", MarkerLabel(widgetGK): "true"}
				m.ResourceVersion = fmt.Sprint(100 + turn)
				must(r.md.Tracker().Update(secretsGVR, m, "ns"))
				failed := sink.warned("a release batch failed")
				eventually(t, "O's batch for N's A2", func() bool {
					return sink.warned("a release batch failed") > failed || !r.heldBy(t, widgetGK, "ns", "s")
				})
				if !r.heldBy(t, widgetGK, "ns", "s") {
					stripped++
				}
			}
			if c.veto && stripped != 0 || !c.veto && stripped == 0 {
				t.Fatalf("O stripped %d of N's holds", stripped)
			}
			if !c.veto {
				return
			}
			if sink.warned("widgetries in "+widgetGK.Group+"/v1 serves the kind now") == 0 {
				t.Errorf("O doesn't say why: %q", sink.warns)
			}
			r.disc.serve("v1", "widgetries", "")
			releasedStepping(t, r, "the hold to be released once the kind is gone", func() bool { return !r.heldBy(t, widgetGK, "ns", "s") })
		})
	}
}

// The evidence is a consistent read: the GET of the CRD carries no
// resourceVersion, with which the API server would answer from its watch
// cache, which may lag (§6.4).
func TestTheCRDsGetIsAConsistentRead(t *testing.T) {
	r := newFakeRig(t)
	tr := r.track(t, widgetGK)
	r.removeCRD(t, widgetGK)
	var gets []string
	md, err := metadata.NewForConfigAndClient(&rest.Config{Host: "https://localhost"}, &http.Client{Transport: roundTrip(func(req *http.Request) (*http.Response, error) {
		gets = append(gets, req.Method+" "+req.URL.RequestURI())
		return httpAnswer(404, "application/json", `{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"NotFound","code":404,`+
			`"details":{"name":"widgets.`+widgetGK.Group+`","group":"apiextensions.k8s.io","kind":"customresourcedefinitions"}}`), nil
	})})
	if err != nil {
		t.Fatal(err)
	}
	r.s.c.Metadata = md
	if err := tr.noHolders(context.Background(), errTest404); err != nil {
		t.Fatal(err)
	}
	if want := []string{"GET /apis/apiextensions.k8s.io/v1/customresourcedefinitions/widgets." + widgetGK.Group}; !slices.Equal(gets, want) {
		t.Errorf("requests %q, want %q: a resourceVersion makes it a read of the watch cache", gets, want)
	}
}

type roundTrip func(*http.Request) (*http.Response, error)

func (f roundTrip) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }
