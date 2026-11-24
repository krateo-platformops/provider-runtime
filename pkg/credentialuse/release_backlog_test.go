package credentialuse

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/metadata"
	"k8s.io/client-go/rest"
)

// The client's own rate limiter (§6.2): what it makes a release wait for is waited
// for, however long, and only an answer that doesn't come is cut.

// apiAt is a real metadata client of qps requests a second, with no burst, against an
// API server whose namespace ns has Secret s, held for ns and being deleted, and no
// holder. Any other object of it is missing, and what slow says is answered after its
// time.
func apiAt(t *testing.T, qps float32, slow func(req *http.Request) time.Duration) clientServer {
	t.Helper()
	gone := metav1.NewTime(time.Now().Add(-time.Hour))
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if slow != nil {
			select {
			case <-time.After(slow(req)):
			case <-req.Context().Done():
			}
		}
		code, out := http.StatusOK, any(nil)
		switch p := req.URL.Path; {
		case p == "/api/v1/namespaces/ns/secrets/s":
			m := heldObject(secretGVK, "ns", "s", "u", widgetGK, "ns")
			m.DeletionTimestamp = &gone
			if req.Method == http.MethodPatch {
				m.Finalizers = nil
			}
			m.TypeMeta = metav1.TypeMeta{APIVersion: "meta.k8s.io/v1", Kind: "PartialObjectMetadata"}
			out = m
		case p == "/api/v1/namespaces/ns":
			m := namespaceObject("ns")
			m.TypeMeta = metav1.TypeMeta{APIVersion: "meta.k8s.io/v1", Kind: "PartialObjectMetadata"}
			out = m
		case strings.HasSuffix(p, "/widgets"):
			out = &metav1.PartialObjectMetadataList{TypeMeta: metav1.TypeMeta{APIVersion: "meta.k8s.io/v1", Kind: "PartialObjectMetadataList"}}
		default:
			st := apierrors.NewNotFound(schema.GroupResource{Resource: "things"}, p[strings.LastIndex(p, "/")+1:]).Status()
			st.APIVersion, st.Kind = "v1", "Status"
			code, out = http.StatusNotFound, st
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		_ = json.NewEncoder(w).Encode(out)
	}))
	t.Cleanup(api.Close)
	md, err := metadata.NewForConfig(&rest.Config{Host: api.URL, QPS: qps, Burst: 1})
	if err != nil {
		t.Fatal(err)
	}
	return clientServer{md: md, mapper: newFakeRig(t).s.c.Mapper, pageTime: 400 * time.Millisecond}
}

// A standing backlog of calls in the client's limiter, longer than any read's time,
// makes the release of an object that is being deleted late, and refuses it nothing:
// R1's read, its Namespace's, R2's list, and R3.
func TestAClientsBacklogDelaysARelease(t *testing.T) {
	srv := apiAt(t, 10, nil)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	var backlog sync.WaitGroup
	for range 10 { // a second of calls, at any time
		backlog.Add(1)
		go func() {
			defer backlog.Done()
			for ctx.Err() == nil {
				_, _ = srv.Get(ctx, schema.GroupKind{Kind: "ConfigMap"}, "ns", "cm")
			}
		}()
	}
	r := newRig(t)
	r.p.srv = srv
	r.p.readLimit = func(string, ID) time.Duration { return 400 * time.Millisecond }
	time.Sleep(1200 * time.Millisecond) // the backlog stands
	s := secretID("ns", "s")
	res, err := r.p.release(ctx, "ns", []ID{s})
	if err != nil || !slices.Equal(res.Released, []ID{s}) {
		t.Errorf("released %v, failed %v, %v; want the Secret released", res.Released, res.Failed, err)
	}
	cancel()
	backlog.Wait()
}

// What a real client sent, and isn't answered, is cut after its own time.
func TestARealReadThatIsntAnswered(t *testing.T) {
	for what, path := range map[string]string{"R1's": "/secrets/s", "the Namespace's": "/namespaces/ns", "the list's": "/widgets"} {
		srv := apiAt(t, 1000, func(req *http.Request) time.Duration {
			if strings.HasSuffix(req.URL.Path, path) {
				return 5 * time.Second
			}
			return 0
		})
		srv.pageTime = 200 * time.Millisecond
		r := newRig(t)
		r.p.srv = srv
		r.p.readLimit = func(string, ID) time.Duration { return 200 * time.Millisecond }
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		s := secretID("ns", "s")
		res, err := r.p.release(ctx, "ns", []ID{s})
		if !slices.Equal(res.Failed, []ID{s}) || !errors.Is(err, context.DeadlineExceeded) || ctx.Err() != nil {
			t.Errorf("%s: failed %v, %v, the caller's context %v; want it cut alone", what, res.Failed, err, ctx.Err())
		}
		cancel()
	}
}

// A Namespace is read for as long as the longest of the reads of the objects that ask
// for it may take.
func TestANamespacesReadIsAsLongAsItsObjects(t *testing.T) {
	srv := apiAt(t, 1000, func(req *http.Request) time.Duration {
		if req.URL.Path == "/api/v1/namespaces/ns" {
			return 300 * time.Millisecond
		}
		return 0
	})
	r := newRig(t)
	r.p.srv = srv
	s := secretID("ns", "s")
	limit := 100 * time.Millisecond
	r.p.readLimit = func(string, ID) time.Duration { return limit }
	if res, err := r.p.release(context.Background(), "ns", []ID{s}); err == nil || !slices.Equal(res.Failed, []ID{s}) {
		t.Errorf("%+v, %v; want the Namespace's read cut", res, err)
	}
	limit = 10 * time.Second
	if res, err := r.p.release(context.Background(), "ns", []ID{s}); err != nil || !slices.Equal(res.Released, []ID{s}) {
		t.Errorf("%+v, %v; want its read as long as its object's, and the Secret released", res, err)
	}
}

// A page of any live list outside the protocol's has a time too, as that of an empty
// batch, which a worker makes.
func TestALiveListsPagesHaveATime(t *testing.T) {
	was := livePageTime
	livePageTime = 20 * time.Millisecond
	defer func() { livePageTime = was }()
	r := newFakeRig(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	m := &pagedMetadata{Interface: r.md, pages: 3, hangs: 2}
	_, err := listLive(ctx, Clients{Metadata: m, Mapper: r.s.c.Mapper}, secretGroupKind, "ns", heldSelector())
	if !errors.Is(err, context.DeadlineExceeded) || ctx.Err() != nil {
		t.Errorf("a page that doesn't answer: %v, with the caller's context %v; want the page's time alone", err, ctx.Err())
	}
	m = &pagedMetadata{Interface: r.md, pages: 3}
	livePageTime = time.Minute
	if items, err := listLive(ctx, Clients{Metadata: m, Mapper: r.s.c.Mapper}, secretGroupKind, "ns", heldSelector()); err != nil || len(items) != 3 {
		t.Errorf("%d items, %v; want the three pages'", len(items), err)
	}
}
