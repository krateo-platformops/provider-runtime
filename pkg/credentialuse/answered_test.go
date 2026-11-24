package credentialuse

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"sync/atomic"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/metadata"
	"k8s.io/client-go/rest"
)

// get is a call of client for url, under ctx: it returns the protocol's major version.
func get(ctx context.Context, client *http.Client, url string) (int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return 0, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	return resp.ProtoMajor, resp.Body.Close()
}

// What is sent and not answered is cut after its time, and what is answered isn't,
// over HTTP/1 and HTTP/2, on a new connection and on one used before; and a trace of
// the caller's goes on being told.
func TestAnsweredCutsWhatIsntAnswered(t *testing.T) {
	for _, proto := range []int{1, 2} {
		for _, reused := range []bool{false, true} {
			api := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				if req.URL.Path == "/slow" {
					select {
					case <-time.After(5 * time.Second):
					case <-req.Context().Done():
					}
				}
				w.WriteHeader(http.StatusOK)
			}))
			api.EnableHTTP2 = proto == 2
			if proto == 2 {
				api.StartTLS()
			} else {
				api.Start()
			}
			client := api.Client()
			if reused {
				got, err := answered(context.Background(), 10*time.Second, func(ctx context.Context) (int, error) { return get(ctx, client, api.URL+"/fast") })
				if err != nil || got != proto {
					t.Errorf("HTTP/%d: a call that is answered: HTTP/%d, %v", proto, got, err)
				}
			}
			var conn atomic.Pointer[httptrace.GotConnInfo]
			ctx := httptrace.WithClientTrace(context.Background(), &httptrace.ClientTrace{GotConn: func(i httptrace.GotConnInfo) { conn.Store(&i) }})
			_, err := answered(ctx, 200*time.Millisecond, func(ctx context.Context) (int, error) { return get(ctx, client, api.URL+"/slow") })
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Errorf("HTTP/%d, reused %v: %v; want it cut", proto, reused, err)
			}
			if c := conn.Load(); c == nil || c.Reused != reused {
				t.Errorf("HTTP/%d: the connection is %+v, want reused %v", proto, c, reused)
			}
			client.CloseIdleConnections()
			api.CloseClientConnections()
			api.Close()
		}
	}
}

// A connection that is never made is cut too: it is asked for, and never got.
func TestAnsweredCutsADialThatHangs(t *testing.T) {
	end := make(chan struct{})
	defer close(end)
	var got atomic.Bool
	client := &http.Client{Transport: &http.Transport{DialContext: func(context.Context, string, string) (net.Conn, error) {
		<-end
		return nil, errors.New("ended")
	}}}
	ctx := httptrace.WithClientTrace(context.Background(), &httptrace.ClientTrace{GotConn: func(httptrace.GotConnInfo) { got.Store(true) }})
	_, err := answered(ctx, 50*time.Millisecond, func(ctx context.Context) (int, error) { return get(ctx, client, "http://192.0.2.1/") })
	if !errors.Is(err, context.DeadlineExceeded) || got.Load() {
		t.Errorf("%v, a connection got %v; want it cut, with none", err, got.Load())
	}
}

// A transport that isn't built on net/http's gives no sign of what is sent, so its
// calls have no time but the caller's: as before there was one.
func TestAnsweredOfATransportWithoutSigns(t *testing.T) {
	client := &http.Client{Transport: roundTripper(func(req *http.Request) (*http.Response, error) {
		<-req.Context().Done()
		return nil, req.Context().Err()
	})}
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	began := time.Now()
	_, err := answered(ctx, 20*time.Millisecond, func(ctx context.Context) (int, error) { return get(ctx, client, "http://192.0.2.1/") })
	var cut *noAnswer
	if errors.As(err, &cut) || ctx.Err() == nil || time.Since(began) < 400*time.Millisecond {
		t.Errorf("%v after %s; want it to end with the caller's context alone", err, time.Since(began))
	}
}

type roundTripper func(*http.Request) (*http.Response, error)

func (f roundTripper) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

// Each of a client's own attempts has the time: client-go makes one again after a 429,
// inside the same call, once it has slept and waited in its rate limiter, which isn't a
// call that isn't answered.
func TestAnsweredTimesEachAttempt(t *testing.T) {
	var asked atomic.Int32
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		st := apierrors.NewNotFound(schema.GroupResource{Resource: "secrets"}, "s").Status()
		code := http.StatusNotFound
		if asked.Add(1) == 1 {
			st = apierrors.NewTooManyRequests("busy", 1).Status()
			code = http.StatusTooManyRequests
			w.Header().Set("Retry-After", "1")
		}
		st.APIVersion, st.Kind = "v1", "Status"
		w.WriteHeader(code)
		_ = json.NewEncoder(w).Encode(st)
	}))
	defer api.Close()
	md, err := metadata.NewForConfig(&rest.Config{Host: api.URL, QPS: 1000, Burst: 1000})
	if err != nil {
		t.Fatal(err)
	}
	srv := clientServer{md: md, mapper: newFakeRig(t).s.c.Mapper}
	_, err = answered(context.Background(), 400*time.Millisecond, func(ctx context.Context) (*metav1.PartialObjectMetadata, error) {
		return srv.Get(ctx, secretGroupKind, "ns", "s")
	})
	if Classify(err) != ClassNotFound || asked.Load() != 2 {
		t.Errorf("%v after %d attempts; want the second's answer, that it is missing", err, asked.Load())
	}
}

// What comes before the call is sent, as the wait in a client's rate limiter, isn't
// its time, and sees no deadline; the caller's own end is.
func TestAnsweredDoesntTimeWhatComesBefore(t *testing.T) {
	api := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer api.Close()
	_, err := answered(context.Background(), 300*time.Millisecond, func(ctx context.Context) (int, error) {
		if _, has := ctx.Deadline(); has {
			t.Error("the call's context has a deadline")
		}
		time.Sleep(600 * time.Millisecond)
		return get(ctx, api.Client(), api.URL)
	})
	if err != nil {
		t.Errorf("%v; want the call answered", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = answered(ctx, time.Minute, func(ctx context.Context) (int, error) { return get(ctx, api.Client(), api.URL) })
	var cut *noAnswer
	if !errors.Is(err, context.Canceled) || errors.As(err, &cut) {
		t.Errorf("%v; want the caller's end", err)
	}
}

// An answer that comes as its time ends is the answer: an object that is missing is
// missing.
func TestAnsweredKeepsAnAnswersClass(t *testing.T) {
	_, err := answered(context.Background(), time.Nanosecond, func(ctx context.Context) (int, error) {
		httptrace.ContextClientTrace(ctx).GetConn("")
		<-ctx.Done()
		return 0, unservedError()
	})
	if Classify(err) != Classify(unservedError()) {
		t.Errorf("%v is %s, want the answer's class %s", err, Classify(err), Classify(unservedError()))
	}
	_, err = answered(context.Background(), time.Nanosecond, func(ctx context.Context) (int, error) {
		httptrace.ContextClientTrace(ctx).GetConn("")
		<-ctx.Done()
		return 0, rbacForbidden("ns", "s")
	})
	if Classify(err) != ClassRBAC {
		t.Errorf("%v is %s, want the answer's class %s", err, Classify(err), ClassRBAC)
	}
}
