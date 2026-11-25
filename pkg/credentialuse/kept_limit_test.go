package credentialuse

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/metadata"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/util/flowcontrol"
)

// The gate of recorded refs (§11.3) on a client whose rate limit is short of
// turns. client-go's limiter refuses at once a call whose turn comes after its
// context's deadline, so the calls that a pass has no turns for would fail by
// their place in it, pass after pass.
//
// They run on synctest's clock, so a slow machine changes no time here.

// apiServer answers the reads of a real client, in process: it has every
// Namespace, and the Secrets in consents, by name, with consent for "ns" or
// without any.
type apiServer struct{ consents map[string]bool }

func (s apiServer) RoundTrip(r *http.Request) (*http.Response, error) {
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/api/v1/namespaces/"), "/")
	name := parts[len(parts)-1]
	m := metav1.PartialObjectMetadata{TypeMeta: metav1.TypeMeta{APIVersion: "meta.k8s.io/v1", Kind: "PartialObjectMetadata"}}
	m.Name, m.UID, m.ResourceVersion = name, "u", "1"
	var out any = &m
	code := http.StatusOK
	if len(parts) == 3 {
		m.Namespace = parts[0]
		consent, ok := s.consents[name]
		switch {
		case !ok:
			code = http.StatusNotFound
			out = &metav1.Status{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Status"}, Status: metav1.StatusFailure, Reason: metav1.StatusReasonNotFound,
				Code: http.StatusNotFound, Details: &metav1.StatusDetails{Kind: "secrets", Name: name}}
		case consent:
			m.Annotations = map[string]string{AnnotationAllowHoldersFrom: "ns"}
		}
	}
	body, err := json.Marshal(out)
	if err != nil {
		return nil, err
	}
	return &http.Response{Status: http.StatusText(code), StatusCode: code, Proto: "HTTP/1.1", ProtoMajor: 1, ProtoMinor: 1, Request: r,
		Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(bytes.NewReader(body)), ContentLength: int64(len(body))}, nil
}

// limited is a server that reads Secrets and Namespaces with a real client, of
// the given rate limit, from an apiServer. The rest is the server's it wraps.
type limited struct {
	server
	md metadata.Interface
	// fails, if set, answers a Secret's read instead of the API server.
	fails func(ctx context.Context, name string) error

	mu   sync.Mutex
	read map[string]int // how often each Secret's read was answered, by ns/name
}

func (s *limited) Get(ctx context.Context, gk schema.GroupKind, namespace, name string) (*metav1.PartialObjectMetadata, error) {
	switch gk {
	case namespaceGroupKind:
		return s.md.Resource(schema.GroupVersionResource{Version: "v1", Resource: "namespaces"}).Get(ctx, name, metav1.GetOptions{})
	case secretGroupKind:
		if s.fails != nil {
			if err := s.fails(ctx, name); err != nil {
				return nil, err
			}
		}
		out, err := s.md.Resource(secretsGVR).Namespace(namespace).Get(ctx, name, metav1.GetOptions{})
		if c := Classify(err); c == ClassNone || c == ClassNotFound {
			s.mu.Lock()
			s.read[namespace+"/"+name]++
			s.mu.Unlock()
		}
		return out, err
	}
	return s.server.Get(ctx, gk, namespace, name)
}

// answered is how many Secrets' reads were answered, and how often that of the
// one that was the most.
func (s *limited) answered() (n, most int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, times := range s.read {
		most = max(most, times)
	}
	return len(s.read), most
}

// busy is a limiter of 200 a second, for which others keep n calls waiting
// until the test ends: so each call waits about n times 5ms.
func busy(t *testing.T, n int) flowcontrol.RateLimiter {
	t.Helper()
	lim := flowcontrol.NewTokenBucketRateLimiter(200, 1)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	for range n {
		go func() {
			for lim.Wait(ctx) == nil {
			}
		}()
	}
	return lim
}

// throttled is keptRig with the gate's own deadline, on a client of lim, and a
// record of ids.
func throttled(t *testing.T, lim flowcontrol.RateLimiter, consents map[string]bool, ids []ID) (*rig, *limited) {
	t.Helper()
	r, fc, _ := keptRig(t, true)
	r.target(fc, "kt", "Secret", "home")
	r.p.gateTurns = newGateTurns()
	r.p.gateContext = gateDeadline
	md, err := metadata.NewForConfig(&rest.Config{Host: "http://api.test", RateLimiter: lim, Transport: apiServer{consents}})
	if err != nil {
		t.Fatal(err)
	}
	l := &limited{server: r.srv, md: md, read: map[string]int{}}
	r.p.srv = l
	forgeRecord(t, r, ids)
	return r, l
}

// A client's limiter refuses at once a call whose turn comes after its
// context's deadline. With the gate's, it waits for its turn, and the deadline
// cuts the wait: whatever the pass has left.
func TestTheGatesDeadlineLetsALimiterWait(t *testing.T) {
	// The next turn is d away.
	taken := func(d time.Duration) flowcontrol.RateLimiter {
		lim := flowcontrol.NewTokenBucketRateLimiter(float32(1/d.Seconds()), 1)
		lim.Accept()
		return lim
	}
	near := func(d, want time.Duration) bool { return (d - want).Abs() < time.Millisecond } // the limiter counts in floats
	t.Run("premise: refused, with a deadline before it", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
			defer cancel()
			start := time.Now()
			if err := taken(time.Second).Wait(ctx); err == nil || ctx.Err() != nil || time.Since(start) != 0 {
				t.Errorf("Wait = %v after %v, with the context %v; want it refused at once", err, time.Since(start), ctx.Err())
			}
		})
	})
	for name, tc := range map[string]struct{ left, turn, cut time.Duration }{
		"it waits, and is cut, by a pass whose deadline is before its turn": {400 * time.Millisecond, time.Second, 200 * time.Millisecond},
		"it waits, and is cut, by a pass whose deadline is after its turn":  {1200 * time.Millisecond, time.Second, 600 * time.Millisecond},
		"it waits, and is cut, by a pass without a deadline":                {0, 10 * time.Second, gateReadTimeout},
		"it waits, and is cut, by a pass with time to spare":                {time.Minute, 10 * time.Second, gateReadTimeout},
	} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				pass, end := context.Background(), context.CancelFunc(func() {})
				if tc.left > 0 {
					pass, end = context.WithTimeout(pass, tc.left)
				}
				defer end()
				reads, cancel := gateDeadline(pass)
				defer cancel()
				lim, start := taken(tc.turn), time.Now()
				err := lim.Wait(reads)
				if took := time.Since(start); err == nil || !errors.Is(reads.Err(), context.DeadlineExceeded) || pass.Err() != nil || took != tc.cut {
					t.Errorf("Wait = %v after %v, with the reads' context %v and the pass's %v; want it cut after %v", err, took, reads.Err(), pass.Err(), tc.cut)
				}
			})
		})
	}
	t.Run("it waits, and gets its turn", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			reads, cancel := gateDeadline(context.Background())
			defer cancel()
			lim, start := taken(time.Second), time.Now()
			if err := lim.Wait(reads); err != nil || !near(time.Since(start), time.Second) {
				t.Errorf("Wait = %v after %v, want its turn after a second", err, time.Since(start))
			}
		})
	})
	for name, tc := range map[string]struct {
		behind bool // somebody waits behind it when it is cut
		want   time.Duration
	}{
		"a wait that is cut gives its turn back":                     {false, time.Second},
		"and costs the client one turn, if somebody waits behind it": {true, 2 * time.Second},
	} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				lim, start := taken(time.Second), time.Now()
				got := make(chan time.Duration, 1)
				next := func() {
					if err := lim.Wait(context.Background()); err != nil {
						t.Error(err)
					}
					got <- time.Since(start)
				}
				if tc.behind {
					go func() {
						time.Sleep(100 * time.Millisecond)
						next()
					}()
				}
				pass, end := context.WithTimeout(context.Background(), 400*time.Millisecond)
				defer end()
				reads, cancel := gateDeadline(pass)
				defer cancel()
				if err := lim.Wait(reads); err == nil {
					t.Fatal("it got a turn")
				}
				if !tc.behind {
					next()
				}
				if d := <-got; !near(d, tc.want) {
					t.Errorf("the next turn came after %v, want %v", d, tc.want)
				}
			})
		})
	}
	t.Run("no goroutine is left", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) { // which waits for them all
			lim := taken(time.Second)
			for range 50 {
				pass, end := context.WithTimeout(context.Background(), 2*time.Millisecond)
				reads, cancel := gateDeadline(pass)
				child, endChild := context.WithCancel(reads) // as a request makes
				_ = lim.Wait(child)
				endChild()
				cancel()
				end()
			}
		})
	})
}

// Every ref the gate asks about is read, whatever its place, on a client short
// of turns: a pass reads what it has the turns for, and the next goes on from
// there.
func TestTheGateOnAClientShortOfTurns(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const m = maxGateReads
		ids := make([]ID, m) // each in a Namespace of its own, so two turns each
		for i := range ids {
			ids[i] = SecretRef(fmt.Sprintf("n%02d", i), "a").ID()
		}
		r, l := throttled(t, busy(t, 10), nil, ids)
		for pass := 1; ; pass++ {
			before, _ := l.answered()
			c, end := context.WithTimeout(context.Background(), time.Second)
			if _, err := r.p.acquire(c, r.get(t, widgetKey("ns", "h")), failingNext()); err != nil {
				t.Fatal(err)
			}
			end()
			n, most := l.answered()
			t.Logf("pass %d read %d more, %d of %d", pass, n-before, n, m)
			if n == m {
				if pass == 1 {
					t.Fatal("one pass read them all: the case shows nothing")
				}
				return
			}
			if most > 1 {
				t.Fatalf("pass %d read a ref again, with %d of %d read", pass, n, m)
			}
			if n == before {
				t.Fatalf("pass %d read none, with %d of %d read", pass, n, m)
			}
		}
	})
}

// A deletion pass whose gate runs out of turns still holds the ref it names,
// with an A2 after a read under its own context (§11.3 rule 1).
func TestADeletionPassWhoseGateIsShortOfTurns(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ids := []ID{otherSecretID()}
		for i := range maxGateReads - 1 {
			ids = append(ids, SecretRef(fmt.Sprintf("n%02d", i), "a").ID())
		}
		r, l := throttled(t, busy(t, 10), map[string]bool{"s": true}, ids)
		must(r.m.Delete(widgetKey("ns", "h")))
		r.srv.calls = nil
		pass, end := context.WithTimeout(context.Background(), 2*time.Second)
		defer end()
		a, err := r.p.acquire(pass, r.get(t, widgetKey("ns", "h")), []Ref{otherSecret()})
		if err != nil {
			t.Fatal(err)
		}
		if n, _ := l.answered(); n >= maxGateReads {
			t.Fatalf("the gate read all %d refs: the case shows nothing", n)
		}
		if n := countCalls(r.srv.calls, "patch Secret/s"); n != 1 || len(a.Held) != 1 {
			t.Errorf("%d A2s of the ref it names, held %v (%v); want one", n, a.Held, a.Err)
		}
	})
}

// Reads that hang, reads that always fail, and a client short of turns, all at
// once: a withdrawn ref behind them is dropped, within as many passes as there
// are refs.
func TestTheGateWithHungAndFailingReads(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var ids []ID
		for _, name := range []string{"a0", "a1", "a2", "a3", "a4", "a5", "a6", "a7", "bad0", "bad1", "bad2", "hung", "s"} {
			ids = append(ids, SecretRef("other", name).ID())
		}
		r, l := throttled(t, busy(t, 4), map[string]bool{"s": false}, ids)
		l.fails = func(ctx context.Context, name string) error {
			switch {
			case name == "hung":
				<-ctx.Done()
				return ctx.Err()
			case strings.HasPrefix(name, "bad"):
				return errors.New("no answer")
			}
			return nil
		}
		passes := 0
		for keptRecorded(t, r) && passes < len(ids) {
			passes++
			pass, end := context.WithTimeout(context.Background(), 400*time.Millisecond)
			if _, err := r.p.acquire(pass, r.get(t, widgetKey("ns", "h")), failingNext()); err != nil {
				t.Fatal(err)
			}
			end()
		}
		t.Logf("%d passes", passes)
		if keptRecorded(t, r) {
			t.Errorf("the withdrawn ref is still recorded after %d passes", passes)
		}
	})
}
