package credentialuse

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	goruntime "runtime"
	"strings"
	"sync"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/metadata"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/util/flowcontrol"
)

// The gate of recorded refs (§11.3) on a client whose rate limit is short of
// tokens. client-go's limiter refuses at once a call whose turn comes after its
// context's deadline, so the calls that a pass has no tokens for would fail by
// their place in it, pass after pass.

// limited is a server that reads Secrets and Namespaces with a real client, of
// the given rate limit, from an API server that has every Namespace, and the
// Secrets in consents, by name: with consent for "ns", or without any. The
// rest is the server's it wraps.
type limited struct {
	server
	md metadata.Interface
	// fails, if set, answers a Secret's read instead of the API server.
	fails func(ctx context.Context, name string) error

	mu   sync.Mutex
	read map[string]bool // the Secrets whose read was answered, as ns/name
}

func newLimited(t *testing.T, s server, lim flowcontrol.RateLimiter, consents map[string]bool) *limited {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/api/v1/namespaces/"), "/")
		name := parts[len(parts)-1]
		m := metav1.PartialObjectMetadata{TypeMeta: metav1.TypeMeta{APIVersion: "meta.k8s.io/v1", Kind: "PartialObjectMetadata"}}
		m.Name, m.UID, m.ResourceVersion = name, "u", "1"
		var out any = &m
		if len(parts) == 3 {
			consent, ok := consents[name]
			switch {
			case !ok:
				w.WriteHeader(http.StatusNotFound)
				out = &metav1.Status{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Status"}, Status: metav1.StatusFailure, Reason: metav1.StatusReasonNotFound,
					Code: http.StatusNotFound, Details: &metav1.StatusDetails{Kind: "secrets", Name: name}}
			case consent:
				m.Annotations = map[string]string{AnnotationAllowHoldersFrom: "ns"}
			}
			m.Namespace = parts[0]
		}
		if err := json.NewEncoder(w).Encode(out); err != nil {
			t.Error(err)
		}
	}))
	t.Cleanup(srv.Close)
	md, err := metadata.NewForConfig(&rest.Config{Host: srv.URL, RateLimiter: lim})
	if err != nil {
		t.Fatal(err)
	}
	return &limited{server: s, md: md, read: map[string]bool{}}
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
			s.read[namespace+"/"+name] = true
			s.mu.Unlock()
		}
		return out, err
	}
	return s.server.Get(ctx, gk, namespace, name)
}

func (s *limited) answered() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.read)
}

// busy is a limiter of 200 a second, for which others keep n calls waiting
// until the test ends: so each call waits n times 5ms.
func busy(t *testing.T, n int) flowcontrol.RateLimiter {
	t.Helper()
	lim := flowcontrol.NewTokenBucketRateLimiter(200, 1)
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	for range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for lim.Wait(ctx) == nil {
			}
		}()
	}
	t.Cleanup(func() {
		cancel()
		wg.Wait()
	})
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
	l := newLimited(t, r.srv, lim, consents)
	r.p.srv = l
	forgeRecord(t, r, ids)
	return r, l
}

// A client's limiter refuses at once a call whose turn comes after its
// context's deadline. With the gate's, it waits for its turn, and the deadline
// cuts the wait: whatever the pass has left.
func TestTheGatesDeadlineLetsALimiterWait(t *testing.T) {
	// The next turn is a second away.
	taken := func() flowcontrol.RateLimiter {
		lim := flowcontrol.NewTokenBucketRateLimiter(1, 1)
		lim.Accept()
		return lim
	}
	t.Run("premise: refused, with a deadline before it", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		defer cancel()
		start := time.Now()
		if err := taken().Wait(ctx); err == nil || ctx.Err() != nil || time.Since(start) > 250*time.Millisecond {
			t.Errorf("Wait = %v after %v, with the context %v; want it refused at once", err, time.Since(start), ctx.Err())
		}
	})
	t.Run("it waits, and is cut", func(t *testing.T) {
		for _, left := range []time.Duration{400 * time.Millisecond, 1200 * time.Millisecond} { // before the turn, and after it
			pass, end := context.WithTimeout(context.Background(), left)
			reads, cancel := gateDeadline(pass)
			start := time.Now()
			err := taken().Wait(reads)
			if took := time.Since(start); err == nil || reads.Err() == nil || pass.Err() != nil || took < left/2-50*time.Millisecond {
				t.Errorf("with %v left: Wait = %v after %v, with the reads' context %v and the pass's %v; want it cut at half", left, err, took, reads.Err(), pass.Err())
			}
			cancel()
			end()
		}
	})
	t.Run("it waits, and gets its turn", func(t *testing.T) {
		reads, cancel := gateDeadline(context.Background())
		defer cancel()
		if err := taken().Wait(reads); err != nil {
			t.Errorf("Wait = %v, want its turn after a second", err)
		}
	})
	t.Run("a wait that is cut gives its turn back, when nobody waits behind it", func(t *testing.T) {
		lim := taken()
		start := time.Now()
		pass, end := context.WithTimeout(context.Background(), 400*time.Millisecond)
		defer end()
		reads, cancel := gateDeadline(pass)
		defer cancel()
		if err := lim.Wait(reads); err == nil {
			t.Fatal("it got a turn")
		}
		if err := lim.Wait(context.Background()); err != nil || time.Since(start) > 1800*time.Millisecond {
			t.Errorf("the next turn came after %v (%v), want it in the one given back, after a second", time.Since(start), err)
		}
	})
	t.Run("no goroutine is left", func(t *testing.T) {
		before := goruntime.NumGoroutine()
		lim := taken()
		for range 50 {
			pass, end := context.WithTimeout(context.Background(), 2*time.Millisecond)
			reads, cancel := gateDeadline(pass)
			child, endChild := context.WithCancel(reads) // as a request makes
			_ = lim.Wait(child)
			endChild()
			cancel()
			end()
		}
		eventually(t, "the goroutines there were", func() bool { return goruntime.NumGoroutine() <= before })
	})
}

// Every ref the gate asks about is read, whatever its place, on a client short
// of tokens: a pass reads what it has the tokens for, and the next goes on from
// there.
func TestTheGateOnAClientShortOfTokens(t *testing.T) {
	const m = maxGateReads
	ids := make([]ID, m) // each in a Namespace of its own, so two GETs each
	for i := range ids {
		ids[i] = SecretRef(fmt.Sprintf("n%02d", i), "a").ID()
	}
	r, l := throttled(t, busy(t, 10), nil, ids)
	least, passes := m, 0
	for l.answered() < m && passes < m {
		passes++
		before := l.answered()
		pass, end := context.WithTimeout(context.Background(), time.Second)
		if _, err := r.p.acquire(pass, r.get(t, widgetKey("ns", "h")), failingNext()); err != nil {
			t.Fatal(err)
		}
		end()
		if n := l.answered(); n < m {
			least = min(least, n-before)
		}
	}
	t.Logf("%d of %d refs read in %d passes, of which the least read %d", l.answered(), m, passes, least)
	if l.answered() < m {
		t.Errorf("%d refs weren't read in %d passes", m-l.answered(), passes)
	} else if least > 0 && passes > (m+least-1)/least+1 {
		t.Errorf("it took %d passes of %d refs or more, want at most %d", passes, least, (m+least-1)/least+1)
	}
}

// A deletion pass whose gate runs out of tokens still holds the ref it names,
// with an A2 after a read under its own context (§11.3 rule 1).
func TestADeletionPassWhoseGateIsShortOfTokens(t *testing.T) {
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
	if n := l.answered(); n >= maxGateReads {
		t.Fatalf("the gate read all %d refs: the case shows nothing", n)
	}
	if n := countCalls(r.srv.calls, "patch Secret/s"); n != 1 || len(a.Held) != 1 {
		t.Errorf("%d A2s of the ref it names, held %v (%v); want one", n, a.Held, a.Err)
	}
}

// Reads that hang, reads that always fail, and a client short of tokens, all
// at once: a withdrawn ref behind them is dropped, within as many passes as
// there are refs.
func TestTheGateWithHungAndFailingReads(t *testing.T) {
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
	if keptRecorded(t, r) {
		t.Errorf("the withdrawn ref is still recorded after %d passes", passes)
	}
}
