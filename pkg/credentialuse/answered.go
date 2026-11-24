package credentialuse

import (
	"context"
	"errors"
	"fmt"
	"net/http/httptrace"
	"sync"
	"sync/atomic"
	"time"
)

// errNoAnswer is why a call that was sent and not answered in its time was cut.
var errNoAnswer = errors.New("credential-use: no answer")

// A noAnswer is the error of such a call.
type noAnswer struct {
	limit time.Duration
	err   error
}

func (e *noAnswer) Error() string        { return fmt.Sprintf("%v in %s: %v", errNoAnswer, e.limit, e.err) }
func (e *noAnswer) Unwrap() error        { return e.err }
func (e *noAnswer) Is(target error) bool { return target == context.DeadlineExceeded }

// answered makes an API call that must be answered within limit of when it is sent.
//
// A deadline on its context would do worse. The client's own rate limiter reads it:
// it refuses at once, unsent, a call whose wait would pass it, so behind a backlog
// longer than the limit nothing would ever be read. And hiding the deadline from the
// limiter, as the consent gate may (its pass is retried whole, and is nobody's
// worker), would still cut the wait itself. So the time starts with the transport's
// first sign that the call is out, a connection asked for, and the limiter's wait is
// as long as it is: it makes the call late, as it did before there was a limit.
//
// A connection that is never made counts as sent. A transport that gives no sign, as
// a RoundTripper that isn't built on net/http's, is never cut, but by ctx.
func answered[T any](ctx context.Context, limit time.Duration, call func(context.Context) (T, error)) (T, error) {
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	var once sync.Once
	var timer atomic.Pointer[time.Timer]
	sent := func() {
		once.Do(func() { timer.Store(time.AfterFunc(limit, func() { cancel(errNoAnswer) })) })
	}
	out, err := call(httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{
		GetConn:      func(string) { sent() },
		GotConn:      func(httptrace.GotConnInfo) { sent() },
		WroteHeaders: sent,
	}))
	if t := timer.Load(); t != nil {
		t.Stop()
	}
	if err != nil && errors.Is(context.Cause(ctx), errNoAnswer) {
		err = &noAnswer{limit, err}
	}
	return out, err
}
