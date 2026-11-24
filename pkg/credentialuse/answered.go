package credentialuse

import (
	"context"
	"errors"
	"fmt"
	"net/http/httptrace"
	"sync"
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

// answerRest is how long what follows an answer's first byte may take: its body, or,
// when the client makes the call again by itself, the wait before that. A var but for
// tests.
var answerRest = 5 * time.Minute

// answered makes an API call that must be answered within limit of when it is sent.
//
// A deadline on its context would do worse. The client's own rate limiter reads it:
// it refuses at once, unsent, a call whose wait would pass it, so behind a backlog
// longer than the limit nothing would ever be read. One that the limiter couldn't see
// would still cut the wait. So the time starts when the transport asks for a
// connection, which net/http does first of all, and the limiter's wait before that is
// as long as it is: it makes the call late, as it did before there was a limit.
//
// client-go makes a call again by itself after some answers, as a 429, inside the same
// one, once it has slept and waited in the limiter again. So each attempt has the
// limit, and once one is answered, its first byte read, what follows has answerRest. An
// attempt that ends with no answer and no error of the call's, as a reset that
// client-go tries again after a second, gives no sign, so what it waits then counts in
// its limit, until the next is sent.
//
// A connection that is never made counts as sent. A transport that gives no sign, as
// a RoundTripper that isn't built on net/http's, is never cut, but by ctx.
func answered[T any](ctx context.Context, limit time.Duration, call func(context.Context) (T, error)) (T, error) {
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	var mu sync.Mutex
	var timer *time.Timer
	var had time.Duration
	over := false
	within := func(d time.Duration) {
		mu.Lock()
		defer mu.Unlock()
		if over {
			return
		}
		if timer != nil {
			timer.Stop()
		}
		had, timer = d, time.AfterFunc(d, func() { cancel(errNoAnswer) })
	}
	out, err := call(httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{
		GetConn:              func(string) { within(limit) },
		GotFirstResponseByte: func() { within(answerRest) },
	}))
	mu.Lock()
	defer mu.Unlock()
	over = true
	if timer != nil {
		timer.Stop()
	}
	if err != nil && errors.Is(context.Cause(ctx), errNoAnswer) {
		err = &noAnswer{had, err}
	}
	return out, err
}
