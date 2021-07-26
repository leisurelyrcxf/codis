package traceablecontext

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"time"
)

func IsCanceledErr(ctx context.Context, err error) bool {
	cErr, ok := err.(canceledErr)
	if !ok {
		return false
	}
	return ctx == nil || cErr.canceledCtx == ctx
}

// genCanceledErr is the error returned by Context.Err when the context is canceled.
func genCanceledErr(canceledCtx context.Context, reason error) canceledErr {
	return canceledErr{canceledCtx: canceledCtx, reason: reason}
}

var anyCanceledErr error = canceledErr{canceledCtx: context.Background()}

type canceledErr struct {
	canceledCtx context.Context
	reason      error
}

func (e canceledErr) Error() string {
	var reasonDesc = ""
	if e.reason != nil {
		reasonDesc = fmt.Sprintf(" due to: '%v'", e.reason)
	}
	return fmt.Sprintf("context %s canceled%s", contextName(e.canceledCtx), reasonDesc)
}

func IsDeadlineExceededErr(ctx context.Context, err error) bool {
	dErr, ok := err.(deadlineExceededError)
	if !ok {
		return false
	}
	return ctx == nil || dErr.expiredCtx == ctx
}

// genDeadlineExceededErr is the error returned by Context.Err when the context's
// deadline passes.
func genDeadlineExceededErr(expiredCtx context.Context) deadlineExceededError {
	return deadlineExceededError{expiredCtx: expiredCtx}
}

var anyDeadlineExceededErr error = deadlineExceededError{}

type deadlineExceededError struct {
	expiredCtx context.Context
}

func (e deadlineExceededError) Error() string {
	return fmt.Sprintf("context %s deadline exceeded", contextName(e.expiredCtx))
}
func (deadlineExceededError) Timeout() bool   { return true }
func (deadlineExceededError) Temporary() bool { return true }

// WithCancel is similar compared to standard
// WithCancel, but returns a traceable error instead
func WithCancel(parent context.Context) (ctx context.Context, cancel context.CancelFunc) {
	c := newCancelCtx(parent)
	propagateCancel(parent, &c)
	return &c, func() { c.cancel(true, genCanceledErr(&c, nil)) }
}

// WithCancel is similar compared to standard
// WithCancel, but returns a traceable error instead
func WithCancelEx(parent context.Context) (ctx context.Context, cancel func(reason error)) {
	c := newCancelCtx(parent)
	propagateCancel(parent, &c)
	return &c, func(reason error) { c.cancel(true, genCanceledErr(&c, reason)) }
}

// newCancelCtx returns an initialized cancelCtx.
func newCancelCtx(parent context.Context) cancelCtx {
	return cancelCtx{Context: parent}
}

// goroutines counts the number of goroutines ever created; for testing.
var goroutines int32

// propagateCancel arranges for child to be canceled when parent is.
func propagateCancel(parent context.Context, child canceler) {
	done := parent.Done()
	if done == nil {
		return // parent is never canceled
	}

	select {
	case <-done:
		// parent is already canceled
		child.cancel(false, parent.Err())
		return
	default:
	}

	if p, ok := parentCancelCtx(parent); ok {
		p.mu.Lock()
		if p.err != nil {
			// parent has already been canceled
			child.cancel(false, p.err)
		} else {
			if p.children == nil {
				p.children = make(map[canceler]struct{})
			}
			p.children[child] = struct{}{}
		}
		p.mu.Unlock()
	} else {
		atomic.AddInt32(&goroutines, +1)
		go func() {
			select {
			case <-parent.Done():
				child.cancel(false, parent.Err())
			case <-child.Done():
			}
		}()
	}
}

// &cancelCtxKey is the key that a cancelCtx returns itself for.
var cancelCtxKey int

// parentCancelCtx returns the underlying *cancelCtx for parent.
// It does this by looking up parent.Value(&cancelCtxKey) to find
// the innermost enclosing *cancelCtx and then checking whether
// parent.Done() matches that *cancelCtx. (If not, the *cancelCtx
// has been wrapped in a custom implementation providing a
// different done channel, in which case we should not bypass it.)
func parentCancelCtx(parent context.Context) (*cancelCtx, bool) {
	done := parent.Done()
	if done == closedchan || done == nil {
		return nil, false
	}
	p, ok := parent.Value(&cancelCtxKey).(*cancelCtx)
	if !ok {
		return nil, false
	}
	p.mu.Lock()
	ok = p.done == done
	p.mu.Unlock()
	if !ok {
		return nil, false
	}
	return p, true
}

// removeChild removes a context from its parent.
func removeChild(parent context.Context, child canceler) {
	p, ok := parentCancelCtx(parent)
	if !ok {
		return
	}
	p.mu.Lock()
	if p.children != nil {
		delete(p.children, child)
	}
	p.mu.Unlock()
}

// A canceler is a context type that can be canceled directly. The
// implementations are *cancelCtx and *timerCtx.
type canceler interface {
	cancel(removeFromParent bool, err error)
	Done() <-chan struct{}
}

// closedchan is a reusable closed channel.
var closedchan = make(chan struct{})

func init() {
	close(closedchan)
}

// A cancelCtx can be canceled. When canceled, it also cancels any children
// that implement canceler.
type cancelCtx struct {
	context.Context

	mu       sync.Mutex            // protects following fields
	done     chan struct{}         // created lazily, closed by first cancel call
	children map[canceler]struct{} // set to nil by the first cancel call
	err      error                 // set to non-nil by the first cancel call
}

func (c *cancelCtx) Value(key interface{}) interface{} {
	if key == &cancelCtxKey {
		return c
	}
	return c.Context.Value(key)
}

func (c *cancelCtx) Done() <-chan struct{} {
	c.mu.Lock()
	if c.done == nil {
		c.done = make(chan struct{})
	}
	d := c.done
	c.mu.Unlock()
	return d
}

func (c *cancelCtx) Err() error {
	c.mu.Lock()
	err := c.err
	c.mu.Unlock()
	return err
}

type stringer interface {
	String() string
}

func contextName(c context.Context) string {
	if s, ok := c.(stringer); ok {
		return s.String()
	}
	return reflect.TypeOf(c).String()
}

func (c *cancelCtx) String() string {
	return contextName(c.Context) + ".WithCancel"
}

// cancel closes c.done, cancels each of c's children, and, if
// removeFromParent is true, removes c from its parent's children.
func (c *cancelCtx) cancel(removeFromParent bool, err error) {
	if err == nil {
		panic("context: internal error: missing cancel error")
	}
	c.mu.Lock()
	if c.err != nil {
		c.mu.Unlock()
		return // already canceled
	}
	c.err = err
	if c.done == nil {
		c.done = closedchan
	} else {
		close(c.done)
	}
	for child := range c.children {
		// NOTE: acquiring the child's lock while holding parent's lock.
		child.cancel(false, err)
	}
	c.children = nil
	c.mu.Unlock()

	if removeFromParent {
		removeChild(c.Context, c)
	}
}

// WithDeadline is similar compared to standard
// WithDeadline, but returns a traceable error instead
func WithDeadline(parent context.Context, d time.Time) (context.Context, context.CancelFunc) {
	if cur, ok := parent.Deadline(); ok && cur.Before(d) {
		// The current deadline is already sooner than the new one.
		return WithCancel(parent)
	}
	c := &timerCtx{
		cancelCtx: newCancelCtx(parent),
		deadline:  d,
	}
	propagateCancel(parent, c)
	dur := time.Until(d)
	if dur <= 0 {
		c.cancel(true, genDeadlineExceededErr(c)) // deadline has already passed
		return c, func() { c.cancel(false, genCanceledErr(c, nil)) }
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.err == nil {
		c.timer = time.AfterFunc(dur, func() {
			c.cancel(true, genDeadlineExceededErr(c))
		})
	}
	return c, func() { c.cancel(true, genCanceledErr(c, nil)) }
}

// A timerCtx carries a timer and a deadline. It embeds a cancelCtx to
// implement Done and Err. It implements cancel by stopping its timer then
// delegating to cancelCtx.cancel.
type timerCtx struct {
	cancelCtx
	timer *time.Timer // Under cancelCtx.mu.

	deadline time.Time
}

func (c *timerCtx) Deadline() (deadline time.Time, ok bool) {
	return c.deadline, true
}

func (c *timerCtx) String() string {
	return contextName(c.cancelCtx.Context) + ".WithDeadline(" +
		c.deadline.String() + " [" +
		time.Until(c.deadline).String() + "])"
}

func (c *timerCtx) cancel(removeFromParent bool, err error) {
	c.cancelCtx.cancel(false, err)
	if removeFromParent {
		// Remove this timerCtx from its parent cancelCtx's children.
		removeChild(c.cancelCtx.Context, c)
	}
	c.mu.Lock()
	if c.timer != nil {
		c.timer.Stop()
		c.timer = nil
	}
	c.mu.Unlock()
}

// WithTimeout is similar compared to standard
// WithTimeout, but returns a traceable error instead
func WithTimeout(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	return WithDeadline(parent, time.Now().Add(timeout))
}
