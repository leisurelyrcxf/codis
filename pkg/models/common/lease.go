package common

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/CodisLabs/codis/pkg/utils/assert"

	"github.com/CodisLabs/codis/pkg/utils/log"
	"github.com/CodisLabs/codis/pkg/utils/traceablecontext"
)

var ErrLeaseClosed = errors.New("lease closed")

type Deadline struct {
	deadline time.Time
	ttl      int64
}

type AtomicDeadline struct {
	deadline atomic.Value
}

func NewAtomicDeadline(deadline time.Time, ttl int64) AtomicDeadline {
	d := AtomicDeadline{}
	d.Set(deadline, ttl)
	return d
}

func (t *AtomicDeadline) Set(deadline time.Time, ttl int64) {
	t.deadline.Store(&Deadline{
		deadline: deadline,
		ttl:      ttl,
	})
}

func (t *AtomicDeadline) Get() (time.Time, int64) {
	x := t.deadline.Load()
	assert.Must(x != nil)
	return x.(*Deadline).deadline, x.(*Deadline).ttl
}

// Lease guarantees to see Lease.deadline exceeded before lease timeout in etcd,
// thus make it safer to use for a distributed lock than etcd.concurrency.Mutex.
// With Lease, one can implement a distributed election without worrying about
// multi-masters exist at a given time.
type Lease struct {
	client LockClient

	ID        interface{}
	GrantTime time.Time

	ctx       context.Context
	canceller func(err error)

	deadline AtomicDeadline

	deadlineLoopInterval time.Duration
	onError              func(desc error, reason error) bool
	beforeKeepAliveOnce  BeforeKeepAliveOnceFunc

	cancelOnce               sync.Once
	deadlineLoopDone, closed chan struct{}
}

func NewLease(
	parent context.Context, lockClient LockClient, id interface{}, ttl int64,
	deadlineLoopInterval time.Duration, grantTime time.Time,
	onError func(desc error, reason error) bool,
	beforeKeepAliveOnce BeforeKeepAliveOnceFunc) *Lease {
	ctx, cancel := traceablecontext.WithCancelEx(parent)
	l := &Lease{
		client: lockClient,

		ID:        id,
		GrantTime: grantTime,

		ctx:                  ctx,
		canceller:            cancel,
		deadline:             NewAtomicDeadline(genDeadline(grantTime, ttl), ttl),
		deadlineLoopInterval: deadlineLoopInterval,
		onError:              onError,
		beforeKeepAliveOnce:  beforeKeepAliveOnce,
		deadlineLoopDone:     make(chan struct{}),
		closed:               make(chan struct{}),
	}

	go l.loopDeadline()
	go l.loopKeepAlive(genNextKeepAlive(grantTime, ttl))

	return l
}

func (l *Lease) cancel(reason error) {
	l.cancelOnce.Do(func() {
		l.canceller(reason)

		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
			defer cancel()

			_ = l.client.Revoke(ctx, l)
		}()
	})
}

func (l *Lease) Close() {
	close(l.closed)
	l.cancel(ErrLeaseClosed)
	<-l.deadlineLoopDone
}

func (l *Lease) CloseAsync() func() {
	close(l.closed)
	l.cancel(ErrLeaseClosed)

	return func() {
		<-l.deadlineLoopDone
	}
}

func (l *Lease) loopDeadline() {
	defer close(l.deadlineLoopDone)

	var (
		deadlineExceeded           = false
		deadline, definiteDeadline time.Time
		ttl                        int64
	)

	for {
		if !deadlineExceeded {
			deadline, ttl = l.deadline.Get()
			definiteDeadline = inferDefiniteDeadline(deadline, ttl)
		}

		now := time.Now() // guarantee to see l.definitelyDone after l.done
		if !deadlineExceeded && deadline.Before(now) {
			l.onError(ErrLockExpired, ErrLockExpired)
			l.cancel(ErrLockExpired)

			log.Warnf("[loopDeadline] lease %v expired, call l.onDeadlineExceeded(), deadline: %v, now: %v", l.ID, deadline, now)
			deadlineExceeded = true
		}
		if definiteDeadline.Before(now) {
			l.onError(ErrLockDefinitelyExpired, ErrLockDefinitelyExpired)
			l.cancel(ErrLockDefinitelyExpired)

			log.Warnf("[loopDeadline] lease %v expired, call l.onDefiniteDeadlineExceeded(), definiteDeadline: %v, deadline: %v, diff: %s, now: %v", l.ID, definiteDeadline, deadline, definiteDeadline.Sub(deadline), now)
			return
		}

		select {
		case <-time.After(l.deadlineLoopInterval):
		case <-l.closed:
			return
		}
	}
}

func (l *Lease) loopKeepAlive(nextKeepAlive time.Time) {
	_, ttl := l.deadline.Get()

	for {
		if !l.beforeKeepAliveOnce(l.ctx, l, ttl) {
			return
		}
		updateTime := time.Now()
		newTTL, kontinue, err := l.client.KeepAliveOnce(l.ctx, l, ttl)
		if err == nil {
			l.deadline.Set(genDeadline(updateTime, newTTL), newTTL)
			log.Debugf("[loopKeepAlive] update lease %p deadline to %s, ttl: %d", l, genDeadline(updateTime, newTTL), newTTL)
		} else {
			newTTL = ttl
			log.Warnf("[loopKeepAlive] update lease %p failed: '%v', ttl: %d", l, err, newTTL)
		}
		if !kontinue {
			return
		}

		nextKeepAlive = genNextKeepAlive(nextKeepAlive, newTTL)
		select {
		case <-l.ctx.Done():
			return
		case <-time.After(nextKeepAlive.Sub(time.Now())):
			break
		}
		ttl = newTTL
	}
}

func (l *Lease) mustExpired() bool {
	deadline, ttl := l.deadline.Get()
	return deadline.Add(time.Second * time.Duration(ttl) * 8 / 3).Before(time.Now())
}

const (
	keepAliveFraction        = 20
	deadlineFraction         = 63
	definiteDeadlineFraction = 83
)

func genNextKeepAlive(curKeepAlive time.Time, ttl int64) (nextKeepAlive time.Time) {
	return curKeepAlive.Add(time.Second * time.Duration(ttl) * keepAliveFraction / 100)
}

func genDeadline(base time.Time, ttl int64) time.Time {
	return base.Add(time.Second * time.Duration(ttl) * deadlineFraction / 100)
}

func inferDefiniteDeadline(deadline time.Time, ttl int64) time.Time {
	return deadline.Add(time.Second * time.Duration(ttl) * (definiteDeadlineFraction - deadlineFraction) / 100)
}
