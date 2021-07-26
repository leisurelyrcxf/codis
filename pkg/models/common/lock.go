package common

import (
	"context"
	"errors"
	"time"

	"github.com/CodisLabs/codis/pkg/utils/assert"
	"github.com/CodisLabs/codis/pkg/utils/log"
	"github.com/CodisLabs/codis/pkg/utils/math2"
	"github.com/CodisLabs/codis/pkg/utils/traceablecontext"
)

const MinTTL = 10

var (
	ErrLockExpired           = errors.New("lock expired")
	ErrLockDefinitelyExpired = errors.New("lock definitely expired")
	ErrKeyAlreadyExists      = errors.New("key already exists")
	ErrClosedLockClient      = errors.New("use of closed lock client")
	ErrCASFailed             = errors.New("cas failed")
	ErrOnDataFailed          = errors.New("on data failed")
	ErrWatchFailed           = errors.New("watch failed")

	ErrDummy = errors.New("this is a dummy distributed lock")
)

type BeforeKeepAliveOnceFunc func(ctx context.Context, lease *Lease, ttl int64) (kontinue bool)

type UserCallbacks struct {
	OnData func(data []byte) error
	OnErr  func(desc, reason error) (kontinue bool) // should be non blocking

	BeforeKeepAliveOnce BeforeKeepAliveOnceFunc // used for testing only
}

type LockConfig struct {
	TTL                 int64
	SpinIntervalOnError time.Duration
	WatchPeriod         time.Duration

	deadlineLoopInterval time.Duration
}

func (l *LockConfig) Sanitize(test bool) {
	if !test {
		l.TTL = math2.MaxInt64(l.TTL, MinTTL)
	} else {
		l.TTL = math2.MaxInt64(l.TTL, 2)
	}
	if l.SpinIntervalOnError <= 0 {
		l.SpinIntervalOnError = time.Second
	}
	if l.WatchPeriod <= 0 {
		l.WatchPeriod = time.Minute
	}
	l.deadlineLoopInterval = math2.MinDuration(time.Millisecond*100, time.Second*time.Duration(l.TTL)/250)
}

type DistributedLock interface {
	Lock(context.Context) (context.Context, error)
	Unlock() // unlock must succeed
}

type DummyDistributedLock struct{}

func (DummyDistributedLock) Context() context.Context {
	return context.TODO()
}
func (DummyDistributedLock) Lock() (*Lease, error) {
	return nil, ErrDummy
}
func (DummyDistributedLock) Unlock() error {
	return ErrDummy
}
func (DummyDistributedLock) OnError(err error) {
	return
}

type LockClient interface {
	IsClosed() bool
	CAS(ctx, parent context.Context, path string, data []byte, ttl int64, deadlineLoopInterval time.Duration,
		onError func(desc, reason error) bool, _ BeforeKeepAliveOnceFunc) (curData []byte, curRev int64, lease *Lease, err error)
	WatchOnce(ctx context.Context, path string, revision int64) error
	KeepAliveOnce(ctx context.Context, lease *Lease, ttl int64) (newTTL int64, kontinue bool, err error)
	Revoke(ctx context.Context, lease *Lease) error
	DeleteRevision(path string, rev int64) error
}

type DummyLockClient struct {
}

func (c *DummyLockClient) CAS(context.Context, context.Context, string, []byte, int64, time.Duration,
	func(error, error) bool, BeforeKeepAliveOnceFunc) (curData []byte, curRev int64, lease *Lease, err error) {
	return nil, -1, nil, ErrDummy
}
func (c *DummyLockClient) WatchOnce(context.Context, string, int64) error {
	return ErrDummy
}
func (c *DummyLockClient) KeepAliveOnce(context.Context, *Lease, int64) (int64, bool, error) {
	return 0, false, ErrDummy
}
func (c *DummyLockClient) Revoke(context.Context, *Lease) error {
	return ErrDummy
}
func (c *DummyLockClient) DeleteRevision(path string, rev int64) error {
	return ErrDummy
}

type DistributedLockImpl struct {
	client LockClient

	path          string
	data          []byte
	cfg           LockConfig
	userCallbacks UserCallbacks

	lease *Lease
	rev   int64
}

func NewDistributedLockImpl(lockClient LockClient, path string, data []byte, cfg LockConfig, userCallbacks UserCallbacks) *DistributedLockImpl {
	l := &DistributedLockImpl{
		client:        lockClient,
		path:          path,
		data:          data,
		cfg:           cfg,
		userCallbacks: userCallbacks,
	}
	l.cfg.Sanitize(userCallbacks.BeforeKeepAliveOnce != nil)
	l.sanitizeUserCallbacks()
	return l
}

func (l *DistributedLockImpl) sanitizeUserCallbacks() {
	if l.userCallbacks.OnData == nil {
		l.userCallbacks.OnData = func(data []byte) error { return nil }
	}
	userOnErr := l.userCallbacks.OnErr
	if userOnErr == nil {
		userOnErr = func(desc, reason error) bool { return true }
	}
	if l.userCallbacks.BeforeKeepAliveOnce == nil { // product env
		l.userCallbacks.OnErr = func(desc, err error) bool {
			log.Errorf("withLocked error occurred: %v: %v", desc, err)
			return userOnErr(desc, err)
		}
	} else { // test env
		l.userCallbacks.OnErr = func(desc, err error) bool {
			log.Infof("withLocked error occurred: %v: %v", desc, err)
			return userOnErr(desc, err)
		}
	}
	if l.userCallbacks.BeforeKeepAliveOnce == nil {
		l.userCallbacks.BeforeKeepAliveOnce = func(ctx context.Context, lease *Lease, ttl int64) (kontinue bool) {
			return true
		}
	}
}

func (l *DistributedLockImpl) Lock(ctx context.Context) (_ context.Context, err error) {
	log.Debugf("[DistributedLockImpl][Lock] try to grab distributed lock %s", l.path)
	defer func() {
		if err != nil {
			if err == ctx.Err() {
				log.Infof("[DistributedLockImpl][Lock] lock failed: %v", err)
			} else {
				log.Errorf("[DistributedLockImpl][Lock] lock failed: %v", err)
			}
		}
	}()
	for !l.client.IsClosed() && ctx.Err() == nil {
		casCtx, casCancel := context.WithTimeout(ctx, l.cfg.WatchPeriod)
		curData, curRev, lease, err := l.client.CAS(casCtx, ctx, l.path, l.data, l.cfg.TTL, l.cfg.deadlineLoopInterval,
			l.userCallbacks.OnErr, l.userCallbacks.BeforeKeepAliveOnce)
		casCancel()
		if err == nil {
			assert.Must(lease != nil)
			l.lease = lease
			l.rev = curRev

			log.Debugf("[DistributedLockImpl][Lock] grabbed distributed lock %s at %s", l.path, time.Now())
			return l.lease.ctx, nil
		}
		assert.Must(lease == nil)
		if err != ErrKeyAlreadyExists {
			if !l.userCallbacks.OnErr(ErrCASFailed, err) {
				return nil, err
			}
			if err != ctx.Err() {
				time.Sleep(l.cfg.SpinIntervalOnError)
			}
			continue
		}
		if err := l.userCallbacks.OnData(curData); err != nil {
			if !l.userCallbacks.OnErr(ErrOnDataFailed, err) {
				return nil, err
			}
			if err != ctx.Err() {
				time.Sleep(l.cfg.SpinIntervalOnError)
			}
			continue
		}
		watchCtx, watchCancel := traceablecontext.WithTimeout(ctx, l.cfg.WatchPeriod)
		if err = l.client.WatchOnce(watchCtx, l.path, curRev); err != nil && !traceablecontext.IsDeadlineExceededErr(watchCtx, err) {
			if !l.userCallbacks.OnErr(ErrWatchFailed, err) {
				return nil, err
			}
			if err != ctx.Err() {
				time.Sleep(l.cfg.SpinIntervalOnError)
			}
			watchCancel()
			continue
		}
		if err == nil {
			log.Debugf("[DistributedLockImpl][Lock] event occurred on lock %s, retrying CAS...", l.path)
		} else {
			log.Debugf("[DistributedLockImpl][Lock] watcher timeout on lock %s(watchPeriod:%s), retrying CAS...", l.path, l.cfg.WatchPeriod)
		}
		watchCancel()
	}
	if l.client.IsClosed() {
		err = ErrClosedLockClient
	} else {
		err = ctx.Err()
	}
	assert.Must(err != nil)
	l.userCallbacks.OnErr(err, err)
	return nil, err
}

func (l *DistributedLockImpl) Unlock() {
	if l.lease == nil {
		return
	}

	defer l.lease.CloseAsync()()

	for err := ErrDummy; err != nil && !l.lease.mustExpired(); {
		if err = l.client.DeleteRevision(l.path, l.rev); err != nil {
			log.Warnf("[DistributedLockImpl][Unlock] delete path %v @revision %d failed: %v", l.path, l.rev, err)
		}
	}

	l.lease = nil
	log.Debugf("[DistributedLockImpl][Unlock] unlocked %s at %s", l.path, time.Now())
}
