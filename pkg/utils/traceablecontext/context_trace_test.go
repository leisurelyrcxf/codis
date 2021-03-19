package traceablecontext

import (
	"context"
	"testing"
	"time"

	testifyassert "github.com/stretchr/testify/assert"
)

func TestIsDeadlineExceededErr(t *testing.T) {
	assert := testifyassert.New(t)
	testIsDeadlineExceededErr(assert, 1*time.Millisecond, 10*time.Millisecond)
	testIsDeadlineExceededErr(assert, 10*time.Millisecond, 1*time.Millisecond)
}

func testIsDeadlineExceededErr(assert *testifyassert.Assertions,
	parentTimeout time.Duration, childTimeout time.Duration) {
	parentCtx, parentCancel := WithTimeout(context.Background(), parentTimeout)
	defer parentCancel()

	childCtx, childCancel := WithTimeout(parentCtx, childTimeout)
	defer childCancel()

	childErr := waitContext(childCtx)

	var expCtx, nonExpCtx context.Context
	if parentTimeout < childTimeout {
		expCtx = parentCtx
		nonExpCtx = childCtx
		assert.Equal(childErr, parentCtx.Err())
	} else {
		expCtx = childCtx
		nonExpCtx = parentCtx
		assert.NotEqual(childErr, parentCtx.Err())
	}

	assert.True(IsDeadlineExceededErr(expCtx, childErr))
	assert.False(IsDeadlineExceededErr(nonExpCtx, childErr))
	assert.True(IsDeadlineExceededErr(nil, childErr))

	parentErr := waitContext(parentCtx)
	assert.True(IsDeadlineExceededErr(parentCtx, parentErr))
	assert.False(IsDeadlineExceededErr(childCtx, parentErr))
	assert.True(IsDeadlineExceededErr(nil, parentErr))
}

func TestIsDeadlineExceededErrMixed1(t *testing.T) {
	assert := testifyassert.New(t)
	testIsDeadlineExceededErrMixed1(assert, 1*time.Millisecond, 10*time.Millisecond)
	testIsDeadlineExceededErrMixed1(assert, 10*time.Millisecond, 1*time.Millisecond)
}

func testIsDeadlineExceededErrMixed1(assert *testifyassert.Assertions,
	parentTimeout time.Duration, childTimeout time.Duration) {
	parentCtx, parentCancel := WithTimeout(context.Background(), parentTimeout)
	defer parentCancel()

	childCtx, childCancel := context.WithTimeout(parentCtx, childTimeout)
	defer childCancel()

	childErr := waitContext(childCtx)

	if parentTimeout < childTimeout {
		assert.Equal(childErr, parentCtx.Err())
		assert.True(IsDeadlineExceededErr(parentCtx, childErr))
		assert.False(IsDeadlineExceededErr(childCtx, childErr))
		assert.True(IsDeadlineExceededErr(nil, childErr))

		parentErr := waitContext(parentCtx)
		assert.True(IsDeadlineExceededErr(parentCtx, parentErr))
		assert.False(IsDeadlineExceededErr(childCtx, parentErr))
		assert.True(IsDeadlineExceededErr(nil, parentErr))
	} else {
		assert.NotEqual(childErr, parentCtx.Err())
		assert.True(childErr == context.DeadlineExceeded)
		assert.False(IsDeadlineExceededErr(parentCtx, childErr))
		assert.False(IsDeadlineExceededErr(childCtx, childErr))
		assert.False(IsDeadlineExceededErr(nil, childErr))

		parentErr := waitContext(parentCtx)
		assert.True(IsDeadlineExceededErr(parentCtx, parentErr))
		assert.False(IsDeadlineExceededErr(childCtx, parentErr))
		assert.True(IsDeadlineExceededErr(nil, parentErr))
	}
}

func TestIsDeadlineExceededErrMixed2(t *testing.T) {
	assert := testifyassert.New(t)
	testIsDeadlineExceededErrMixed2(assert, 1*time.Millisecond, 10*time.Millisecond)
	testIsDeadlineExceededErrMixed2(assert, 10*time.Millisecond, 1*time.Millisecond)
}

func testIsDeadlineExceededErrMixed2(assert *testifyassert.Assertions,
	parentTimeout time.Duration, childTimeout time.Duration) {
	parentCtx, parentCancel := context.WithTimeout(context.Background(), parentTimeout)
	defer parentCancel()

	childCtx, childCancel := WithTimeout(parentCtx, childTimeout)
	defer childCancel()

	childErr := waitContext(childCtx)
	if parentTimeout < childTimeout {
		assert.Equal(childErr, parentCtx.Err())
		assert.True(childErr == context.DeadlineExceeded)
		assert.False(IsDeadlineExceededErr(parentCtx, childErr))
		assert.False(IsDeadlineExceededErr(childCtx, childErr))
		assert.False(IsDeadlineExceededErr(nil, childErr))

		parentErr := waitContext(parentCtx)
		assert.False(IsDeadlineExceededErr(parentCtx, parentErr))
		assert.False(IsDeadlineExceededErr(childCtx, parentErr))
		assert.False(IsDeadlineExceededErr(nil, parentErr))
	} else {
		assert.NotEqual(childErr, parentCtx.Err())
		assert.True(IsDeadlineExceededErr(childCtx, childErr))
		assert.False(IsDeadlineExceededErr(parentCtx, childErr))
		assert.True(IsDeadlineExceededErr(nil, childErr))

		parentErr := waitContext(parentCtx)
		assert.False(IsDeadlineExceededErr(parentCtx, parentErr))
		assert.False(IsDeadlineExceededErr(childCtx, parentErr))
		assert.False(IsDeadlineExceededErr(nil, parentErr))
	}
}

func TestIsCanceledErr(t *testing.T) {
	assert := testifyassert.New(t)
	testIsCanceledErr(assert, func(parentCtx, childCtx context.Context) context.Context {
		return parentCtx
	})
	testIsCanceledErr(assert, func(parentCtx, childCtx context.Context) context.Context {
		return childCtx
	})
}

func testIsCanceledErr(assert *testifyassert.Assertions, pickVictim func(parentCtx, childCtx context.Context) context.Context) {
	parentCtx, parentCancel := WithCancel(context.Background())
	defer parentCancel()

	childCtx, childCancel := WithCancel(parentCtx)
	defer childCancel()

	victim := pickVictim(parentCtx, childCtx)
	nonVictim := func(victim context.Context) context.Context {
		for _, ctx := range []context.Context{parentCtx, childCtx} {
			if ctx != victim {
				return ctx
			}
		}
		assert.True(false)
		return nil
	}(victim)
	go func() {
		if victim == parentCtx {
			parentCancel()
			return
		}
		assert.Equal(childCtx, victim)
		childCancel()
		parentCancel()
	}()

	childErr := waitContext(childCtx)
	if victim == parentCtx {
		assert.Equal(childErr, parentCtx.Err())
	} else {
		assert.NotEqual(childErr, parentCtx.Err())
	}
	assert.True(IsCanceledErr(victim, childErr))
	assert.False(IsCanceledErr(nonVictim, childErr))
	assert.True(IsCanceledErr(nil, childErr))

	parentErr := waitContext(parentCtx)
	assert.True(IsCanceledErr(parentCtx, parentErr))
	assert.False(IsCanceledErr(childCtx, parentErr))
	assert.True(IsCanceledErr(nil, parentErr))
}

func waitContext(ctx context.Context) error {
	<-ctx.Done()
	return ctx.Err()
}
