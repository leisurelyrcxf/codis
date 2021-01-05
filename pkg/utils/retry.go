package utils

import (
	"context"
	"time"

	"github.com/CodisLabs/codis/pkg/utils/errors"
	"github.com/CodisLabs/codis/pkg/utils/log"
)

var RetryAnyError = func(error) bool {
	return true
}

func WithRetry(interval, timeout time.Duration, f func() error) error {
	return WithRetryEx(interval, timeout, f, RetryAnyError)
}

func WithRetryEx(interval, timeout time.Duration, f func() error, isRetryableErr func(error) bool) error {
	var start = time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	for {
		err := f()
		if err == nil || !isRetryableErr(err) {
			return err
		}
		select {
		case <-ctx.Done():
			log.Errorf("[WithRetryEx] failed after retrying for %s, last error: '%v'", time.Since(start), errors.Trace(err))
			return err
		default:
			log.Warnf("[WithRetryEx] run failed with error '%v', retrying in %s...", err, interval)
			time.Sleep(interval)
		}
	}
}
