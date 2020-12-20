package utils

import (
	"context"
	"time"

	"github.com/CodisLabs/codis/pkg/utils/log"
)

func WithRetry(interval, timeout time.Duration, f func() error) error {
	var start = time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	for {
		err := f()
		if err == nil {
			return err
		}
		select {
		case <-ctx.Done():
			log.Errorf("[WithRetry] failed after retrying for %s, last error: '%v'", time.Since(start), err)
			return err
		default:
			log.Warnf("[WithRetry] run failed with error '%v', retrying...", err)
			time.Sleep(interval)
		}
	}
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
			log.Errorf("[WithRetry] failed after retrying for %s, last error: '%v'", time.Since(start), err)
			return err
		default:
			log.Warnf("[WithRetry] run failed with error '%v', retrying...", err)
			time.Sleep(interval)
		}
	}
}
