package utils

import (
	"context"
	"github.com/CodisLabs/codis/pkg/utils/log"
	"time"
)

func RetryInSecond(maxTries int, f func() error) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(maxTries))
	defer cancel()
	for {
		select {
		case <-ctx.Done():
			log.Warnf("[RetryInSecond] last retry also failed after %d times", maxTries)
			return ctx.Err()
		default:
			if err := f(); err != nil {
				log.Warnf("[RetryInSecond] run failed %v, retrying...", err)
				break
			}
			return nil
		}
		time.Sleep(time.Second * 1)
	}
}
