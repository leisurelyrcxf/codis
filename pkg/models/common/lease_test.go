package common

import (
	"sync"
	"testing"
	"time"

	"github.com/CodisLabs/codis/pkg/utils/log"
)

func TestAtomicTime(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(2)

	tt := NewAtomicDeadline(time.Now(), 0)
	go func() {
		defer wg.Done()

		for i := int64(1); i < 10000; i++ {
			tt.Set(time.Now(), i)
			time.Sleep(time.Millisecond)
		}
	}()

	go func() {
		defer wg.Done()

		for i := 0; i < 10000; i++ {
			deadline, ttl := tt.Get()
			log.Infof("ttl: %d, deadline: %s", ttl, deadline)
			time.Sleep(time.Millisecond)
		}
	}()

	wg.Wait()
}
