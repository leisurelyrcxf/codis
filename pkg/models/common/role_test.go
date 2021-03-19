package common

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/CodisLabs/codis/pkg/utils/assert"
)

func TestRole(t *testing.T) {
	var r Role
	assert.Must(r == RoleSlave)
	assert.Must(r != RoleMaster)
	assert.Must(RoleSlave == RoleSlave)
	assert.Must(RoleMaster == RoleMaster)
	assert.Must(RoleSlave != RoleMaster)

	var rh = NewRoleState()
	assert.Must(rh.IsSlave(nil))
	rh.SetSlave("127.0.0.4", time.Now())
	go func() {
		assert.Must(rh.IsSlave(nil))
		time.Sleep(time.Second)
		rh.SetMaster(time.Now())
	}()
	assert.Must(rh.IsSlave(nil))
	for i := 0; i < 10; i++ {
		fmt.Printf("%v\n", rh.IsMaster())
		time.Sleep(150 * time.Millisecond)
	}
	assert.Must(rh.IsMaster())
}

func TestRoleState_SetSlaveTracing(t *testing.T) {
	for i := 0; i < 10; i++ {
		if !testRoleStateSetSlaveTracing(t) {
			t.Errorf("test failed at round %d", i)
			return
		}
	}
}

func testRoleStateSetSlaveTracing(t *testing.T) (b bool) {
	var wg sync.WaitGroup
	wg.Add(3)
	var rh = NewRoleState()

	go func() {
		defer wg.Done()

		for i := 0; i < 1000; i++ {
			rh.SetMaster(time.Now())
			time.Sleep(100 * time.Microsecond)
			rh.SetSlave("", time.Now())
		}
	}()
	go func() {
		defer wg.Done()

		for i := 0; i < 100000; i++ {
			rh.SetSlave("123", time.Now())
		}
	}()

	const master = "127.0.0.1"
	go func() {
		defer wg.Done()

		rand.Seed(int64(time.Now().Nanosecond()))
		time.Sleep(time.Duration(rand.Int63n(64)) * time.Millisecond)
		rh.SetSlave(master, time.Now().Add(time.Hour))
	}()
	wg.Wait()

	var masterAddr string
	if !rh.IsSlave(&masterAddr) || masterAddr != master {
		t.Logf("test failed, exp: slave with master '%s', but got %s", master, rh)
		return false
	}

	return true
}
