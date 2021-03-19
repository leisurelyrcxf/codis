package models

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/CodisLabs/codis/pkg/utils/sync2/atomic2"

	"github.com/CodisLabs/codis/pkg/models/common"
	"github.com/CodisLabs/codis/pkg/utils/assert"
	"github.com/CodisLabs/codis/pkg/utils/log"
	testifyassert "github.com/stretchr/testify/assert"
)

const testRoot = "/test/"

type MasterLease struct {
	LeaseStart, LeaseEnd time.Time
}

func (l MasterLease) String() string {
	return fmt.Sprintf("[%s, %s]", l.LeaseStart, l.LeaseEnd)
}

func newClient(t *testing.T, coordinator string) common.LockClient {
	etcdAddr := os.Getenv("ETCD_TEST_SERVER_ADDR")
	if len(etcdAddr) == 0 {
		t.Errorf("etcd server addr not found, you must set env ETCD_TEST_SERVER_ADDR first")
		return nil
	}
	c, err := NewClient(coordinator, etcdAddr, "", time.Second*5)
	if err != nil {
		t.Errorf("can't connection to %s, detail: '%s'", etcdAddr, err.Error())
		return nil
	}

	if err := c.Rmdir(testRoot); err != nil {
		t.Errorf("%s delete dir '%s' failed: %s", coordinator, testRoot, err)
		return nil
	}
	t.Logf("test dir '%s' cleared before testing", testRoot)
	return c
}

func TestDistributedLockV2(t *testing.T) {
	c := newClient(t, "etcd")
	if c == nil {
		t.Errorf("c==nil")
		return
	}
	for _, blockOnUserContext := range []bool{true, false} {
		for _, neverRelease := range []bool{true, false} {
			if neverRelease && !blockOnUserContext {
				continue
			}
			if !testDistributedLock(t, c, blockOnUserContext, neverRelease) {
				t.Errorf("testDistributedLock(%s, blockOnUserContext: %v, neverRelease:%v) failed", "etcd", blockOnUserContext, neverRelease)
				return
			}
		}
	}
}

func TestDistributedLockV3(t *testing.T) {
	t.Parallel()
	runtime.GOMAXPROCS(6)

	c := newClient(t, "etcdv3")
	if c == nil {
		t.Errorf("c==nil")
		return
	}
	for _, blockOnUserContext := range []bool{true, false} {
		for _, neverRelease := range []bool{true, false} {
			if !blockOnUserContext && neverRelease {
				continue
			}
			if !testDistributedLock(t, c, blockOnUserContext, neverRelease) {
				t.Errorf("testDistributedLock(%s, blockOnUserContext: %v, neverRelease:%v) failed", "etcdv3", blockOnUserContext, neverRelease)
				return
			}
		}
	}
}

func testDistributedLock(t *testing.T, c common.LockClient, blockOnUserContext, neverRelease bool) (b bool) {
	log.StdLog.SetLevel(log.LevelWarn)

	const (
		lockPath  = "/test/lock"
		threadNum = 7
	)
	assert.Must(c != nil)

	var (
		wgWorkers      sync.WaitGroup
		allWorkersDone = make(chan struct{})
		rhs            = make([]*common.RoleState, threadNum)
		masterCount    atomic2.Int64

		wgObservers sync.WaitGroup
		masterTerms = make([][]string, threadNum)
		appendTerm  = func(j int, term string) {
			if len(masterTerms[j]) == 0 || masterTerms[j][len(masterTerms[j])-1] != term {
				masterTerms[j] = append(masterTerms[j], term)
			}
		}

		masterLeases   = make([]MasterLease, 0, 10)
		leaseMutex     sync.Mutex
		onBecomeMaster = func(j int) {
			t := time.Now()
			masterCount.Add(1)

			leaseMutex.Lock()
			masterLeases = append(masterLeases, MasterLease{LeaseStart: t})
			leaseMutex.Unlock()

			log.Warnf("thread %d grabbed lock at %s", j, t)
		}
		onLooseMaster = func(j int, newMaster string, desc error) {
			t := time.Now()
			masterCount.Sub(1)

			leaseMutex.Lock()
			assert.Must(len(masterLeases) > 0)
			last := &masterLeases[len(masterLeases)-1]
			last.LeaseEnd = t
			assert.Must(last.LeaseStart.Before(last.LeaseEnd))
			leaseMutex.Unlock()

			if newMaster == "" {
				log.Warnf("")
				if desc != nil {
					log.Warnf("thread %d lost master at %s due to %v", j, t, desc)
				} else {
					log.Warnf("thread %d give up master at %s", j, t)
				}
				log.Warnf("")
			}
		}

		//stopped     atomic2.Bool
		ctx, cancel = context.WithCancel(context.Background())
	)
	defer cancel()
	for i := range rhs {
		rs := common.NewRoleState()
		rhs[i] = &rs
	}
	for j := 0; j < threadNum; j++ {
		wgWorkers.Add(1)
		go func(j int) {
			defer wgWorkers.Done()

			for ctx.Err() == nil {
				if err := WithLocked(
					ctx,
					c, lockPath, []byte(strconv.Itoa(j)),
					common.LockConfig{
						TTL:                 1,
						SpinIntervalOnError: 500 * time.Millisecond,
						WatchPeriod:         10 * time.Second,
					}, common.UserCallbacks{
						OnData: func(data []byte) error {
							if len(data) == 0 {
								panic("data")
							}
							log.Warnf("thread %d found master %v", j, string(data))
							if rhs[j].SetSlaveWhetherRoleChanged(string(data)) {
								onLooseMaster(j, string(data), nil)
							}
							appendTerm(j, string(data))
							return nil
						},
						OnErr: func(desc error, reason error) (kontinue bool) {
							if desc == common.ErrLockExpired {
								go func() {
									time.Sleep(5 * time.Second)
								}()
								return
							}
							if rhs[j].SetSlaveWhetherRoleChanged("") {
								onLooseMaster(j, "", desc)
							}
							return true
						},
						BeforeKeepAliveOnce: func(ctx context.Context, lease *common.Lease, ttl int64) (kontinue bool) {
							if neverRelease {
								return true
							}
							return lease.GrantTime.Add(time.Second * time.Duration(randomInt(2, 4)) / 2).After(time.Now())
						},
					}, func(ctx context.Context) error {
						assert.Must(rhs[j].SetMasterWhetherRoleChanged())
						onBecomeMaster(j)
						appendTerm(j, strconv.Itoa(j))
						if blockOnUserContext {
							<-ctx.Done()
						} else {
							time.Sleep(time.Second * time.Duration(randomInt(4, 5)) / 2)
						}
						if rhs[j].SetSlaveWhetherRoleChanged("") {
							onLooseMaster(j, "", ctx.Err())
						}
						return ctx.Err()
					}); err != nil && ctx.Err() == nil {
					log.Warnf("thread %d retry due to err '%v' at %s\n", j, err, time.Now())
				}
			}
		}(j)
	}

	for i := 0; i < 5; i++ {
		wgObservers.Add(1)
		go func() {
			defer wgObservers.Done()

			for {
				select {
				case <-allWorkersDone:
					return
				default:
					break
				}
				if count := masterCount.Int64(); count != 0 && count != 1 {
					t.Errorf("mutual exclusion failed: masterCount(%d) not 0 or 1", count)
					time.Sleep(1 * time.Second)
					os.Exit(1)
				}
				//t.Logf("master count: %d", masterCount.Int64())
				time.Sleep(2 * time.Millisecond)
			}
		}()
	}

	time.Sleep(36 * time.Second)
	cancel()
	wgWorkers.Wait()
	close(allWorkersDone)
	wgObservers.Wait()

	tAssert := testifyassert.New(t)
	b = true
	{

		maximumLen := -1
		maximumJ := -1
		for j := 0; j < threadNum; j++ {
			if len(masterTerms[j]) > maximumLen {
				maximumLen = len(masterTerms[j])
				maximumJ = j
			}
		}
		expMasterTerms := strings.Join(masterTerms[maximumJ], ",")
		for j := 1; j < threadNum; j++ {
			jMasterTerms := strings.Join(masterTerms[j], ",")
			if !tAssert.Truef(strings.HasPrefix(expMasterTerms, jMasterTerms), "strings.HasPrefix(%v, %v)", expMasterTerms, jMasterTerms) {
				b = false
			}
		}
		for j := 0; j < threadNum; j++ {
			fmt.Printf("thread %d master terms: %v\n", j, strings.Join(masterTerms[j], ","))
		}
	}

	{
		for i := 1; i < len(masterLeases); i++ {
			prev, cur := masterLeases[i-1], masterLeases[i]
			if !tAssert.True(prev.LeaseEnd.Before(cur.LeaseStart)) {
				b = false
			}
		}
		fmt.Printf("Master Lease 0: %s\n", masterLeases[0])
		for i := 1; i < len(masterLeases); i++ {
			prev, cur := masterLeases[i-1], masterLeases[i]
			fmt.Printf("Master Lease %d: %s, interval between lease: %s\n", i, cur, cur.LeaseStart.Sub(prev.LeaseEnd))
		}
	}
	return b
}

func randomInt(i, j int64) int64 {
	rand.Seed(int64(time.Now().Nanosecond()))
	return rand.Int63n(j-i+1) + i
}
