package redis

import (
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/CodisLabs/codis/pkg/utils/pika"

	"github.com/CodisLabs/codis/pkg/utils/log"
)

func TestSlaveOf(t *testing.T) {
	p := NewPool("", 30*time.Second)

	const master, slave = "127.0.0.1:56380", "127.0.0.1:56381"
	var err error

	errorf := func(format string, args ...interface{}) {
		t.Errorf(format, args...)
		os.Exit(1)
	}

	slaveOfAllSlotsAsync := func(masterAddr, slaveAddr string, slaveSlots []int, force bool) error {
		return p.WithRedisClient(slaveAddr, func(client *Client) error {
			return client.SlaveOfAllSlots(masterAddr, slaveSlots, force)
		})
	}

	slaveOfSlotsAsync := func(masterAddr, slaveAddr string, sg pika.SlotGroup, force bool) error {
		return p.WithRedisClient(slaveAddr, func(client *Client) error {
			return client.SlaveOfSlots(masterAddr, sg, force)
		})
	}

	for i := 0; i < 100; i++ {
		if err = p.AddSlotIfNotExists(slave, 0); err != nil {
			errorf(err.Error())
		}

		if err = p.AddSlotIfNotExists(slave, 1); err != nil {
			errorf(err.Error())
		}

		if err = p.SlaveOfAsync(master, slave, 0, false, false); err != nil {
			errorf(err.Error())
		}

		if err = slaveOfSlotsAsync(master, slave, pika.NewSlotGroup(0, 1), false); err != nil {
			errorf(err.Error())
		}

		if err = p.SlaveOfAsync(master, slave, 1, false, false); err != nil {
			errorf(err.Error())
		}

		if err = slaveOfAllSlotsAsync(master, slave, []int{0, 1}, false); err != nil {
			errorf(err.Error())
		}

		if err = slaveOfAllSlotsAsync("no:one", slave, []int{0, 1}, false); err != nil {
			errorf(err.Error())
		}

		if err = p.CleanSlotIfExists(slave, 0); err != nil {
			errorf(err.Error())
		}

		if err = p.CleanSlotIfExists(slave, 0); err != nil {
			errorf(err.Error())
		}

		if err = p.CleanSlotIfExists(slave, 1); err != nil {
			errorf(err.Error())
		}

		if err = p.CleanSlotIfExists(slave, 1); err != nil {
			errorf(err.Error())
		}

		if err = p.AddSlotIfNotExists(slave, 0); err != nil {
			errorf(err.Error())
		}

		if err = p.AddSlotIfNotExists(slave, 0); err != nil {
			errorf(err.Error())
		}

		if err = p.AddSlotIfNotExists(slave, 1); err != nil {
			errorf(err.Error())
		}

		log.Infof("")
		log.Infof("")
		log.Infof("")
	}
}

func TestReslaveOf(t *testing.T) {
	for i := 0; i < 100; i++ {
		if !testReslaveOf(t) {
			t.Errorf("testReslaveOf failed")
			return
		}
		//time.Sleep(time.Second)
		slaveSSTCount, err := countSSTFile("/usr/local/codis-etcd/pika/pika-56381/db/db0/1/strings/")
		if err != nil {
			t.Error(err)
			return
		}
		now := time.Now()
		masterDumpSSTCount, err := countSSTFile(fmt.Sprintf("/usr/local/codis-etcd/pika/pika-56382/dump/%s/db0/1/strings/", now.Format("20060102")))
		if err != nil {
			t.Error(err)
			return
		}
		if slaveSSTCount != masterDumpSSTCount {
			t.Errorf("slaveSSTCount(%d) != masterDumpSSTCount(%d)", slaveSSTCount, masterDumpSSTCount)
			return
		}
		t.Logf("ROUND %d: test ok, continue next round", i)
		time.Sleep(200 * time.Millisecond)
	}
}

func testReslaveOf(t *testing.T) (b bool) {
	clientMaster, err := NewClient("127.0.0.1:56382", "", 60*time.Second)
	if err != nil {
		t.Error(err.Error())
		return
	}
	clientSlave, err := NewClient("127.0.0.1:56381", "", 60*time.Second)
	if err != nil {
		t.Error(err)
		return
	}
	_ = clientSlave.ReconnectIfNeeded()
	if err := clientSlave.SlaveOf("no:one", 1, false, false); err != nil {
		t.Error(err)
		return
	}
	_ = clientSlave.ReconnectIfNeeded()
	if err := clientSlave.DeleteSlot(1); err != nil {
		t.Error(err)
		return
	}
	_ = clientSlave.ReconnectIfNeeded()
	if err := clientSlave.AddSlot(1); err != nil {
		t.Error(err)
		return
	}
	_ = clientSlave.ReconnectIfNeeded()
	if err := clientSlave.SlaveOf(clientMaster.Addr, 1, false, false); err != nil {
		t.Errorf(err.Error())
		return
	}

LoopFor:
	for {
		timeout := time.After(2500 * time.Millisecond)
		for {
			if slaveOfDone(clientMaster, clientSlave) {
				t.Logf("slave of done")
				return true
			}

			select {
			case <-timeout:
				_ = clientSlave.ReconnectIfNeeded()
				if err = clientSlave.SlaveOf("no:one", 1, false, false); err != nil {
					t.Errorf(err.Error())
					return
				}
				_ = clientSlave.ReconnectIfNeeded()
				if err := clientSlave.SlaveOf(clientMaster.Addr, 1, false, false); err != nil {
					t.Errorf(err.Error())
					return
				}
				continue LoopFor
			default:
				time.Sleep(100 * time.Millisecond)
			}
		}
	}
}

func slaveOfDone(clientMaster, clientSlave *Client) bool {
	_ = clientMaster.ReconnectIfNeeded()
	masterSlotInfo, err := clientMaster.SlotInfo(1)
	if err != nil {
		return false
	}
	slaveReplInfo, err := masterSlotInfo.FindSlaveReplInfo(clientSlave.Addr)
	if err != nil {
		return false
	}
	return slaveReplInfo.Status == pika.SlaveStatusBinlogSync && slaveReplInfo.Lag == 0
}

func countSSTFile(dir string) (count int, _ error) {
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return 0, err
	}
	for _, f := range files {
		if strings.HasSuffix(f.Name(), ".sst") {
			count++
		}
	}
	return count, nil
}
