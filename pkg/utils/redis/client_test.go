package redis

import (
	"os"
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
	clientMaster, err := NewClient("127.0.0.1:56382", "", time.Second)
	if err != nil {
		t.Error(err.Error())
		return
	}
	clientSlave, err := NewClient("127.0.0.1:56381", "", time.Second)
	if err != nil {
		t.Error(err)
		return
	}
	_ = clientSlave.ReconnectIfNeeded()
	if err := clientSlave.SlaveOf("no:one", 1, false, false); err != nil {
		t.Error(err)
	}
	_ = clientSlave.ReconnectIfNeeded()
	if err := clientSlave.DeleteSlot(1); err != nil {
		t.Error(err)
	}
	_ = clientSlave.ReconnectIfNeeded()
	if err := clientSlave.AddSlot(1); err != nil {
		t.Error(err)
	}

LoopFor:
	for {
		_ = clientSlave.ReconnectIfNeeded()
		err := clientSlave.SlaveOf(clientMaster.Addr, 1, false, false)
		if err != nil {
			t.Errorf(err.Error())
			return
		}

		timeout := time.After(2 * time.Second)
		for {
			if slaveOfDone(clientMaster, clientSlave) {
				t.Logf("slave of done")
				return
			}

			select {
			case <-timeout:
				//time.Sleep(2 * time.Second)
				continue LoopFor
			default:
				time.Sleep(200 * time.Millisecond)
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
