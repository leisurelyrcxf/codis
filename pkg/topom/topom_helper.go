package topom

import (
	"fmt"
	"time"

	"github.com/CodisLabs/codis/pkg/utils"
	"github.com/CodisLabs/codis/pkg/utils/log"
	"github.com/CodisLabs/codis/pkg/utils/pika"
	"github.com/CodisLabs/codis/pkg/utils/redis"
)

func (s *Topom) withRedisClient(addr string, do func(*redis.Client) error) error {
	cli, err := s.action.redisp.GetClient(addr)
	if err != nil {
		return err
	}
	defer s.action.redisp.PutClient(cli)

	return do(cli)
}

func (s *Topom) getPikaSlotInfo(addr string, slot int) (slotInfo pika.SlotInfo, _ error) {
	return slotInfo, s.withRedisClient(addr, func(client *redis.Client) (err error) {
		slotInfo, err = client.SlotInfo(slot)
		return
	})
}

func (s *Topom) addSlotIfNotExists(addr string, slot int) error {
	err := s.withRedisClient(addr, func(client *redis.Client) error {
		return client.AddSlot(slot)
	})
	if err == nil {
		return nil
	}
	if _, getErr := s.getPikaSlotInfo(addr, slot); getErr == nil {
		return nil // error pruning
	}
	return err
}

// Delete slot if exists
func (s *Topom) cleanSlot(addr string, slot int) error {
	err := s.withRedisClient(addr, func(client *redis.Client) error {
		return client.DeleteSlot(slot)
	})
	if err == nil {
		return nil
	}
	if _, getErr := s.getPikaSlotInfo(addr, slot); getErr == pika.ErrSlotNotExists {
		return nil // error pruning
	}
	return err
}

func (s *Topom) slaveOfAsync(masterAddr, slaveAddr string, slot int) error {
	return s.withRedisClient(slaveAddr, func(client *redis.Client) error {
		return client.SlaveOf(masterAddr, slot)
	})
}

func (s *Topom) detachSlot(masterAddr, slaveAddr string, slot int) error {
	if err := s.slaveOfAsync("no:one", slaveAddr, slot); err != nil {
		return err
	}

	return utils.WithRetry(time.Millisecond*100, time.Second*2, func() error {
		slotInfo, err := s.getPikaSlotInfo(masterAddr, slot)
		if err != nil {
			return err
		}

		if slaveReplInfo := slotInfo.FindSlaveReplInfo(slaveAddr); slaveReplInfo != nil {
			return fmt.Errorf("slave %s found on master %s", slaveAddr, masterAddr)
		}
		return nil
	})
}

func (s *Topom) backupSlot(masterAddress, slaveAddress string, slot int) error {
	if err := s.addSlotIfNotExists(slaveAddress, slot); err != nil {
		log.Errorf("[backupSlot] slot-[%d] failed to add slot on target %s, err: '%v'", slot, slaveAddress, err)
		return err
	}

	if err := s.slaveOfAsync(masterAddress, slaveAddress, slot); err != nil {
		log.Errorf("[backupSlot] slot-[%d] failed to slaveof %s for target %s, err: '%v'", slot, masterAddress, slaveAddress, err)
		return err
	}

	return nil
}

func (s *Topom) getReplSlaveInfo(masterAddr, slaveAddr string, slot int) (*pika.SlaveReplInfo, error) {
	slotInfo, err := s.getPikaSlotInfo(masterAddr, slot)
	if err != nil {
		return nil, err
	}

	slaveReplInfo := slotInfo.FindSlaveReplInfo(slaveAddr)
	if slaveReplInfo == nil {
		return nil, fmt.Errorf("slave %s not found on master %s", slaveAddr, masterAddr)
	}
	return slaveReplInfo, nil
}
