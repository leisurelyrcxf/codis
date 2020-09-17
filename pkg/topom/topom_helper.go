package topom

import (
	"fmt"
	"time"

	"github.com/CodisLabs/codis/pkg/models"
	"github.com/CodisLabs/codis/pkg/utils/errors"

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

func (s *Topom) addSlotIfNotExistsSM(m *models.SlotMapping, addr string) error {
	err := s.addSlotIfNotExists(addr, m.Id)
	m.ClearCachedSlotInfo(addr)
	return err
}

// Delete slot if exists
func (s *Topom) cleanSlotSM(m *models.SlotMapping, addr string) error {
	err := s.cleanSlot(addr, m.Id)
	m.ClearCachedSlotInfo(addr)
	return err
}

func (s *Topom) slaveOfAsyncSM(m *models.SlotMapping, masterAddr, slaveAddr string) error {
	err := s.slaveOfAsync(masterAddr, slaveAddr, m.Id)
	m.ClearCachedSlotInfo(masterAddr, slaveAddr)
	return err
}

func (s *Topom) detachSlotAsync(m *models.SlotMapping, masterAddr, slaveAddr string) error {
	err := s.slaveOfAsync("no:one", slaveAddr, m.Id)
	m.ClearCachedSlotInfo(masterAddr, slaveAddr)
	return err
}

func (s *Topom) detachSlot(m *models.SlotMapping, masterAddr, slaveAddr string) error {
	if err := s.detachSlotAsync(m, masterAddr, slaveAddr); err != nil {
		return err
	}

	return utils.WithRetry(time.Millisecond*100, time.Second*2, func() error {
		if _, err := s.getSlaveReplInfo(m, masterAddr, slaveAddr); err != pika.ErrSlaveNotFound {
			m.ClearCachedSlotInfo(masterAddr)
			return fmt.Errorf("slot-[%d] detach failed: slave %s found on master %s", m.Id, slaveAddr, masterAddr)
		}
		return nil
	})
}

func (s *Topom) backupSlotAsync(m *models.SlotMapping, masterAddress, slaveAddress string) error {
	if err := s.addSlotIfNotExistsSM(m, slaveAddress); err != nil {
		log.Errorf("[backupSlotAsync] slot-[%d] failed to add slot on target %s, err: '%v'", m.Id, slaveAddress, err)
		return err
	}

	if err := s.slaveOfAsyncSM(m, masterAddress, slaveAddress); err != nil {
		log.Errorf("[backupSlotAsync] slot-[%d] failed to slaveof %s for target %s, err: '%v'", m.Id, masterAddress, slaveAddress, err)
		return err
	}

	return nil
}

func (s *Topom) assureTargetSlavesLinked(ctx *context, m *models.SlotMapping) error {
	targetMasterSlotInfo, err := s.getTargetMasterSlotInfo(m)
	if err != nil {
		return err
	}

	targetMasterAddr := m.Action.Info.TargetMaster
	targetSlaveAddrs := ctx.getGroupSlaves(m.Action.TargetId)
	for _, targetSlaveAddr := range targetSlaveAddrs {
		if _, err := targetMasterSlotInfo.FindSlaveReplInfo(targetSlaveAddr); err != nil {
			if err := s.backupSlotAsync(m, targetMasterAddr, targetSlaveAddr); err != nil { // pika support linked replication
				log.Errorf("[assureTargetSlavesLinked] slot-[%d] backup target slaves slot fail, target master:%v, target slave: %v, err: %v ", m.Id, targetMasterAddr, targetSlaveAddr, err)
			} else {
				log.Infof("[assureTargetSlavesLinked] slot-[%d] backup target slaves succeeded, target_slave(%s)->target_master(%s)", m.Id, targetSlaveAddr, targetMasterAddr)
			}
		}
	}
	return nil
}

func (s *Topom) compareMasterSlave(m *models.SlotMapping, masterAddr, slaveAddr string, gap uint64,
	onCompareSlotOK func(sourceMaster, targetMaster string) error) error {
	slaveReplInfo, err := s.getSlaveReplInfo(m, masterAddr, slaveAddr)
	if err != nil {
		return err
	}

	if slaveReplInfo.Status != pika.SlaveStatusBinlogSync {
		return errors.Errorf("[%sSlot] slot-[%d] slave status not match, exp: %s, actual: %s", m.Action.State, m.Id, pika.SlaveStatusBinlogSync, slaveReplInfo.Status)
	}

	if slaveReplInfo.Lag > gap {
		return errors.Errorf("[%sSlot] slot-[%d] %s,lag(%d)>gap(%d)", m.Action.State, m.Id, errMsgLagNotMatch, slaveReplInfo.Lag, gap)
	}
	log.Infof("[%sSlot] slot-[%d] success, gap reached %d.", m.Action.State, m.Id, gap)
	return onCompareSlotOK(masterAddr, slaveAddr)
}

func (s *Topom) GetSlotMigrationProgress(m *models.SlotMapping, err error) models.SlotMigrationProgress {
	p := models.NewSlotMigrationProgress(m.Action.Info.SourceMaster, m.Action.Info.TargetMaster, err)

	if replInfo, err := s.getSlaveReplInfo(m, m.Action.Info.SourceMaster, m.Action.Info.TargetMaster); err == nil {
		p.Main.TargetMaster = &replInfo
	}
	if targetMasterSlotInfo, err := s.getTargetMasterSlotInfo(m); err == nil {
		p.Backup.TargetSlaves = targetMasterSlotInfo.SlaveReplInfos
	}
	return p
}

func (s *Topom) getSlaveReplInfo(m *models.SlotMapping, masterAddr, slaveAddr string) (pika.SlaveReplInfo, error) {
	masterSlotInfo, err := s.getMasterSlotInfo(m, masterAddr)
	if err != nil {
		return pika.SlaveReplInfo{}, err
	}
	return masterSlotInfo.FindSlaveReplInfo(slaveAddr)
}

func (s *Topom) getMasterSlotInfo(m *models.SlotMapping, masterAddr string) (pika.SlotInfo, error) {
	if masterAddr == m.Action.Info.SourceMaster {
		return s.getSourceMasterSlotInfo(m)
	}
	if masterAddr == m.Action.Info.TargetMaster {
		return s.getTargetMasterSlotInfo(m)
	}
	return pika.SlotInfo{}, fmt.Errorf("master %s neither SourceMaster %s nor TargetMaster %s", masterAddr, m.Action.Info.SourceMaster, m.Action.Info.TargetMaster)
}

func (s *Topom) getSourceMasterSlotInfo(m *models.SlotMapping) (pika.SlotInfo, error) {
	if m.Action.Info.SourceMasterSlotInfo == nil {
		sourceMasterSlotInfo, err := s.getPikaSlotInfo(m.Action.Info.SourceMaster, m.Id)
		if err != nil {
			return pika.SlotInfo{}, errors.Errorf("slot-[%d], can't find source master %s slot info: '%v'", m.Id, m.Action.Info.SourceMaster, err)
		}
		m.Action.Info.SourceMasterSlotInfo = &sourceMasterSlotInfo
	}
	return *m.Action.Info.SourceMasterSlotInfo, nil
}

func (s *Topom) getTargetMasterSlotInfo(m *models.SlotMapping) (pika.SlotInfo, error) {
	if m.Action.Info.TargetMasterSlotInfo == nil {
		targetMasterSlotInfo, err := s.getPikaSlotInfo(m.Action.Info.TargetMaster, m.Id)
		if err != nil {
			return pika.SlotInfo{}, errors.Errorf("slot-[%d], can't find target master %s slot info: '%v'", m.Id, m.Action.Info.TargetMaster, err)
		}
		m.Action.Info.TargetMasterSlotInfo = &targetMasterSlotInfo
	}
	return *m.Action.Info.TargetMasterSlotInfo, nil
}
