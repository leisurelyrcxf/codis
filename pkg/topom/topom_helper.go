package topom

import (
	"fmt"
	"strings"
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

func (s *Topom) getPikaSlotsInfo(addr string) (slotsInfo map[int]pika.SlotInfo, _ error) {
	return slotsInfo, s.withRedisClient(addr, func(client *redis.Client) (err error) {
		slotsInfo, err = client.PkSlotsInfo()
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

func (s *Topom) slaveOfAsync(masterAddr, slaveAddr string, slot int, force bool) error {
	return s.withRedisClient(slaveAddr, func(client *redis.Client) error {
		return client.SlaveOf(masterAddr, slot, force)
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

func (s *Topom) slaveOfAsyncSM(m *models.SlotMapping, masterAddr, slaveAddr string, force bool) error {
	err := s.slaveOfAsync(masterAddr, slaveAddr, m.Id, force)
	m.ClearCachedSlotInfo(masterAddr, slaveAddr)
	return err
}

func (s *Topom) cleanupSlotsOfGroup(ctx *context, m *models.SlotMapping, groupID int) error {
	masterAddr, slaveAddrs := ctx.getGroupMaster(groupID), ctx.getGroupSlaves(groupID)

	if err := s.unlinkSlaves(m, masterAddr); err != nil {
		log.Errorf("[cleanupSlotsOfGroup] failed to unlink slaves of target master '%s': '%s'", masterAddr, err)
		return err
	}

	for _, slaveAddr := range slaveAddrs {
		if err := s.cleanSlotSM(m, slaveAddr); err != nil {
			log.Errorf("[cleanupSlotsOfGroup] slot-[%d] clean slot of slave %s failed: %v ", m.Id, slaveAddr, err)
		}
	}
	err := s.cleanSlotSM(m, masterAddr)
	if err != nil {
		log.Errorf("[cleanupSlotsOfGroup] slot-[%d] clean slot of master %s failed: %v ", m.Id, masterAddr, err)
	}
	return err
}

func (s *Topom) unlinkSlaves(m *models.SlotMapping, masterAddr string) error {
	masterSlotInfo, err := s.getMasterSlotInfo(m, masterAddr)
	if err != nil {
		return err
	}
	if len(masterSlotInfo.SlaveReplInfos) == 0 {
		return nil
	}

	for _, slaveReplInfo := range masterSlotInfo.SlaveReplInfos {
		if err := s.detachSlotAsyncSM(m, masterAddr, slaveReplInfo.Addr); err != nil {
			log.Errorf("[unlinkSlaves] slot-[%d] detach source slave slot fail, slave address:%v: '%v'", m.Id, slaveReplInfo.Addr, err)
		}
	}
	if err = utils.WithRetry(time.Millisecond*100, time.Second*2, func() error {
		masterSlotInfo, err := s.getMasterSlotInfo(m, masterAddr)
		if err != nil {
			m.ClearCachedSlotInfo(masterAddr)
			return err
		}
		if len(masterSlotInfo.SlaveReplInfos) == 0 {
			return nil
		}
		m.ClearCachedSlotInfo(masterAddr)
		return errors.Errorf("slot-[%d] slaves of master %s not all unlinked", m.Id, masterAddr)
	}); err != nil {
		log.Errorf("[unlinkSlaves] slot-[%d] detach failed: '%v'", m.Id, err)
	}
	return err
}

func (s *Topom) detachSlotAsyncSM(m *models.SlotMapping, masterAddr, slaveAddr string) error {
	err := s.slaveOfAsync("no:one", slaveAddr, m.Id, false)
	m.ClearCachedSlotInfo(masterAddr, slaveAddr)
	return err
}

func (s *Topom) detachSlotSM(m *models.SlotMapping, masterAddr, slaveAddr string) error {
	if err := s.detachSlotAsyncSM(m, masterAddr, slaveAddr); err != nil {
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

	if err := s.slaveOfAsyncSM(m, masterAddress, slaveAddress, false); err != nil {
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

func (s *Topom) backedUpSlot(ctx *context, m *models.SlotMapping, gap uint64) error {
	targetMasterSlotInfo, err := s.getTargetMasterSlotInfo(m)
	if err != nil {
		return err
	}

	targetSlaveAddrs := ctx.getGroupSlaves(m.Action.TargetId)
	if len(targetSlaveAddrs) == 0 { // intended no slaves
		return nil
	}

	if len(targetMasterSlotInfo.SlaveReplInfos) == 0 {
		return errors.Errorf("slot-[%d] no linked slave exists on target master %s", m.Id, m.Action.Info.TargetMaster)
	}

	if len(targetMasterSlotInfo.SyncedSlaves()) == 0 {
		return errors.Errorf("slot-[%d] no synced slaves exists on target master %s", m.Id, m.Action.Info.TargetMaster)
	}

	var errs []error
	for _, targetSlaveAddr := range targetSlaveAddrs {
		slaveReplInfo, err := targetMasterSlotInfo.FindSlaveReplInfo(targetSlaveAddr)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if err = slaveReplInfo.GapReached(gap); err == nil {
			return nil // at least one is enough
		}
		errs = append(errs, err)
	}
	for _, err := range errs {
		if strings.Contains(err.Error(), pika.ErrMsgLagNotMatch) {
			return errors.Errorf("slot-[%d] backup %s, min_lag(%d)>gap(%d)", m.Id, pika.ErrMsgLagNotMatch, targetMasterSlotInfo.GetMinReplLag(), gap)
		}
	}
	return errors.Errorf("slot-[%d] backup not ok, min_lag(%d)>gap(%d)", m.Id, targetMasterSlotInfo.GetMinReplLag(), gap)
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
		return pika.InvalidSlaveReplInfo, err
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
	if m.Action.Info.SourceMasterSlotInfo == nil || m.Action.Info.SourceMasterSlotInfo.IsExpired() {
		sourceMasterSlotInfo, err := s.getPikaSlotInfo(m.Action.Info.SourceMaster, m.Id)
		if err != nil {
			return pika.SlotInfo{}, errors.Errorf("slot-[%d], can't find source master %s slot info: '%v'", m.Id, m.Action.Info.SourceMaster, err)
		}
		m.Action.Info.SourceMasterSlotInfo = models.NewCachedSlotInfo(sourceMasterSlotInfo, time.Second)
	}
	return m.Action.Info.SourceMasterSlotInfo.SlotInfo, nil
}

func (s *Topom) getTargetMasterSlotInfo(m *models.SlotMapping) (pika.SlotInfo, error) {
	if m.Action.Info.TargetMasterSlotInfo == nil || m.Action.Info.TargetMasterSlotInfo.IsExpired() {
		targetMasterSlotInfo, err := s.getPikaSlotInfo(m.Action.Info.TargetMaster, m.Id)
		if err != nil {
			return pika.SlotInfo{}, errors.Errorf("slot-[%d], can't find target master %s slot info: '%v'", m.Id, m.Action.Info.TargetMaster, err)
		}
		m.Action.Info.TargetMasterSlotInfo = models.NewCachedSlotInfo(targetMasterSlotInfo, time.Second)
	}
	return m.Action.Info.TargetMasterSlotInfo.SlotInfo, nil
}
