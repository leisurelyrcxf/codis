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
)

func (s *Topom) cleanupServersSlot(m *models.SlotMapping, cleanupAddrs []string, cleanupSlot int, masterAddr string, slaveAddrs []string) error {
	if err := s.action.redisp.UnlinkSlaves(masterAddr, slaveAddrs, m.Id, func(masterAddr string, _ int) (pika.SlotInfo, error) {
		return s.getMasterSlotInfo(m, masterAddr)
	}); err != nil {
		log.Errorf("[cleanupServersSlot] failed to unlink slaves of target master '%s': '%s'", masterAddr, err)
		return err
	}

	var err error
	for _, cleanupAddr := range cleanupAddrs {
		if cleanErr := s.action.redisp.CleanSlotIfExists(cleanupAddr, cleanupSlot); cleanErr != nil {
			log.Errorf("[cleanupServersSlot] slot-[%d] clean slot of slave %s failed: %v ", m.Id, cleanupAddr, cleanErr)
			err = errors.Wrap(err, cleanErr)
		}
	}
	return err
}

func (s *Topom) detachSlot(m *models.SlotMapping, masterAddr, slaveAddr string) error {
	if err := s.action.redisp.BecomeMaster(slaveAddr, m.Id); err != nil {
		return err
	}

	return utils.WithRetry(time.Millisecond*100, time.Second*2, func() error {
		if _, err := s.getSlaveReplInfo(m, masterAddr, slaveAddr); err != pika.ErrSlaveNotFound {
			if err == nil {
				err = errors.New("slave still exists on master")
			}
			return fmt.Errorf("slot-[%d] detach slave %s of master %s failed: %v", m.Id, slaveAddr, masterAddr, err)
		}
		return nil
	})
}

func (s *Topom) createReplLink(ctx *context, m *models.SlotMapping) error {
	return s.linkTargetSlaves(m, ctx.getGroupMasterSlaves(m.Action.TargetId), false)
}

func (s *Topom) assureTargetSlavesLinked(ctx *context, m *models.SlotMapping, targetMasterDetached bool) error {
	return s.linkTargetSlaves(m, ctx.getGroupSlaves(m.Action.TargetId), targetMasterDetached)
}

func (s *Topom) linkTargetSlaves(m *models.SlotMapping, slaveAddrs []string, targetMasterDetached bool) error {
	var (
		masterAddr string
		masterDesc string
	)
	if targetMasterDetached {
		masterAddr = m.Action.Info.TargetMaster
		masterDesc = "target"
	} else {
		masterAddr = m.Action.Info.SourceMaster
		masterDesc = "source"
	}

	masterSlotInfo, err := s.getMasterSlotInfo(m, masterAddr)
	if err != nil {
		return err
	}

	for _, slaveAddr := range slaveAddrs {
		if masterSlotInfo.IsSlaveLinked(slaveAddr) {
			continue // already linked
		}

		if err := func(slaveAddr string) error {
			if err := s.action.redisp.AddSlotIfNotExists(slaveAddr, m.Id); err != nil {
				return errors.Errorf("add slot failed: '%v'", err)
			}
			if err := s.action.redisp.SlaveOfAsync(masterAddr, slaveAddr, m.Id, false, m.Action.Resharding && masterAddr == m.Action.Info.SourceMaster); err != nil {
				return errors.Errorf("slaveof failed: '%v'", err)
			}
			return nil
		}(slaveAddr); err != nil {
			log.Errorf("[assureTargetSlavesLinked] slot-[%d] backup target slaves slot fail, target_slave(%s)->%s_master(%s), err: %v ", m.Id, slaveAddr, masterDesc, masterAddr, err)
		} else {
			log.Infof("[assureTargetSlavesLinked] slot-[%d] backup target slaves succeeded, target_slave(%s)->%s_master(%s)", m.Id, slaveAddr, masterDesc, masterAddr)
		}
	}
	return nil
}

func (s *Topom) backedUpSlot(ctx *context, m *models.SlotMapping, gap uint64, targetMasterDetached bool) error {
	var (
		masterAddr string
		masterDesc string
	)
	if targetMasterDetached {
		masterAddr = m.Action.Info.TargetMaster
		masterDesc = "target"
	} else {
		masterAddr = m.Action.Info.SourceMaster
		masterDesc = "source"
	}

	masterSlotInfo, err := s.getMasterSlotInfo(m, masterAddr)
	if err != nil {
		return err
	}

	targetSlaveAddrs := ctx.getGroupSlaves(m.Action.TargetId)
	if len(targetSlaveAddrs) == 0 { // intended no slaves
		return nil
	}

	if len(masterSlotInfo.LinkedSlaves(targetSlaveAddrs)) == 0 {
		return errors.Errorf("slot-[%d] no linked slaves among %v exists on %s master %s", m.Id, targetSlaveAddrs, masterDesc, masterAddr)
	}

	var errs []error
	for _, targetSlaveAddr := range targetSlaveAddrs {
		if err := masterSlotInfo.GapReached(targetSlaveAddr, gap); err != nil {
			errs = append(errs, err)
			continue
		}
		return nil // one is enough
	}
	for _, err := range errs {
		if strings.Contains(err.Error(), pika.ErrMsgLagNotMatch) {
			return errors.Errorf("slot-[%d] backup %s, %v min_lag(%d)>gap(%d)", m.Id, pika.ErrMsgLagNotMatch, targetSlaveAddrs, masterSlotInfo.GetMinReplLag(targetSlaveAddrs), gap)
		}
	}
	return errors.Errorf("slot-[%d] backup not ok, %v min_lag(%d)>gap(%d)", m.Id, targetSlaveAddrs, masterSlotInfo.GetMinReplLag(targetSlaveAddrs), gap)
}

func (s *Topom) GetSlotMigrationProgress(ctx *context, m *models.SlotMapping, rollbackTimes int, err error) models.SlotMigrationProgress {
	targetSlavesMaster := func() string {
		switch m.Action.State {
		case models.ActionPending, models.ActionPreparing, models.ActionWatching, models.ActionPrepared:
			return m.Action.Info.SourceMaster
		case models.ActionCleanup, models.ActionFinished:
			return m.Action.Info.TargetMaster
		default:
			return ""
		}
	}()

	p := models.NewSlotMigrationProgress(m.Action.Info.SourceMaster, targetSlavesMaster, rollbackTimes, err)
	if m.Action.State == models.ActionCleanup || m.Action.State == models.ActionFinished {
		p.Main.TargetMaster = models.SlaveReplProgress{Addr: m.Action.Info.TargetMaster, Progress: "detached"}
	} else {
		replInfo, err := s.getSlaveReplInfo(m, m.Action.Info.SourceMaster, m.Action.Info.TargetMaster)
		p.Main.TargetMaster = models.NewSlaveReplProgress(replInfo, err)
	}
	if targetSlavesMasterSlotInfo, err := s.getMasterSlotInfo(m, targetSlavesMaster); err == nil {
		for _, targetSlave := range ctx.getGroupSlaves(m.Action.TargetId) {
			targetSlaveReplInfo, err := targetSlavesMasterSlotInfo.FindSlaveReplInfo(targetSlave)
			p.Backup.TargetSlaves = append(p.Backup.TargetSlaves, models.NewSlaveReplProgress(targetSlaveReplInfo, err))
		}
	}
	return p
}

func (s *Topom) getSlaveReplInfo(m *models.SlotMapping, masterAddr, slaveAddr string) (pika.SlaveReplInfo, error) {
	masterSlotInfo, err := s.getMasterSlotInfo(m, masterAddr)
	if err != nil {
		return pika.InvalidSlaveReplInfo(slaveAddr), err
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
	return pika.InvalidSlotInfo, fmt.Errorf("master '%s' neither SourceMaster %s nor TargetMaster %s", masterAddr, m.Action.Info.SourceMaster, m.Action.Info.TargetMaster)
}

func (s *Topom) getSourceMasterSlotInfo(m *models.SlotMapping) (pika.SlotInfo, error) {
	sourceMasterSlotInfo, err := s.action.redisp.GetPikaSlotInfo(m.Action.Info.SourceMaster, m.GetSourceSlot())
	if err != nil {
		return pika.InvalidSlotInfo, errors.Errorf("slot-[%d], can't find source master %s slot info: '%v'", m.Id, m.Action.Info.SourceMaster, err)
	}
	return sourceMasterSlotInfo, nil
}

func (s *Topom) getTargetMasterSlotInfo(m *models.SlotMapping) (pika.SlotInfo, error) {
	targetMasterSlotInfo, err := s.action.redisp.GetPikaSlotInfo(m.Action.Info.TargetMaster, m.Id)
	if err != nil {
		return pika.InvalidSlotInfo, errors.Errorf("slot-[%d], can't find target master %s slot info: '%v'", m.Id, m.Action.Info.TargetMaster, err)
	}
	return targetMasterSlotInfo, nil
}
