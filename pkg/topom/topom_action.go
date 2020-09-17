// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package topom

import (
	"fmt"
	"strings"
	"time"

	"github.com/CodisLabs/codis/pkg/utils/errors"

	"github.com/CodisLabs/codis/pkg/models"
	"github.com/CodisLabs/codis/pkg/utils/log"
	"github.com/CodisLabs/codis/pkg/utils/math2"
)

func (s *Topom) ProcessSlotAction() error {
	for s.IsOnline() {
		var (
			marks = make(map[int]bool)
			plans = make(map[int]bool)
		)
		var accept = func(m *models.SlotMapping) bool {
			if marks[m.GroupId] || marks[m.Action.TargetId] {
				return false
			}
			if plans[m.Id] {
				return false
			}
			return true
		}
		var update = func(m *models.SlotMapping) bool {
			if m.GroupId != 0 {
				marks[m.GroupId] = true
			}
			marks[m.Action.TargetId] = true
			plans[m.Id] = true
			return true
		}
		var parallel = math2.MaxInt(1, s.config.MigrationParallelSlots)
		for parallel > len(plans) {
			_, ok, err := s.SlotActionPrepareFilter(accept, update)
			if err != nil {
				return err
			} else if !ok {
				break
			}
		}
		if len(plans) == 0 {
			return nil
		}
		slots := make([]int, 0, len(plans))
		for slot := range plans {
			slots = append(slots, slot)
		}
		log.Infof("[ProcessSlotAction] plan to process action for slots: %v", slots)
		errs := make([]error, 0, len(plans))
		for sid := range plans {
			f := func(sid int) (err error) {
				s.mu.Lock()
				defer s.mu.Unlock()

				ctx, err := s.newContext()
				if err != nil {
					return err
				}
				m, err := ctx.getSlotMapping(sid)
				if err != nil {
					return err
				}

				switch m.Action.State {
				case models.ActionPending:
					if err = s.transitionSlotState(ctx, m, models.ActionPreparing, VoidAction); err != nil {
						return err
					}
					fallthrough
				case models.ActionPreparing:
					if err = s.transitionSlotState(ctx, m, models.ActionWatching, s.preparingSlot); err != nil {
						return err
					}
					fallthrough
				case models.ActionWatching:
					if err = s.transitionSlotState(ctx, m, models.ActionPrepared, s.watchSlot); err != nil {
						return err
					}
					fallthrough
				case models.ActionPrepared: // writing stopped
					if err = s.transitionSlotState(ctx, m, models.ActionCleanup, s.preparedSlot); err != nil {
						return err
					}
					fallthrough
				case models.ActionCleanup: // writing stopped
					if err := s.transitionSlotState(ctx, m, models.ActionFinished, s.cleanupSlot); err != nil {
						return err
					}
					fallthrough
				case models.ActionFinished:
					err := s.transitionSlotStateRaw(ctx, m, func(m *models.SlotMapping) {
						m.ClearAction()
					}, VoidAction)
					if err == nil {
						s.action.slotsProgress[m.Id] = models.SlotMigrationProgress{}
					}
					return err
				default:
					return errors.Errorf("slot-[%d] action state '%s' is invalid", m.Id, m.Action.State)
				}
			}
			errs = append(errs, f(sid))
		}
		for _, err := range errs {
			if err != nil {
				return err
			}
		}
		time.Sleep(time.Millisecond * 10)
	}
	return nil
}

// Deprecated: there is no state of ActionMigrating any more.
func (s *Topom) processSlotAction(sid int) error {
	var db int
	for s.IsOnline() {
		if exec, err := s.newSlotActionExecutor(sid); err != nil {
			return err
		} else if exec == nil {
			time.Sleep(time.Second)
		} else {
			n, nextdb, err := exec(db)
			if err != nil {
				return err
			}
			log.Debugf("slot-[%d] action executor %d", sid, n)

			if n == 0 && nextdb == -1 {
				return s.SlotActionComplete(sid)
			}
			status := fmt.Sprintf("[OK] Slot[%04d]@DB[%d]=%d", sid, db, n)
			s.action.progress.status.Store(status)

			if us := s.GetSlotActionInterval(); us != 0 {
				time.Sleep(time.Microsecond * time.Duration(us))
			}
			db = nextdb
		}
	}
	return nil
}

func (s *Topom) transitionSlotState(ctx *context, m *models.SlotMapping,
	nextState string,
	action func(*context, *models.SlotMapping) error) error {
	return s.transitionSlotStateRaw(ctx, m, func(m *models.SlotMapping) {
		m.Action.State = nextState
	}, action)
}

func (s *Topom) transitionSlotStateRaw(ctx *context, m *models.SlotMapping,
	update func(*models.SlotMapping),
	action func(*context, *models.SlotMapping) error) (err error) {
	if err = s.transitionSlotStateInternal(ctx, m, update, action); err != nil &&
		!strings.Contains(err.Error(), errMsgLagNotMatch) && // Don't expose lag error to user.
		!(m.Action.State == models.ActionWatching &&
			strings.Contains(err.Error(), errMsgReplLinkNotOK) &&
			time.Since(m.GetStateStart()) < watchReplLinkOKTimeout) { // Don't expose repl link not ok in watch state to user
		om := *m
		update(m)
		nm := *m
		*m = om
		log.Errorf("[transitionSlotStateRaw] slot-%d transition from %s to %s failed, err: %v", m.Id, &om, &nm, errors.Trace(err))
		if strings.Contains(err.Error(), errMsgRollback) {
			s.action.slotsProgress[m.Id] = models.NewSlotMigrationProgress("", "", err)
		} else {
			s.action.slotsProgress[m.Id] = s.GetSlotMigrationProgress(m, err)
		}
	} else {
		s.action.slotsProgress[m.Id] = s.GetSlotMigrationProgress(m, nil)
	}
	return err
}

func (s *Topom) transitionSlotStateInternal(ctx *context, m *models.SlotMapping,
	update func(m *models.SlotMapping),
	action func(ctx *context, m *models.SlotMapping) error) (err error) {
	m.Action.Info.SourceMasterSlotInfo = nil
	m.Action.Info.TargetMasterSlotInfo = nil

	original := *m
	for _, prerequisite := range s.actionPrerequisites() {
		if prerequisiteErr, needsRollback := prerequisite(ctx, m); prerequisiteErr != nil {
			if needsRollback {
				return s.rollbackStateToPreparing(ctx, m, nil, prerequisiteErr)
			}
			return prerequisiteErr
		}
	}

	if actionErr := action(ctx, m); actionErr != nil {
		*m = original
		log.Errorf("[transitionSlotStateInternal] slot-[%d] action of slot %s failed, err: '%v'", m.Id, m, actionErr)
		return actionErr
	}

	return s.updateSlotMappings(ctx, m, func(m *models.SlotMapping) {
		update(m)
		if m.Action.State != models.ActionNothing {
			m.UpdateStateStart()
		}
		log.Warnf("[transitionSlotStateInternal] slot-[%d] updating from %s to %s...", m.Id, &original, m)
	}, &original)
}

func (s *Topom) rollbackStateToPreparing(ctx *context, m *models.SlotMapping, original *models.SlotMapping, reason error) error {
	updateErr := s.updateSlotMappings(ctx, m, func(m *models.SlotMapping) {
		log.Warnf("[rollbackStateToPreparing] slot-[%d] rollback to 'preparing', reason: '%v'", m.Id, reason)
		m.Action.State = models.ActionPreparing
		m.ClearActionInfo()
		m.UpdateStateStart()
	}, original)
	if updateErr != nil {
		log.Errorf("[rollbackStateToPreparing] slot-[%d] rollback to 'preparing' failed: '%v', rollback reason: '%v'", m.Id, updateErr, reason)
		return errors.Errorf("slot-[%d] RollbackErr: '%v', RollbackReason '%v'", m.Id, updateErr, reason)
	}
	log.Errorf("[rollbackStateToPreparing] slot-[%d] %s due to error: '%v'", m.Id, errMsgRollback, reason)
	return errors.Errorf("slot-[%d] %s due to error: '%v'", m.Id, errMsgRollback, reason)
}

func (s *Topom) updateSlotMappings(ctx *context, m *models.SlotMapping, update func(m *models.SlotMapping), original *models.SlotMapping) (err error) {
	if original == nil {
		om := *m
		original = &om
	}

	update(m)
	defer func() {
		s.dirtySlotsCache(m.Id)

		if err != nil {
			*m = *original
			log.Warnf("[updateSlotMappings] slot-[%d] rollback proxies' slot mapping to %v...", m.Id, m)
			if rollbackErr := s.resyncSlotMappings(ctx, m); rollbackErr != nil {
				log.Warnf("[updateSlotMappings] slot-[%d] rollback proxies' slot mapping to %v failed, error: '%v'", m.Id, m, rollbackErr)
				return
			}
			log.Warnf("[updateSlotMappings] slot-[%d] rollback proxies' slot mapping to %v done", m.Id, m)
		}
	}()
	if err = s.resyncSlotMappings(ctx, m); err != nil {
		log.Errorf("[updateSlotMappings] slot-[%d] resync slot mappings failed, err: '%v'", m.Id, err)
		return err
	}
	if err = s.storeUpdateSlotMapping(m); err != nil {
		log.Errorf("[updateSlotMappings] slot-[%d] store update mappings failed, err: '%v'", m.Id, err)
	}
	return err
}

func (s *Topom) ProcessSyncAction() error {
	addr, err := s.SyncActionPrepare()
	if err != nil || addr == "" {
		return err
	}
	log.Warnf("sync-[%s] process action", addr)

	exec, err := s.newSyncActionExecutor(addr)
	if err != nil || exec == nil {
		return err
	}
	return s.SyncActionComplete(addr, exec() != nil)
}

type Prerequisite func(*context, *models.SlotMapping) (_ error, needsRollback bool)

func (s *Topom) actionPrerequisites() []Prerequisite {
	return prerequisitesWrapper([]Prerequisite{
		s.prerequisiteActionEnabled,
		s.prerequisiteSourceGroupNotPromoting,
		s.prerequisiteTargetGroupNotPromoting,
		s.prerequisiteSourceMasterHolds,
		s.prerequisiteTargetMasterHolds,
		s.prerequisiteReplLinkOK,
		s.prerequisiteAssureTargetSlavesLinked,
		s.prerequisiteCleanup,
	})
}

func prerequisitesWrapper(prerequisites []Prerequisite) []Prerequisite {
	ret := make([]Prerequisite, 0, len(prerequisites))
	for _, f := range prerequisites {
		ret = append(ret, prerequisiteWrapper(f))
	}
	return ret
}

func prerequisiteWrapper(prerequisite Prerequisite) Prerequisite {
	return func(ctx *context, m *models.SlotMapping) (error, bool) {
		switch m.Action.State {
		case models.ActionNothing, models.ActionPending, models.ActionPreparing, models.ActionFinished:
			return nil, false // rollback ActionFinished is unsafe, rollback other states is meaningless
		}
		return prerequisite(ctx, m)
	}
}

func (s *Topom) prerequisiteActionEnabled(_ *context, m *models.SlotMapping) (_ error, needsRollback bool) {
	if s.action.disabled.IsFalse() {
		return nil, false
	}
	return errors.Errorf("slot-[%d] topom action disabled", m.Id), false
}

func (s *Topom) prerequisiteSourceGroupNotPromoting(ctx *context, m *models.SlotMapping) (_ error, needsRollback bool) {
	if m.Action.State == models.ActionCleanup {
		return nil, false // Don't care for cleanup because already detached
	}

	if !ctx.isGroupPromoting(m.GroupId) {
		return nil, false
	}
	return errors.Errorf("slot-[%d] Source group-[%d] is promoting", m.Id, m.GroupId), false // don't rollback because promotion doesn't guarantee success
}

func (s *Topom) prerequisiteTargetGroupNotPromoting(ctx *context, m *models.SlotMapping) (_ error, needsRollback bool) {
	if !ctx.isGroupPromoting(m.Action.TargetId) {
		return nil, false
	}
	return errors.Errorf("slot-[%d] Target group-[%d] is promoting", m.Id, m.Action.TargetId), false // don't rollback because promotion doesn't guarantee success
}

func (s *Topom) prerequisiteSourceMasterHolds(ctx *context, m *models.SlotMapping) (_ error, needsRollback bool) {
	if m.Action.State == models.ActionCleanup {
		return nil, false // Don't care for cleanup because already detached
	}

	currentSourceMaster := ctx.getGroupMaster(m.GroupId)
	if currentSourceMaster == m.Action.Info.SourceMaster {
		return nil, false
	}
	return errors.Errorf("source group %d master switched from %s to %s",
		m.GroupId, m.Action.Info.SourceMaster, currentSourceMaster), true
}

func (s *Topom) prerequisiteTargetMasterHolds(ctx *context, m *models.SlotMapping) (_ error, needsRollback bool) {
	currentTargetMaster := ctx.getGroupMaster(m.Action.TargetId)
	if currentTargetMaster == m.Action.Info.TargetMaster {
		return nil, false
	}
	return errors.Errorf("target group %d master switched from %s to %s",
		m.Action.TargetId, m.Action.Info.TargetMaster, currentTargetMaster), true
}

func (s *Topom) prerequisiteReplLinkOK(_ *context, m *models.SlotMapping) (_ error, needsRollback bool) {
	if m.Action.Info.SourceMaster == "" || m.Action.Info.TargetMaster == "" {
		return nil, false
	}

	if m.Action.State == models.ActionCleanup {
		return nil, false // Don't care for cleanup because already detached
	}

	waitPeriod := s.GetSlotActionRollbackWaitPeriod()
	if m.Action.State == models.ActionWatching { // needs extra time because repl linked is just created
		waitPeriod = waitPeriod + watchReplLinkOKTimeout
	}
	if _, err := s.getSlaveReplInfo(m, m.Action.Info.SourceMaster, m.Action.Info.TargetMaster); err != nil {
		log.Errorf("[prerequisiteReplLinkOK] repl link target_master(%s)->source_master(%s) not ok, err: '%v', "+
			"time since state start: %s, wait period: %s", m.Action.Info.TargetMaster, m.Action.Info.SourceMaster, err, time.Since(m.GetStateStart()), waitPeriod)
		return errors.Errorf("%s: %v", errMsgReplLinkNotOK, err), time.Since(m.GetStateStart()) > waitPeriod
	}
	return nil, false
}

func (s *Topom) prerequisiteAssureTargetSlavesLinked(ctx *context, m *models.SlotMapping) (_ error, needsRollback bool) {
	if m.Action.Info.SourceMaster == "" || m.Action.Info.TargetMaster == "" {
		return nil, false
	}

	if m.Action.State == models.ActionCleanup {
		return nil, false
	}

	if err := s.assureTargetSlavesLinked(ctx, m); err != nil {
		return err, time.Since(m.GetStateStart()) > s.GetSlotActionRollbackWaitPeriod() // rollback if target master down
	}
	return nil, false
}

func (s *Topom) prerequisiteCleanup(ctx *context, m *models.SlotMapping) (_ error, needsRollback bool) {
	if m.Action.Info.SourceMaster == "" || m.Action.Info.TargetMaster == "" {
		return nil, false
	}
	if m.Action.State != models.ActionCleanup {
		return nil, false
	}

	if err := s.assureTargetSlavesLinked(ctx, m); err != nil {
		return err, time.Since(m.GetStateStart()) > s.GetSlotActionRollbackWaitPeriod() // rollback if target master down
	}
	return nil, false
	// guarantee safety after cleaned up, is a v2.0.0 feature
	//if err := s.backedUpSlot(ctx, m, 2*s.GetSlotActionGap()); err != nil {
	//	return err, time.Since(m.GetStateStart()) > s.GetSlotActionRollbackWaitPeriod() // rollback if gap violated, otherwise wait time is uncertain.
	//}
	//return s.backedUpSlot(ctx, m, 0), false
}
