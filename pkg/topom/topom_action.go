// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package topom

import (
	"fmt"
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
				case models.ActionPrepared:
					if err = s.transitionSlotState(ctx, m, models.ActionCleanup, s.preparedSlot); err != nil {
						return err
					}
					fallthrough
				case models.ActionCleanup:
					if err := s.transitionSlotState(ctx, m, models.ActionFinished, s.cleanupSlot); err != nil {
						return err
					}
					fallthrough
				case models.ActionFinished:
					return s.transitionSlotStateRaw(ctx, m, func(m *models.SlotMapping) {
						m.ClearAction()
					}, VoidAction)
				default:
					return errors.Errorf("slot-[%d] action state '%s' is invalid", m.Id, m.Action.State)
				}
			}
			errs = append(errs, f(sid))
		}
		for _, v := range errs {
			if v != nil {
				return v
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
	action func(*context, *models.SlotMapping) error) error {
	if err := s.transitionSlotStateInternal(ctx, m, update, action); err != nil {
		om := *m
		update(m)
		nm := *m
		*m = om
		log.Errorf("[transitionSlotStateRaw] slot-%d transition from %v to %v failed, err: %v", m.Id, om, nm, errors.Trace(err))
		m.Action.Info.Progress = fmt.Sprintf("[ERROR] Slot[%04d]: %v", m.Id, err)
		return err
	}
	m.Action.Info.Progress = ""
	return nil
}

func (s *Topom) transitionSlotStateInternal(ctx *context, m *models.SlotMapping,
	update func(m *models.SlotMapping),
	action func(ctx *context, m *models.SlotMapping) error) (err error) {
	original := *m
	if actionErr := action(ctx, m); actionErr != nil {
		*m = original
		log.Errorf("[transitionSlotStateInternal] slot-[%d] action failed, err: '%v'", m.Id, actionErr)
		return actionErr
	}

	return s.updateSlotMappings(ctx, m, func(m *models.SlotMapping) {
		update(m)
		if m.Action.State != models.ActionNothing {
			m.UpdateStateStart()
		}
		log.Warnf("[transitionSlotStateInternal] slot-[%d] update from %v to: %v...", m.Id, original, m)
	}, &original)
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
