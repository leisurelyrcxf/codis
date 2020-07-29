// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package topom

import (
	"fmt"
	"strconv"
	"time"

	"github.com/CodisLabs/codis/pkg/models"
	"github.com/CodisLabs/codis/pkg/utils/errors"
	"github.com/CodisLabs/codis/pkg/utils/log"
	"github.com/CodisLabs/codis/pkg/utils/math2"
	"github.com/CodisLabs/codis/pkg/utils/sync2"
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
		var fut sync2.Future
		for sid, _ := range plans {
			fut.Add()

			go func(sid int) {
				var ctx *context
				var err error
				var m *models.SlotMapping

				defer func() {
					fut.Done(strconv.Itoa(sid), err)
				}()

				if ctx, err = s.newContext(); err != nil {
					log.Errorf("slot-[%d] new context fail, %v", sid, err)
					return
				}
				if m, err = ctx.getSlotMapping(sid); err != nil {
					log.Errorf("slot-[%d] get slot mapping fail, %v", sid, err)
					return
				}

				switch m.Action.State {
				case models.ActionPreparing:
					if err = s.preparingSlotAction(sid); err != nil {
						status := fmt.Sprintf("[ERROR] Slot[%04d]: %s", sid, err)
						s.action.progress.status.Store(status)
						return
					}
					s.action.progress.status.Store("")
					fallthrough

				case models.ActionWatching:
					if err = s.watchingSlotAction(sid); err != nil {
						status := fmt.Sprintf("[ERROR] Slot[%04d]: %s", sid, err)
						s.action.progress.status.Store(status)
						return
					}
					s.action.progress.status.Store("")
					fallthrough

				case models.ActionPrepared:
					if err = s.preparedSlotAction(sid); err != nil {
						status := fmt.Sprintf("[ERROR] Slot[%04d]: %s", sid, err)
						s.action.progress.status.Store(status)
						return
					}
					s.action.progress.status.Store("")
					fallthrough

				case models.ActionMigrating:
					err = s.migratingSlotAction(sid)
					if err != nil {
						status := fmt.Sprintf("[ERROR] Slot[%04d]: %s", sid, err)
						s.action.progress.status.Store(status)
						return
					}
					s.action.progress.status.Store("")
					fallthrough

				case models.ActionCleanup:
					err = s.cleanupSlotAction(sid)
					if err != nil {
						status := fmt.Sprintf("[ERROR] Slot[%04d]: %s", sid, err)
						s.action.progress.status.Store(status)
						return
					}
					s.action.progress.status.Store("")
					fallthrough

				case models.ActionFinished:
					err = s.finishedSlotAction(sid)
					if err != nil {
						status := fmt.Sprintf("[ERROR] Slot[%04d]: %s", sid, err)
						s.action.progress.status.Store(status)
						return
					}
					s.action.progress.status.Store("")

				default:
					err = errors.Errorf("slot-[%d] action state is invalid", sid)
					return

				}

			}(sid)
		}
		for _, v := range fut.Wait() {
			if v != nil {
				return v.(error)
			}
		}
		time.Sleep(time.Millisecond * 10)
	}
	return nil
}

func (s *Topom) finishedSlotAction(sid int) error {
	ctx, err := s.newContext()
	if err != nil {
		return err
	}
	m, err := ctx.getSlotMapping(sid)
	if err != nil {
		return err
	}
	if err := s.resyncSlotMappings(ctx, m); err != nil {
		return err
	}
	defer s.dirtySlotsCache(m.Id)
	m = &models.SlotMapping{
		Id:      m.Id,
		GroupId: m.Action.TargetId,
	}
	if err := s.storeUpdateSlotMapping(m); err != nil {
		return err
	}
	return nil
}

func (s *Topom) cleanupSlotAction(sid int) error {
	ctx, err := s.newContext()
	if err != nil {
		return err
	}
	m, err := ctx.getSlotMapping(sid)
	if err != nil {
		return err
	}
	if err := s.cleanupSlot(m); err != nil {
		log.Errorf("slot-[%d] cleanup slot failed", m.Id)
		return err
	}
	defer s.dirtySlotsCache(m.Id)
	log.Warnf("slot-[%d] resync to: finished", m.Id)
	m.Action.State = models.ActionFinished
	if err := s.resyncSlotMappings(ctx, m); err != nil {
		return err
	}
	if err := s.storeUpdateSlotMapping(m); err != nil {
		return err
	}

	return nil
}

func (s *Topom) migratingSlotAction(sid int) error {
	var db int = 0
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
				ctx, err := s.newContext()
				if err != nil {
					return err
				}
				m, err := ctx.getSlotMapping(sid)
				if err != nil {
					return err
				}
				defer s.dirtySlotsCache(m.Id)
				log.Warnf("slot-[%d] resync to: cleanup", sid)
				m.Action.State = models.ActionCleanup
				if err := s.resyncSlotMappings(ctx, m); err != nil {
					return err
				}
				if err := s.storeUpdateSlotMapping(m); err != nil {
					return err
				}
				return nil
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

func (s *Topom) preparingSlotAction(sid int) error {
	ctx, err := s.newContext()
	if err != nil {
		return err
	}
	m, err := ctx.getSlotMapping(sid)
	if err != nil {
		return err
	}
	if err := s.syncSlot(m); err != nil {
		log.Errorf("slot-[%d] sync slot(add slave) failed, %v", m.Id, err)
		return err
	}
	defer s.dirtySlotsCache(m.Id)
	log.Warnf("slot-[%d] resync to: watching", m.Id)
	m.Action.State = models.ActionWatching
	if err := s.resyncSlotMappings(ctx, m); err != nil {
		return err
	}
	if err := s.storeUpdateSlotMapping(m); err != nil {
		return err
	}
	return nil
}

func (s *Topom) watchingSlotAction(sid int) error {
	ctx, err := s.newContext()
	if err != nil {
		return err
	}
	m, err := ctx.getSlotMapping(sid)
	if err != nil {
		return err
	}
	if err := s.watchSlot(m); err != nil {
		return err
	}
	defer s.dirtySlotsCache(m.Id)
	log.Warnf("slot-[%d] resync to: prepared", m.Id)
	m.Action.State = models.ActionPrepared
	if err := s.resyncSlotMappings(ctx, m); err != nil {
		return err
	}
	if err := s.storeUpdateSlotMapping(m); err != nil {
		return err
	}
	return nil
}

func (s *Topom) preparedSlotAction(sid int) error {
	ctx, err := s.newContext()
	if err != nil {
		return err
	}
	m, err := ctx.getSlotMapping(sid)
	if err != nil {
		return err
	}
	defer s.dirtySlotsCache(m.Id)
	log.Warnf("slot-[%d] resync to: migrating", m.Id)
	m.Action.State = models.ActionMigrating
	if err := s.resyncSlotMappings(ctx, m); err != nil {
		return err
	}
	if err := s.storeUpdateSlotMapping(m); err != nil {
		return err
	}
	return nil
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
