// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package topom

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/CodisLabs/codis/pkg/models"
	"github.com/CodisLabs/codis/pkg/utils/errors"
	"github.com/CodisLabs/codis/pkg/utils/log"
	"github.com/CodisLabs/codis/pkg/utils/math2"
	"github.com/CodisLabs/codis/pkg/utils/redis"
	rbtree "github.com/emirpasic/gods/trees/redblacktree"
)

func (s *Topom) SlotCreateAction(sid int, gid int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return err
	}

	g, err := ctx.getGroup(gid)
	if err != nil {
		return err
	}
	if len(g.Servers) == 0 {
		return errors.Errorf("group-[%d] is empty", gid)
	}

	m, err := ctx.getSlotMapping(sid)
	if err != nil {
		return err
	}
	if m.Action.State != models.ActionNothing {
		return errors.Errorf("slot-[%d] action already exists", sid)
	}
	if m.GroupId == gid {
		return errors.Errorf("slot-[%d] already in group-[%d]", sid, gid)
	}
	defer s.dirtySlotsCache(m.Id)

	m.Action.State = models.ActionPending
	m.Action.Index = ctx.maxSlotActionIndex() + 1
	m.Action.TargetId = g.Id
	return s.storeUpdateSlotMapping(m)
}

func (s *Topom) SlotCreateActionPlan(plan map[int]int) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	ctx, err := s.newContext()
	if err != nil {
		return err
	}

	type Task struct {
		Addr string

		MaxSlotNum int
		Err        error
	}

	var (
		tasks            = make(map[string]*Task)
		slot2MasterAddrs = make(map[int]struct{ SourceMaster, TargetMaster string })

		addTask = func(addrs ...string) {
			for _, addr := range addrs {
				if _, ok := tasks[addr]; !ok {
					tasks[addr] = &Task{Addr: addr, MaxSlotNum: -1}
				}
			}
		}
		getMaxSlotNum = func(t *Task) int {
			if t == nil {
				return -1
			}
			return t.MaxSlotNum
		}
	)
	for sid, targetGid := range plan {
		g, err := ctx.getGroup(targetGid)
		if err != nil {
			return err
		}
		if len(g.Servers) == 0 {
			return errors.Errorf("target group-[%d] is empty", targetGid)
		}

		m, err := ctx.getSlotMapping(sid)
		if err != nil {
			return err
		}
		if m.Action.State != models.ActionNothing {
			return errors.Errorf("slot-[%d] action already exists", sid)
		}
		if m.GroupId == targetGid {
			return errors.Errorf("slot-[%d] already in group-[%d]", sid, targetGid)
		}
		if m.GroupId != 0 {
			g, err := ctx.getGroup(m.GroupId)
			if err != nil {
				return err
			}
			if len(g.Servers) == 0 {
				return errors.Errorf("source group-[%d] is empty", targetGid)
			}
		}
		if targetGid != 0 && m.GroupId != 0 {
			info := struct{ SourceMaster, TargetMaster string }{
				SourceMaster: ctx.getGroupMaster(m.GroupId),
				TargetMaster: ctx.getGroupMaster(targetGid),
			}
			slot2MasterAddrs[m.Id] = info
			addTask(info.SourceMaster, info.TargetMaster)
		}
	}

	if len(tasks) > 0 {
		var wg sync.WaitGroup
		for _, t := range tasks {
			wg.Add(1)

			go func(t *Task) {
				defer wg.Done()

				t.Err = s.withRedisClient(t.Addr, func(client *redis.Client) (err error) {
					t.MaxSlotNum, err = client.GetMaxSlotNum()
					return
				})
			}(t)
		}
		wg.Wait()

		for _, t := range tasks {
			if t.Err != nil {
				return t.Err
			}
			if t.MaxSlotNum <= 0 {
				return fmt.Errorf("%s MaxSlotNum is %d", t.Addr, t.MaxSlotNum)
			}
		}
	}

	for sid, gid := range plan {
		m := ctx.slots[sid]
		defer s.dirtySlotsCache(m.Id)

		m.Action.State = models.ActionPending
		m.Action.Index = ctx.maxSlotActionIndex() + 1
		m.Action.TargetId = gid
		if m.Action.TargetId != 0 && m.GroupId != 0 {
			info := slot2MasterAddrs[m.Id]
			srcMaxSlotNum, targetMaxSlotNum := getMaxSlotNum(tasks[info.SourceMaster]), getMaxSlotNum(tasks[info.TargetMaster])
			if srcMaxSlotNum <= 0 || targetMaxSlotNum <= 0 {
				return fmt.Errorf("unreachable code, %v srcMaxSlotNum(%d) <= 0 || %v targetMaxSlotNum(%d) <= 0",
					info.SourceMaster, srcMaxSlotNum, info.TargetMaster, targetMaxSlotNum)
			}
			if targetMaxSlotNum > srcMaxSlotNum {
				m.Action.Resharding = true
				m.Action.SourceMaxSlotNum = srcMaxSlotNum
			}
		}
		if err := s.storeUpdateSlotMapping(m); err != nil {
			return err
		}
	}
	return nil
}

func (s *Topom) SlotCreateActionSome(groupFrom, groupTo int, numSlots int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return err
	}

	g, err := ctx.getGroup(groupTo)
	if err != nil {
		return err
	}
	if len(g.Servers) == 0 {
		return errors.Errorf("group-[%d] is empty", g.Id)
	}

	var pending []int
	for _, m := range ctx.slots {
		if len(pending) >= numSlots {
			break
		}
		if m.Action.State != models.ActionNothing {
			continue
		}
		if m.GroupId != groupFrom {
			continue
		}
		if m.GroupId == g.Id {
			continue
		}
		pending = append(pending, m.Id)
	}

	for _, sid := range pending {
		m, err := ctx.getSlotMapping(sid)
		if err != nil {
			return err
		}
		defer s.dirtySlotsCache(m.Id)

		m.Action.State = models.ActionPending
		m.Action.Index = ctx.maxSlotActionIndex() + 1
		m.Action.TargetId = g.Id
		if err := s.storeUpdateSlotMapping(m); err != nil {
			return err
		}
	}
	return nil
}

func (s *Topom) SlotCreateActionRange(beg, end int, gid int, must bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return err
	}

	if !(beg >= 0 && beg <= end && end < s.config.MaxSlotNum) {
		return errors.Errorf("invalid slot range [%d,%d]", beg, end)
	}

	g, err := ctx.getGroup(gid)
	if err != nil {
		return err
	}
	if len(g.Servers) == 0 {
		return errors.Errorf("group-[%d] is empty", g.Id)
	}

	var pending []int
	for sid := beg; sid <= end; sid++ {
		m, err := ctx.getSlotMapping(sid)
		if err != nil {
			return err
		}
		if m.Action.State != models.ActionNothing {
			if !must {
				continue
			}
			return errors.Errorf("slot-[%d] action already exists", sid)
		}
		if m.GroupId == g.Id {
			if !must {
				continue
			}
			return errors.Errorf("slot-[%d] already in group-[%d]", sid, g.Id)
		}
		pending = append(pending, m.Id)
	}

	for _, sid := range pending {
		m, err := ctx.getSlotMapping(sid)
		if err != nil {
			return err
		}
		defer s.dirtySlotsCache(m.Id)

		m.Action.State = models.ActionPending
		m.Action.Index = ctx.maxSlotActionIndex() + 1
		m.Action.TargetId = g.Id
		if err := s.storeUpdateSlotMapping(m); err != nil {
			return err
		}
	}
	return nil
}

func (s *Topom) SlotRemoveAction(sid int) error {
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
	if m.Action.State == models.ActionNothing {
		return errors.Errorf("slot-[%d] action doesn't exist", sid)
	}
	if m.Action.State != models.ActionPending {
		return errors.Errorf("slot-[%d] action isn't pending", sid)
	}
	defer s.dirtySlotsCache(m.Id)

	m = &models.SlotMapping{
		Id:      m.Id,
		GroupId: m.GroupId,
	}
	return s.storeUpdateSlotMapping(m)
}

func (s *Topom) SlotActionPrepare() (int, bool, error) {
	return s.SlotActionPrepareFilter(nil, nil)
}

func (s *Topom) SlotActionPrepareFilter(
	accept func(candidate *models.SlotMapping) (accepted bool),
	update func(picked *models.SlotMapping) (kontinue bool)) (updatedSid int, kontinue bool, _ error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	ctx, err := s.newContext()
	if err != nil {
		return -1, false, err
	}

	var minActionIndex = func(filter func(m *models.SlotMapping) bool) (picked *models.SlotMapping) {
		for _, m := range ctx.slots {
			if m.Action.State == models.ActionNothing {
				continue
			}
			if filter(m) {
				if picked != nil && picked.Action.Index < m.Action.Index {
					continue
				}
				if accept == nil || accept(m) {
					picked = m
				}
			}
		}
		return picked
	}

	var m = func() *models.SlotMapping {
		var picked = minActionIndex(func(m *models.SlotMapping) bool {
			return m.Action.State != models.ActionPending
		})
		if picked != nil {
			return picked
		}
		if s.action.disabled.IsTrue() {
			return nil
		}
		return minActionIndex(func(m *models.SlotMapping) bool {
			return m.Action.State == models.ActionPending
		})
	}()

	if m == nil {
		return -1, false, nil
	}

	if update != nil && !update(m) {
		return -1, false, nil
	}

	return m.Id, true, nil
}

// Deprecated: there is no state of ActionMigrating any more.
func (s *Topom) SlotActionComplete(sid int) error {
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

	log.Warnf("slot-[%d] action complete:\n%s", m.Id, m.Encode())

	switch m.Action.State {

	case models.ActionMigrating:

		defer s.dirtySlotsCache(m.Id)

		m.Action.State = models.ActionFinished
		if err := s.storeUpdateSlotMapping(m); err != nil {
			return err
		}

		fallthrough

	case models.ActionFinished:

		log.Warnf("slot-[%d] resync to finished", m.Id)

		if err := s.resyncSlotMappings(ctx, m); err != nil {
			log.Warnf("slot-[%d] resync to finished failed", m.Id)
			return err
		}
		defer s.dirtySlotsCache(m.Id)

		m = &models.SlotMapping{
			Id:      m.Id,
			GroupId: m.Action.TargetId,
		}
		return s.storeUpdateSlotMapping(m)

	default:

		return errors.Errorf("slot-[%d] action state is invalid", m.Id)

	}
}

// Deprecated: there is no state of ActionMigrating any more.
func (s *Topom) newSlotActionExecutor(sid int) (func(db int) (remains int, nextdb int, err error), error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	ctx, err := s.newContext()
	if err != nil {
		return nil, err
	}

	m, err := ctx.getSlotMapping(sid)
	if err != nil {
		return nil, err
	}

	switch m.Action.State {
	case models.ActionMigrating:
		if s.action.disabled.IsTrue() {
			return nil, nil
		}
		if ctx.isGroupPromoting(m.GroupId) {
			return nil, nil
		}
		if ctx.isGroupPromoting(m.Action.TargetId) {
			return nil, nil
		}

		from := ctx.getGroupMaster(m.GroupId)
		dest := ctx.getGroupMaster(m.Action.TargetId)

		s.action.executor.Incr()

		return func(db int) (int, int, error) {
			defer s.action.executor.Decr()
			if from == "" {
				return 0, -1, nil
			}
			c, err := s.action.redisp.GetClient(from)
			if err != nil {
				return 0, -1, err
			}
			defer s.action.redisp.PutClient(c)

			if err := c.Select(db); err != nil {
				return 0, -1, err
			}
			var do func() (int, error)

			method, _ := models.ParseForwardMethod(s.config.MigrationMethod)
			switch method {
			case models.ForwardSync:
				do = func() (int, error) {
					return c.MigrateSlot(sid, dest)
				}
			case models.ForwardSemiAsync:
				var option = &redis.MigrateSlotAsyncOption{
					MaxBulks: s.config.MigrationAsyncMaxBulks,
					MaxBytes: s.config.MigrationAsyncMaxBytes.AsInt(),
					NumKeys:  s.config.MigrationAsyncNumKeys,
					Timeout: math2.MinDuration(time.Second*5,
						s.config.MigrationTimeout.Duration()),
				}
				do = func() (int, error) {
					return c.MigrateSlotAsync(sid, dest, option)
				}
			default:
				log.Panicf("unknown forward method %d", int(method))
			}

			n, err := do()
			if err != nil {
				return 0, -1, err
			} else if n != 0 {
				return n, db, nil
			}

			nextdb := -1
			m, err := c.InfoKeySpace()
			if err != nil {
				return 0, -1, err
			}
			for i := range m {
				if (nextdb == -1 || i < nextdb) && db < i {
					nextdb = i
				}
			}
			return 0, nextdb, nil

		}, nil
	case models.ActionCleanup, models.ActionFinished:
		return func(int) (int, int, error) {
			return 0, -1, nil
		}, nil
	default:
		return nil, errors.Errorf("slot-[%d] action state '%s' is invalid", m.Id, m.Action.State)
	}
}

func (s *Topom) SlotsAssignGroup(slots []*models.SlotMapping) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return err
	}

	for _, m := range slots {
		_, err := ctx.getSlotMapping(m.Id)
		if err != nil {
			return err
		}
		g, err := ctx.getGroup(m.GroupId)
		if err != nil {
			return err
		}
		if len(g.Servers) == 0 {
			return errors.Errorf("group-[%d] is empty", g.Id)
		}
		if m.Action.State != models.ActionNothing {
			return errors.Errorf("invalid slot-[%d] action = %s", m.Id, m.Action.State)
		}
	}

	for i, m := range slots {
		if g := ctx.group[m.GroupId]; !g.OutOfSync {
			defer s.dirtyGroupCache(g.Id)
			g.OutOfSync = true
			if err := s.storeUpdateGroup(g); err != nil {
				return err
			}
		}
		slots[i] = &models.SlotMapping{
			Id: m.Id, GroupId: m.GroupId,
		}
	}

	for _, m := range slots {
		defer s.dirtySlotsCache(m.Id)

		log.Warnf("slot-[%d] will be mapped to group-[%d]", m.Id, m.GroupId)

		if err := s.storeUpdateSlotMapping(m); err != nil {
			return err
		}
	}
	return s.resyncSlotMappings(ctx, slots...)
}

func (s *Topom) SlotsAssignOffline(slots []*models.SlotMapping) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return err
	}

	for _, m := range slots {
		_, err := ctx.getSlotMapping(m.Id)
		if err != nil {
			return err
		}
		if m.GroupId != 0 {
			return errors.Errorf("group of slot-[%d] should be 0", m.Id)
		}
	}

	for i, m := range slots {
		slots[i] = &models.SlotMapping{
			Id: m.Id,
		}
	}

	for _, m := range slots {
		defer s.dirtySlotsCache(m.Id)

		log.Warnf("slot-[%d] will be mapped to group-[%d] (offline)", m.Id, m.GroupId)

		if err := s.storeUpdateSlotMapping(m); err != nil {
			return err
		}
	}
	return s.resyncSlotMappings(ctx, slots...)
}

func (s *Topom) SlotsSetStopStatus(slots []int, stopped bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return err
	}

	for _, slot := range slots {
		m, err := ctx.getSlotMapping(slot)
		if err != nil {
			return err
		}
		if m.Action.State != models.ActionNothing {
			return errors.Errorf("slot-[%d] is migrating", m.Id)
		}
	}

	updatedSlotMappings := make([]*models.SlotMapping, 0, len(slots))
	for _, slot := range slots {
		defer s.dirtySlotsCache(slot)

		m := ctx.slots[slot]
		if stopped {
			log.Warnf("slot-[%d] will be stopped", m.Id)
		} else {
			log.Warnf("slot-[%d] will be started", m.Id)
		}
		m.Stopped = stopped
		if err := s.storeUpdateSlotMapping(m); err != nil {
			return err
		}
		updatedSlotMappings = append(updatedSlotMappings, m)
	}
	return s.resyncSlotMappings(ctx, updatedSlotMappings...)
}

func (s *Topom) SlotsRebalance(confirm bool) (map[int]int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return nil, err
	}

	var groupIds []int
	for _, g := range ctx.group {
		if len(g.Servers) != 0 {
			groupIds = append(groupIds, g.Id)
		}
	}
	sort.Ints(groupIds)

	if len(groupIds) == 0 {
		return nil, errors.Errorf("no valid group could be found")
	}

	var (
		assigned = make(map[int]int)
		pendings = make(map[int][]int)
		moveout  = make(map[int]int)
		docking  []int
	)
	var groupSize = func(gid int) int {
		return assigned[gid] + len(pendings[gid]) - moveout[gid]
	}

	// don't migrate slot if it's being migrated
	for _, m := range ctx.slots {
		if m.Action.State != models.ActionNothing {
			assigned[m.Action.TargetId]++
		}
	}

	var lowerBound = s.config.MaxSlotNum / len(groupIds)

	// don't migrate slot if groupSize < lowerBound
	for _, m := range ctx.slots {
		if m.Action.State != models.ActionNothing {
			continue
		}
		if m.GroupId != 0 {
			if groupSize(m.GroupId) < lowerBound {
				assigned[m.GroupId]++
			} else {
				pendings[m.GroupId] = append(pendings[m.GroupId], m.Id)
			}
		}
	}

	var tree = rbtree.NewWith(func(x, y interface{}) int {
		var gid1 = x.(int)
		var gid2 = y.(int)
		if gid1 != gid2 {
			if d := groupSize(gid1) - groupSize(gid2); d != 0 {
				return d
			}
			return gid1 - gid2
		}
		return 0
	})
	for _, gid := range groupIds {
		tree.Put(gid, nil)
	}

	// assign offline slots to the smallest group
	for _, m := range ctx.slots {
		if m.Action.State != models.ActionNothing {
			continue
		}
		if m.GroupId != 0 {
			continue
		}
		dest := tree.Left().Key.(int)
		tree.Remove(dest)

		docking = append(docking, m.Id)
		moveout[dest]--

		tree.Put(dest, nil)
	}

	var upperBound = (s.config.MaxSlotNum + len(groupIds) - 1) / len(groupIds)

	// rebalance between different server groups
	for tree.Size() >= 2 {
		from := tree.Right().Key.(int)
		tree.Remove(from)

		if len(pendings[from]) == moveout[from] {
			continue
		}
		dest := tree.Left().Key.(int)
		tree.Remove(dest)

		var (
			fromSize = groupSize(from)
			destSize = groupSize(dest)
		)
		if fromSize <= lowerBound {
			break
		}
		if destSize >= upperBound {
			break
		}
		if d := fromSize - destSize; d <= 1 {
			break
		}
		moveout[from]++
		moveout[dest]--

		tree.Put(from, nil)
		tree.Put(dest, nil)
	}

	for gid, n := range moveout {
		if n < 0 {
			continue
		}
		if n > 0 {
			sids := pendings[gid]
			sort.Sort(sort.Reverse(sort.IntSlice(sids)))

			docking = append(docking, sids[0:n]...)
			pendings[gid] = sids[n:]
		}
		delete(moveout, gid)
	}
	sort.Ints(docking)

	var plans = make(map[int]int)

	for _, gid := range groupIds {
		var in = -moveout[gid]
		for i := 0; i < in && len(docking) != 0; i++ {
			plans[docking[0]] = gid
			docking = docking[1:]
		}
	}

	if !confirm {
		return plans, nil
	}

	var slotIds []int
	for sid, _ := range plans {
		slotIds = append(slotIds, sid)
	}
	sort.Ints(slotIds)

	for _, sid := range slotIds {
		m, err := ctx.getSlotMapping(sid)
		if err != nil {
			return nil, err
		}
		defer s.dirtySlotsCache(m.Id)

		m.Action.State = models.ActionPending
		m.Action.Index = ctx.maxSlotActionIndex() + 1
		m.Action.TargetId = plans[sid]
		if err := s.storeUpdateSlotMapping(m); err != nil {
			return nil, err
		}
	}
	return plans, nil
}

func VoidAction(*context, *models.SlotMapping) error {
	return nil
}

func (s *Topom) preparingSlot(ctx *context, m *models.SlotMapping) (err error) {
	if s.action.disabled.IsTrue() {
		return errors.Errorf("slot-[%d] topom action disabled", m.Id)
	}
	if ctx.isGroupPromoting(m.GroupId) {
		return errors.Errorf("slot-[%d] preparing slot, Source group-[%d] is promoting", m.Id, m.GroupId)
	}
	if ctx.isGroupPromoting(m.Action.TargetId) {
		return errors.Errorf("slot-[%d] preparing slot, Target group-[%d] is promoting", m.Id, m.Action.TargetId)
	}

	sourceAddress := ctx.getGroupMaster(m.GroupId)
	targetAddress := ctx.getGroupMaster(m.Action.TargetId)
	m.Action.Info.SourceMaster = sourceAddress
	m.Action.Info.TargetMaster = targetAddress

	if sourceAddress == "" || targetAddress == "" {
		return nil
	}

	if s.action.slotsProgress[m.Id].RollbackTimes >= MaxRollbackTimes {
		log.Warnf("[preparingSlot] rollback too many times(>%d), delete slots of target group and retry", MaxRollbackTimes)

		if err := s.cleanupSlotsOfGroup(ctx, m, m.Action.TargetId); err != nil {
			log.Errorf("[preparingSlot] failed to cleanup slots of target group %d: '%s'", m.Action.TargetId, err)
			return err
		}
		defer func() {
			if err == nil {
				s.action.slotsProgress[m.Id].RollbackTimes = 0
			}
		}()
	}

	if err := s.addSlotIfNotExistsSM(m, targetAddress); err != nil {
		log.Errorf("[preparingSlot] slot-[%d] failed to add slot on target %s, err: '%v'", m.Id, targetAddress, err)
		return err
	}

	if err := s.assureTargetSlavesLinked(ctx, m); err != nil {
		log.Errorf("[preparingSlot] slot-[%d] failed to backup target group, err: '%v'", m.Id, err)
		return err
	}

	if err = s.createReplLink(m, sourceAddress, targetAddress); err != nil {
		log.Errorf("[preparingSlot] slot-[%d] failed to create repl link target_master(%s)->source_master(%s), err: '%v'", m.Id, targetAddress, sourceAddress, err)
	}
	return err
}

func (s *Topom) watchSlot(ctx *context, m *models.SlotMapping) error {
	gap := s.GetSlotActionGap()
	return s.compareSlot(ctx, m, gap, func(string, string) error {
		return s.backedUpSlot(ctx, m, gap)
	})
}

func (s *Topom) preparedSlot(ctx *context, m *models.SlotMapping) error {
	return s.compareSlot(ctx, m, 0, func(sourceMaster, targetMaster string) error {
		return s.detachSlotSM(m, sourceMaster, targetMaster)
	})
}

func (s *Topom) compareSlot(ctx *context, m *models.SlotMapping, gap uint64,
	onCompareSlotOK func(sourceMaster, targetMaster string) error) error {
	sourceMaster := ctx.getGroupMaster(m.GroupId)
	targetMaster := ctx.getGroupMaster(m.Action.TargetId)
	if sourceMaster == "" || targetMaster == "" {
		return nil
	}
	targetMasterReplInfo, err := s.getSlaveReplInfo(m, sourceMaster, targetMaster)
	if err != nil {
		log.Errorf("[%sSlot] slot-[%d] can't find target master replication info: %v", m.Action.State, m.Id, err)
		return err
	}
	if err := targetMasterReplInfo.GapReached(gap); err != nil {
		log.Errorf("[%sSlot] slot-[%d] gap(%d) not reached: %v", m.Action.State, m.Id, gap, err)
		return err
	}
	return onCompareSlotOK(sourceMaster, targetMaster)
}

func (s *Topom) cleanupSlot(ctx *context, m *models.SlotMapping) error {
	if m.Action.Info.SourceMaster == "" || m.Action.Info.TargetMaster == "" {
		return nil
	}

	if m.Action.Resharding {
		for _, am := range ctx.slots {
			if am.Id != m.Id &&
				am.GroupId == m.GroupId &&
				am.Id%m.Action.SourceMaxSlotNum == m.Id%m.Action.SourceMaxSlotNum {
				log.Infof("[cleanupSlot] skip cleanup, another slot %d exists for same group", am.Id, am.GroupId)
				return nil
			}
		}
	}

	if err := s.cleanupSlotsOfGroup(ctx, m, m.GroupId); err != nil {
		log.Errorf("[cleanupSlot] failed to cleanup slots of source group %d: '%s'", m.GroupId, err)
	}
	return nil
}
