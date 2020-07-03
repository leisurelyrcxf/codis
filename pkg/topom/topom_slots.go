// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package topom

import (
	gocontext "context"
	"github.com/CodisLabs/codis/pkg/models"
	"github.com/CodisLabs/codis/pkg/utils"
	"github.com/CodisLabs/codis/pkg/utils/errors"
	"github.com/CodisLabs/codis/pkg/utils/log"
	"github.com/CodisLabs/codis/pkg/utils/math2"
	"github.com/CodisLabs/codis/pkg/utils/redis"
	rbtree "github.com/emirpasic/gods/trees/redblacktree"
	redigo "github.com/garyburd/redigo/redis"
	"net"
	"sort"
	"strconv"
	"strings"
	"time"
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

func (s *Topom) SlotActionPrepareFilter(accept, update func(m *models.SlotMapping) bool) (int, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return 0, false, err
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
		return 0, false, nil
	}

	if update != nil && !update(m) {
		return 0, false, nil
	}

	log.Warnf("slot-[%d] action prepare:\n%s", m.Id, m.Encode())

	switch m.Action.State {
	case models.ActionPending:
		defer s.dirtySlotsCache(m.Id)
		m.Action.State = models.ActionPreparing
		if err := s.storeUpdateSlotMapping(m); err != nil {
			return 0, false, err
		}

	default:
		return m.Id, true, nil
	}

	return m.Id, true, nil
}

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

	case models.ActionFinished:

		return func(int) (int, int, error) {
			return 0, -1, nil
		}, nil

	default:

		return nil, errors.Errorf("slot-[%d] action state is invalid", m.Id)

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

func (s *Topom) syncSlot(m *models.SlotMapping) error {
	ctx, err := s.newContext()
	if err != nil {
		return err
	}
	if s.action.disabled.IsTrue() {
		return errors.Errorf("slot-[%d] sync slot, disabled", m.Id)
	}
	if ctx.isGroupPromoting(m.GroupId) {
		return errors.Errorf("slot-[%d] sync slot, Source group-[%d] is promoting", m.Id, m.GroupId)
	}
	if ctx.isGroupPromoting(m.Action.TargetId) {
		return errors.Errorf("slot-[%d] sync slot, Target group-[%d] is promoting", m.Id, m.Action.TargetId)
	}

	sourceAddress := ctx.getGroupMaster(m.GroupId)
	targetAddress := ctx.getGroupMaster(m.Action.TargetId)
	if sourceAddress == "" || targetAddress == "" {
		return nil
	}
	sourceHost, sourcePort, sourceSplitErr := net.SplitHostPort(sourceAddress)
	if sourceSplitErr != nil {
		log.Errorf("slot-[%d] sync slot, source[%s] address not validate, %v ", m.Id, sourceAddress, sourceSplitErr)
		return nil
	}
	_, _, targetSplitErr := net.SplitHostPort(targetAddress)
	if targetSplitErr != nil {
		log.Errorf("slot-[%d] sync slot, source[%s] address not validate, %v ", m.Id, targetAddress, targetSplitErr)
		return nil
	}

	targetCli, targetErr := s.action.redisp.GetClient(targetAddress)
	if targetErr != nil {
		return targetErr
	}
	defer s.action.redisp.PutClient(targetCli)

	_, addSlotsErr := targetCli.Do("pkcluster", "addslots", m.Id)
	if addSlotsErr != nil {
		targetCli, targetErr = s.action.redisp.GetClient(targetAddress)
	}
	if _, err := targetCli.Do("pkcluster", "slotsslaveof", sourceHost, sourcePort, m.Id); err != nil {
		return err
	}

	sourceCli, sourceErr := s.action.redisp.GetClient(sourceAddress)
	if sourceErr != nil {
		return sourceErr
	}
	defer s.action.redisp.PutClient(sourceCli)

	time.Sleep(time.Second * 1)
	if retryErr := utils.RetryInSecond(100, func() error {
		infoReply, infoErr := sourceCli.Do("pkcluster", "info", "slot", m.Id)
		if infoErr != nil {
			return infoErr
		}
		//(db0:2) binlog_offset=122 28755832,safety_purge=write2file112\r\n  Role: Master\r\n  connected_slaves: 1\r\n  slave[0]: 10.129.100.194:6381\r\n  replication_status: SlaveBinlogSync\r\n  lag: 0\r\n\r\n
		infoReplyStr, infoReplyErr := redigo.String(infoReply, nil)
		if infoReplyErr != nil {
			return infoReplyErr
		}
		log.Infof("slot-[%d] sync slot info reply:%s", m.Id, infoReplyStr)
		if strings.Contains(infoReplyStr, targetAddress) {
			log.Infof("slot-[%d] sync slot success ", m.Id)
			return nil
		}
		return errors.Errorf("slot-[%d] sync slot, validate fail, retry ... ", m.Id)
	}); retryErr != nil {
		return errors.Errorf("slot-[%d] sync slot, validate relation fail", m.Id)
	}

	return nil
}

func (s *Topom) watchSlot(m *models.SlotMapping) error {
	ctx, err := s.newContext()
	if err != nil {
		return errors.Errorf("slot-[%d] watch slot get ctx fail, %v ", m.Id, err)
	}
	sourceAddress := ctx.getGroupMaster(m.GroupId)
	targetAddress := ctx.getGroupMaster(m.Action.TargetId)
	if sourceAddress == "" || targetAddress == "" {
		return nil
	}

	var gap = math2.MaxUInt64(100000, s.config.MigrationGap)
	if compareErr := s.compareSlot(m.Id, sourceAddress, targetAddress, gap); compareErr != nil {
		return compareErr
	}

	log.Infof("slot-[%d] watch slot prepare for detach success, gap reached %v, let's detach.", m.Id, gap)

	go func() {
		waitDetach := make(chan bool)
		ctxDetach, cancelDetach := gocontext.WithTimeout(gocontext.Background(), 24*time.Hour)
		defer cancelDetach()
		go func(ctx gocontext.Context) {
			for {
				select {
				case <-ctx.Done():
					log.Errorf("slot-[%d] watch slot timeout ", m.Id)
					waitDetach <- true
					return
				default:
					log.Infof("slot-[%d] watch slot ... ", m.Id)
					time.Sleep(time.Second * 2)
				}

				if compareErr := s.compareSlot(m.Id, sourceAddress, targetAddress, 0); compareErr != nil {
					continue
				}
				log.Infof("slot-[%d] watch slot compare success.", m.Id)

				if detachErr := s.detachSlot(m.Id, targetAddress); detachErr != nil {
					log.Errorf("slot-[%d] watch slot detach fail, %v ", m.Id, detachErr)
					continue
				}

				log.Infof("slot-[%d] watch slot detach success.", m.Id)
				waitDetach <- true
				return
			}

		}(ctxDetach)

		<-waitDetach
	}()

	return nil
}

func (s *Topom) cleanupSlot(m *models.SlotMapping) error {
	ctx, err := s.newContext()
	if err != nil {
		return err
	}
	if s.action.disabled.IsTrue() {
		return errors.Errorf("slot-[%d] sync slot, disabled", m.Id)
	}
	if ctx.isGroupPromoting(m.GroupId) {
		return errors.Errorf("slot-[%d] sync slot, Source group-[%d] is promoting", m.Id, m.GroupId)
	}
	if ctx.isGroupPromoting(m.Action.TargetId) {
		return errors.Errorf("slot-[%d] sync slot, Target group-[%d] is promoting", m.Id, m.Action.TargetId)
	}

	sourceMasterAddress := ctx.getGroupMaster(m.GroupId)
	targetMasterAddress := ctx.getGroupMaster(m.Action.TargetId)
	if sourceMasterAddress == "" || targetMasterAddress == "" {
		return nil
	}

	sourceSlavesAddress := ctx.getGroupSlaves(m.GroupId)
	for _, slaveAddress := range sourceSlavesAddress {
		if err := s.detachSlot(m.Id, slaveAddress); err != nil {
			log.Errorf("slot-[%d] cleanup slot, detach source slaves slot fail, slave address:%v, %v ", m.Id, slaveAddress, err)
		}
	}
	time.Sleep(time.Second * 2)
	for _, slaveAddress := range sourceSlavesAddress {
		if err := s.cleanSlot(m.Id, slaveAddress); err != nil {
			log.Errorf("slot-[%d] cleanup slot, clean source slaves slot fail, slave address:%v, %v ", m.Id, slaveAddress, err)
		}
	}
	time.Sleep(time.Second * 2)
	if err := s.cleanSlot(m.Id, sourceMasterAddress); err != nil {
		log.Errorf("slot-[%d] cleanup slot detach fail, source address:%v, %v ", m.Id, sourceMasterAddress, err)
	}

	targetSlavesAddress := ctx.getGroupSlaves(m.Action.TargetId)
	for _, slaveAddress := range targetSlavesAddress {
		if err := s.backupSlot(m.Id, targetMasterAddress, slaveAddress); err != nil {
			log.Errorf("slot-[%d] cleanup slot, backup target slaves slot fail, target:%v, slave: %v, %v ", m.Id, targetMasterAddress, slaveAddress, err)
		}
	}

	return nil
}

func (s *Topom) cleanSlot(slot int, address string) error {
	cli, err := s.action.redisp.GetClient(address)
	if err != nil {
		return err
	}
	defer s.action.redisp.PutClient(cli)

	if _, err := cli.Do("pkcluster", "delslots", slot); err != nil {
		return err
	}
	return nil
}

func (s *Topom) backupSlot(slot int, masterAddress, slaveAddress string) error {
	cli, err := s.action.redisp.GetClient(slaveAddress)
	if err != nil {
		return err
	}
	defer s.action.redisp.PutClient(cli)

	masterHost, masterPort, splitErr := net.SplitHostPort(masterAddress)
	if splitErr != nil {
		return splitErr
	}

	_, addSlotsErr := cli.Do("pkcluster", "addslots", slot)
	if addSlotsErr != nil {
		cli, err = s.action.redisp.GetClient(slaveAddress)
	}
	if _, err := cli.Do("pkcluster", "slotsslaveof", masterHost, masterPort, slot); err != nil {
		return err
	}

	return nil
}

func (s *Topom) compareSlot(slot int, sourceAddress, targetAddress string, gap uint64) error {
	cli, err := s.action.redisp.GetClient(sourceAddress)
	if err != nil {
		return err
	}
	defer s.action.redisp.PutClient(cli)

	infoReply, infoErr := cli.Do("pkcluster", "info", "slot", slot)
	if infoErr != nil {
		return infoErr
	}
	infoReplyStr, infoReplyErr := redigo.String(infoReply, nil)
	if infoReplyErr != nil {
		return infoReplyErr
	}

	// (db0:2) binlog_offset=0 13384104,safety_purge=none
	//   Role: Master
	//   connected_slaves: 2
	//   slave[0]: 10.233.100.186:6384
	//   replication_status: SlaveBinlogSync
	//   lag: 0
	//   slave[1]: 10.233.100.186:6381
	//   replication_status: SlaveBinlogSync
	//   lag: 7658842

	//(db0:2) binlog_offset=0 13334816,safety_purge=none\r\n  Role: Master\r\n  connected_slaves: 2\r\n  slave[0]: 10.233.100.186:6383\r\n  replication_status: SlaveBinlogSync\r\n  lag: 0\r\n  slave[1]: 10.233.100.186:6382\r\n  replication_status: SlaveBinlogSync\r\n  lag: 4894842\r\n\r\n"
	//log.Infof("compare infoReplyStr:%v", infoReplyStr)
	infoReplySlice := strings.Split(infoReplyStr, "\r\n")
	var lag uint64
	var lagErr error
	var isTarget, isRepl bool
	for _, slice := range infoReplySlice {
		field := strings.Split(slice, ": ")
		if len(field) < 2 {
			continue
		}
		//log.Infof("compare isTarget:%v, isRepl:%v, field[0]:%v, field[1]:%v", isTarget, isRepl, field[0], field[1])
		if strings.HasPrefix(strings.TrimSpace(field[0]), "slave") && strings.TrimSpace(field[1]) == strings.TrimSpace(targetAddress) {
			isTarget = true
		}
		if isTarget && strings.TrimSpace(field[0]) == "replication_status" && strings.TrimSpace(field[1]) == "SlaveBinlogSync" {
			isRepl = true
		}
		if isTarget && isRepl && strings.TrimSpace(field[0]) == "lag" {
			lag, lagErr = strconv.ParseUint(strings.TrimSpace(field[1]), 10, 64)
			if lagErr != nil {
				return lagErr
			}
			if lag <= gap {
				log.Infof("slot-[%d] compare success, lag:%d <= gap:%d", slot, lag, gap)
				return nil
			}
			isTarget = false
			isRepl = true
		}
	}

	return errors.Errorf("slot-[%d] compare,lag:%v,gap:%v", slot, lag, gap)
}

func (s *Topom) detachSlot(slot int, address string) error {
	cli, err := s.action.redisp.GetClient(address)
	if err != nil {
		return err
	}
	defer s.action.redisp.PutClient(cli)

	if _, err := cli.Do("pkcluster", "slotsslaveof", "no", "one", slot); err != nil {
		return err
	}

	return nil
}
