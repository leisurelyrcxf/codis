// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package topom

import (
	"math"
	"time"

	"github.com/CodisLabs/codis/pkg/utils/pika"

	"github.com/CodisLabs/codis/pkg/models"
	"github.com/CodisLabs/codis/pkg/utils/errors"
	"github.com/CodisLabs/codis/pkg/utils/log"
	"github.com/CodisLabs/codis/pkg/utils/redis"
)

func (s *Topom) CreateGroup(gid int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return err
	}

	if gid <= 0 || gid > models.MaxGroupId {
		return errors.Errorf("invalid group id = %d, out of range", gid)
	}
	if ctx.group[gid] != nil {
		return errors.Errorf("group-[%d] already exists", gid)
	}
	defer s.dirtyGroupCache(gid)

	g := &models.Group{
		Id:      gid,
		Servers: []*models.GroupServer{},
	}
	return s.storeCreateGroup(g)
}

func (s *Topom) RemoveGroup(gid int) error {
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
	if len(g.Servers) != 0 {
		return errors.Errorf("group-[%d] isn't empty", gid)
	}
	defer s.dirtyGroupCache(g.Id)

	return s.storeRemoveGroup(g)
}

func (s *Topom) GetGroupByServer(addr string) (*models.Group, int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	ctx, err := s.newContext()
	if err != nil {
		return nil, -1, err
	}

	return ctx.getGroupByServer(addr)
}

func (s *Topom) GetGroup(gid int) (*models.Group, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	ctx, err := s.newContext()
	if err != nil {
		return nil, err
	}

	return ctx.getGroup(gid)
}

func (s *Topom) ResyncGroup(gid int) error {
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

	if err := s.resyncSlotMappingsByGroupId(ctx, gid); err != nil {
		log.Warnf("group-[%d] resync-group failed", g.Id)
		return err
	}
	defer s.dirtyGroupCache(gid)

	g.OutOfSync = false
	return s.storeUpdateGroup(g)
}

func (s *Topom) ResyncGroupAll() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return err
	}

	for _, g := range ctx.group {
		if err := s.resyncSlotMappingsByGroupId(ctx, g.Id); err != nil {
			log.Warnf("group-[%d] resync-group failed", g.Id)
			return err
		}
		defer s.dirtyGroupCache(g.Id)

		g.OutOfSync = false
		if err := s.storeUpdateGroup(g); err != nil {
			return err
		}
	}
	return nil
}

func (s *Topom) GroupAddServer(gid int, dc, addr string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return err
	}

	if addr == "" {
		return errors.Errorf("invalid server address")
	}

	for _, g := range ctx.group {
		for _, x := range g.Servers {
			if x.Addr == addr {
				return errors.Errorf("server-[%s] already exists", addr)
			}
		}
	}

	g, err := ctx.getGroup(gid)
	if err != nil {
		return err
	}
	if g.Promoting.State != models.ActionNothing {
		return errors.Errorf("group-[%d] is promoting", g.Id)
	}

	if p := ctx.sentinel; len(p.Servers) != 0 {
		defer s.dirtySentinelCache()
		p.OutOfSync = true
		if err := s.storeUpdateSentinel(p); err != nil {
			return err
		}
	}
	defer s.dirtyGroupCache(g.Id)

	g.Servers = append(g.Servers, &models.GroupServer{Addr: addr, DataCenter: dc})
	return s.storeUpdateGroup(g)
}

func (s *Topom) GroupDelServer(gid int, addr string) error {
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
	index, err := ctx.getGroupIndex(g, addr)
	if err != nil {
		return err
	}

	if g.Promoting.State != models.ActionNothing {
		return errors.Errorf("group-[%d] is promoting", g.Id)
	}

	if index == 0 {
		if len(g.Servers) != 1 || ctx.isGroupInUse(g.Id) {
			return errors.Errorf("group-[%d] can't remove master, still in use", g.Id)
		}
	}

	if p := ctx.sentinel; len(p.Servers) != 0 {
		defer s.dirtySentinelCache()
		p.OutOfSync = true
		if err := s.storeUpdateSentinel(p); err != nil {
			return err
		}
	}
	defer s.dirtyGroupCache(g.Id)

	if index != 0 && g.Servers[index].ReplicaGroup {
		g.OutOfSync = true
	}

	var slice = make([]*models.GroupServer, 0, len(g.Servers))
	for i, x := range g.Servers {
		if i != index {
			slice = append(slice, x)
		}
	}
	if len(slice) == 0 {
		g.OutOfSync = false
	}

	g.Servers = slice

	return s.storeUpdateGroup(g)
}

func (s *Topom) CreateGroupPromoteAction(gid int, addr string) error {
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
	index, err := ctx.getGroupIndex(g, addr)
	if err != nil {
		return err
	}

	if g.Promoting.State != models.ActionNothing {
		if index != g.Promoting.Index {
			return errors.Errorf("group-[%d] is promoting index = %d", g.Id, g.Promoting.Index)
		}
	} else {
		if index == 0 {
			return errors.Errorf("group-[%d] can't promote master", g.Id)
		}
	}
	if n := s.action.executor.Int64(); n != 0 {
		return errors.Errorf("slots-migration is running = %d", n)
	}

	g.Promoting.Index = index
	g.Promoting.State = models.ActionPending

	defer s.dirtyGroupCache(g.Id)
	if err := s.storeUpdateGroup(g); err != nil {
		return err
	}
	return nil
}

func (s *Topom) ProcessGroupPromoteAction() error {
	for {
		nextGroupID, err := s.GetNexToPromoteGroup()
		if err != nil {
			return err
		}
		if nextGroupID <= 0 {
			return nil
		}
		if err := s.groupPromoteServer(nextGroupID); err != nil {
			return err
		}
	}
}

func (s *Topom) GetNexToPromoteGroup() (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	ctx, err := s.newContext()
	if err != nil {
		return -1, err
	}

	for gid, g := range ctx.group {
		if g.Promoting.State != models.ActionNothing && g.Promoting.State != models.ActionPending {
			return gid, nil
		}
	}

	for gid, g := range ctx.group {
		if g.Promoting.State == models.ActionPending {
			return gid, nil
		}
	}
	return -1, nil
}

func (s *Topom) GroupPromoteServer(gid int, addr string) error {
	return errors.Errorf("discarded")
}

func (s *Topom) groupPromoteServer(gid int) error {
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
	if g.Promoting.Index == 0 {
		return errors.Errorf("can't promote master of group %d", g.Id)
	}

	var masterDown = false

	switch g.Promoting.State {
	case models.ActionNothing:
		return nil

	case models.ActionPending:
		defer s.dirtyGroupCache(g.Id)

		log.Warnf("group-[%d] in state pending", g.Id)

		slots := ctx.getSlotMappingsByGroupId(g.Id)

		g.Promoting.State = models.ActionPreparing
		if err := s.resyncSlotMappings(ctx, slots...); err != nil {
			return err
		}
		if err := s.storeUpdateGroup(g); err != nil {
			return err
		}

		fallthrough

	case models.ActionPreparing:

		defer s.dirtyGroupCache(g.Id)

		log.Warnf("group-[%d] resync to preparing", g.Id)

		_ = s.slaveLagsOK(ctx, g, &masterDown, g.Servers[g.Promoting.Index].Addr,
			math.MaxUint64, true, func(string, bool) error {
				return nil
			})

		g.Promoting.State = models.ActionWatching
		slots := ctx.getSlotMappingsByGroupId(g.Id)
		if err := s.resyncSlotMappings(ctx, slots...); err != nil {
			log.Warnf("group-[%d] resync-rollback to preparing due to error: %v", g.Id, err)
			g.Promoting.State = models.ActionPreparing
			s.resyncSlotMappings(ctx, slots...)
			log.Warnf("group-[%d] resync-rollback to preparing, done", g.Id)
			return err
		}
		if err := s.storeUpdateGroup(g); err != nil {
			return err
		}

		fallthrough

	case models.ActionWatching:

		defer s.dirtyGroupCache(g.Id)

		log.Warnf("group-[%d] in state watching", g.Id)

		if err := s.slaveLagsOK(ctx, g, &masterDown, g.Servers[g.Promoting.Index].Addr, s.GetSlotActionGap(), false,
			func(slaveAddr string, needsCheckSlaveAliveness bool) error {
				if !needsCheckSlaveAliveness {
					return nil
				}
				_, err := s.action.redisp.GetPikaSlotsInfo(slaveAddr)
				return err
			}); err != nil {
			return err
		}

		g.Promoting.State = models.ActionPrepared
		slots := ctx.getSlotMappingsByGroupId(g.Id)
		if err := s.resyncSlotMappings(ctx, slots...); err != nil {
			log.Warnf("group-[%d] resync-rollback to watching due to error: %v", g.Id, err)
			g.Promoting.State = models.ActionWatching
			s.resyncSlotMappings(ctx, slots...)
			log.Warnf("group-[%d] resync-rollback to watching, done", g.Id)
			return err
		}
		if err := s.storeUpdateGroup(g); err != nil {
			return err
		}

		time.Sleep(time.Millisecond * 10)
		fallthrough

	case models.ActionPrepared:

		defer s.dirtyGroupCache(g.Id)

		log.Warnf("group-[%d] in state prepared", g.Id)
		if err := s.slaveLagsOK(ctx, g, &masterDown, g.Servers[g.Promoting.Index].Addr, 0, false, func(slaveAddr string, _ bool) error {
			return s.action.redisp.WithRedisClient(slaveAddr, func(c *redis.Client) error {
				err := c.BecomeMasterAllSlots()
				if err != nil {
					log.WarnErrorf(err, "redis %s set master to NO:ONE failed", slaveAddr)
				}
				return err
			})
		}); err != nil {
			return err
		}

		g.Promoting.State = models.ActionPromoting
		slots := ctx.getSlotMappingsByGroupId(g.Id)
		if err := s.resyncSlotMappings(ctx, slots...); err != nil {
			log.Warnf("group-[%d] resync-rollback to prepared due to error: %v", g.Id, err)
			g.Promoting.State = models.ActionPrepared
			s.resyncSlotMappings(ctx, slots...)
			log.Warnf("group-[%d] resync-rollback to prepared, done", g.Id)
			return err
		}
		if err := s.storeUpdateGroup(g); err != nil {
			return err
		}

		fallthrough

	case models.ActionPromoting:

		if p := ctx.sentinel; len(p.Servers) != 0 {
			defer s.dirtySentinelCache()
			p.OutOfSync = true
			if err := s.storeUpdateSentinel(p); err != nil {
				return err
			}
			groupIds := map[int]bool{g.Id: true}
			sentinel := redis.NewSentinel(s.config.ProductName, s.config.ProductAuth)
			if err := sentinel.RemoveGroups(p.Servers, s.config.SentinelClientTimeout.Duration(), groupIds); err != nil {
				log.WarnErrorf(err, "group-[%d] remove sentinels failed", g.Id)
			}
			if s.ha.masters != nil {
				delete(s.ha.masters, gid)
			}
		}

		defer s.dirtyGroupCache(g.Id)

		log.Warnf("group-[%d] resync to promoting", g.Id)

		var index = g.Promoting.Index
		var slice = make([]*models.GroupServer, 0, len(g.Servers))
		slice = append(slice, g.Servers[index])
		for i, x := range g.Servers {
			if i != index && i != 0 {
				slice = append(slice, x)
			}
		}
		slice = append(slice, g.Servers[0])

		for _, x := range slice {
			x.Action.Index = 0
			x.Action.State = models.ActionNothing
		}

		g.Servers = slice
		g.Promoting.Index = 0

		g.Promoting.State = models.ActionFinished
		if err := s.resyncSlotMappings(ctx, ctx.getSlotMappingsByGroupId(g.Id)...); err != nil {
			log.Errorf("group-[%d] resync to finished failed", g.Id)
			return err
		}
		if err := s.storeUpdateGroup(g); err != nil {
			return err
		}
		log.Warnf("group-[%d] resync to finished", g.Id)

		fallthrough

	case models.ActionFinished:

		defer s.dirtyGroupCache(g.Id)

		log.Warnf("group-[%d] resync to finished", g.Id)

		slots := models.SlotMappings(ctx.getSlotMappingsByGroupId(g.Id)).ToSlots()
		for i, x := range g.Servers {
			if i != 0 {
				if err := s.SlaveOfMasterUnsafe(x.Addr, slots, pika.SlaveOfNonForce); err != nil {
					log.Errorf("slaveof master failed for slave %s: %v", x.Addr, err)
				}
			}
		}

		g.ClearPromoting().OutOfSync = false
		if err := s.resyncSlotMappings(ctx, ctx.getSlotMappingsByGroupId(g.Id)...); err != nil {
			log.Errorf("group-[%d] resync to ActionNothing failed", g.Id)
			return err
		}
		if err := s.storeUpdateGroup(g); err != nil {
			return err
		}
		log.Warnf("group-[%d] resync to ActionNothing done", g.Id)
		return nil

	default:
		return errors.Errorf("group-[%d] action state is invalid", gid)
	}
}

func (s *Topom) slaveLagsOK(ctx *context, g *models.Group, masterDown *bool, slaveAddr string, gap uint64,
	createReplLinkIfNeeded bool, onLagOK func(slaveAddr string, needsCheckSlaveAliveness bool) error) error {
	if *masterDown {
		return onLagOK(slaveAddr, true) // if master is down, then is ok to promote
	}

	masterAddr := ctx.getGroupMaster(g.Id)
	if masterAddr == "" {
		*masterDown = true
		return onLagOK(slaveAddr, true) // if master is down, then is ok to promote
	}

	masterSlotsInfo, err := s.action.redisp.GetPikaSlotsInfo(masterAddr)
	if err != nil {
		*masterDown = true
		return onLagOK(slaveAddr, true) // if master is down, then is ok to promote
	}

	slots := ctx.getSlotMappingsByGroupId(g.Id)
	for _, m := range slots {
		masterSlotInfo, ok := masterSlotsInfo[m.Id]
		if !ok {
			return errors.Errorf("can't find master slot info for slot %d of group %d", m.Id, g.Id)
		}
		slaveReplInfo, slotErr := masterSlotInfo.FindSlaveReplInfo(slaveAddr)
		if slotErr != nil {
			if createReplLinkIfNeeded {
				_ = s.action.redisp.SlaveOfAsync(masterAddr, slaveAddr, m.Id, false, false)
			}
			err = errors.Wrap(err, errors.Errorf("%v: slaveAddr:%s,slot:%d", slotErr, slaveAddr, m.Id))
			continue
		}
		if slotErr := slaveReplInfo.GapReached(gap); slotErr != nil {
			err = errors.Wrap(err, errors.Errorf("%v: slaveAddr:%s,slot:%d", slotErr, slaveAddr, m.Id))
			continue
		}
	}
	if err != nil {
		return err
	}
	return onLagOK(slaveAddr, len(slots) == 0)
}

func (s *Topom) trySwitchGroupMaster(gid int, master string, cache *redis.InfoCache) error {
	ctx, err := s.newContext()
	if err != nil {
		return err
	}
	g, err := ctx.getGroup(gid)
	if err != nil {
		return err
	}

	var index = func() int {
		for i, x := range g.Servers {
			if x.Addr == master {
				return i
			}
		}
		for i, x := range g.Servers {
			rid1 := cache.GetRunId(master)
			rid2 := cache.GetRunId(x.Addr)
			if rid1 != "" && rid1 == rid2 {
				return i
			}
		}
		return -1
	}()
	if index == -1 {
		return errors.Errorf("group-[%d] doesn't have server %s with runid = '%s'", g.Id, master, cache.GetRunId(master))
	}
	if index == 0 {
		return nil
	}
	defer s.dirtyGroupCache(g.Id)

	log.Warnf("group-[%d] will switch master to server[%d] = %s", g.Id, index, g.Servers[index].Addr)

	g.Servers[0], g.Servers[index] = g.Servers[index], g.Servers[0]
	g.OutOfSync = true
	return s.storeUpdateGroup(g)
}

func (s *Topom) EnableReplicaGroups(gid int, addr string, value bool) error {
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
	index, err := ctx.getGroupIndex(g, addr)
	if err != nil {
		return err
	}

	if g.Promoting.State != models.ActionNothing {
		return errors.Errorf("group-[%d] is promoting", g.Id)
	}
	defer s.dirtyGroupCache(g.Id)

	if len(g.Servers) != 1 && ctx.isGroupInUse(g.Id) {
		g.OutOfSync = true
	}
	g.Servers[index].ReplicaGroup = value

	return s.storeUpdateGroup(g)
}

func (s *Topom) EnableReplicaGroupsAll(value bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return err
	}

	for _, g := range ctx.group {
		if g.Promoting.State != models.ActionNothing {
			return errors.Errorf("group-[%d] is promoting", g.Id)
		}
		defer s.dirtyGroupCache(g.Id) // TODO isn't this ridiculous?

		var dirty bool
		for _, x := range g.Servers {
			if x.ReplicaGroup != value {
				x.ReplicaGroup = value
				dirty = true
			}
		}
		if !dirty {
			continue
		}
		if len(g.Servers) != 1 && ctx.isGroupInUse(g.Id) {
			g.OutOfSync = true
		}
		if err := s.storeUpdateGroup(g); err != nil {
			return err
		}
	}
	return nil
}

func (s *Topom) SyncCreateAction(addr string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return err
	}

	g, index, err := ctx.getGroupByServer(addr)
	if err != nil {
		return err
	}
	if g.Promoting.State != models.ActionNothing {
		return errors.Errorf("group-[%d] is promoting", g.Id)
	}

	if g.Servers[index].Action.State == models.ActionPending {
		return errors.Errorf("server-[%s] action already exist", addr)
	}
	defer s.dirtyGroupCache(g.Id)

	g.Servers[index].Action.Index = ctx.maxSyncActionIndex() + 1
	g.Servers[index].Action.State = models.ActionPending
	return s.storeUpdateGroup(g)
}

func (s *Topom) SyncRemoveAction(addr string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return err
	}

	g, index, err := ctx.getGroupByServer(addr)
	if err != nil {
		return err
	}
	if g.Promoting.State != models.ActionNothing {
		return errors.Errorf("group-[%d] is promoting", g.Id)
	}

	if g.Servers[index].Action.State == models.ActionNothing {
		return errors.Errorf("server-[%s] action doesn't exist", addr)
	}
	defer s.dirtyGroupCache(g.Id)

	g.Servers[index].Action.Index = 0
	g.Servers[index].Action.State = models.ActionNothing
	return s.storeUpdateGroup(g)
}

func (s *Topom) SyncActionPrepare() (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return "", err
	}

	addr := ctx.minSyncActionIndex()
	if addr == "" {
		return "", nil
	}

	g, index, err := ctx.getGroupByServer(addr)
	if err != nil {
		return "", err
	}
	if g.Promoting.State != models.ActionNothing {
		return "", nil
	}

	if g.Servers[index].Action.State != models.ActionPending {
		return "", errors.Errorf("server-[%s] action state is invalid", addr)
	}
	defer s.dirtyGroupCache(g.Id)

	log.Warnf("server-[%s] action prepare", addr)

	g.Servers[index].Action.Index = 0
	g.Servers[index].Action.State = models.ActionSyncing
	return addr, s.storeUpdateGroup(g)
}

func (s *Topom) SyncActionComplete(addr string, failed bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return err
	}

	g, index, err := ctx.getGroupByServer(addr)
	if err != nil {
		return nil
	}
	if g.Promoting.State != models.ActionNothing {
		return nil
	}

	if g.Servers[index].Action.State != models.ActionSyncing {
		return nil
	}
	defer s.dirtyGroupCache(g.Id)

	log.Warnf("server-[%s] action failed = %t", addr, failed)

	var state string
	if !failed {
		state = "synced"
	} else {
		state = "synced_failed"
	}
	g.Servers[index].Action.State = state
	return s.storeUpdateGroup(g)
}

func (s *Topom) newSyncActionExecutor(addr string) (func() error, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return nil, err
	}

	g, index, err := ctx.getGroupByServer(addr)
	if err != nil {
		return nil, nil
	}

	if g.Servers[index].Action.State != models.ActionSyncing {
		return nil, nil
	}

	slotsInfo, err := s.action.redisp.GetPikaSlotsInfo(addr)
	if err != nil {
		return nil, err
	}
	return func() error {
		return s.SlaveOfMaster(addr, pika.SlotsInfo(slotsInfo).Slots(), pika.SlaveOfMayForce)
	}, nil
}
