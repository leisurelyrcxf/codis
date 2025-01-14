// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package topom

import (
	"net"
	"strings"
	"sync"
	"time"

	"github.com/CodisLabs/codis/pkg/models"
	"github.com/CodisLabs/codis/pkg/utils"
	"github.com/CodisLabs/codis/pkg/utils/errors"
	"github.com/CodisLabs/codis/pkg/utils/log"
	"github.com/CodisLabs/codis/pkg/utils/math2"
)

//var MaxSlotNum int = models.MaxSlotNum

type context struct {
	slots []*models.SlotMapping
	group map[int]*models.Group
	proxy map[string]*models.Proxy

	sentinel *models.Sentinel

	hosts struct {
		sync.Mutex
		m map[string]net.IP
	}
	method int
	config *Config
}

func (ctx *context) getSlotMapping(sid int) (*models.SlotMapping, error) {
	if len(ctx.slots) != ctx.config.MaxSlotNum {
		return nil, errors.Errorf("invalid number of slots = %d/%d", len(ctx.slots), ctx.config.MaxSlotNum)
	}
	if sid >= 0 && sid < ctx.config.MaxSlotNum {
		if sm := ctx.slots[sid]; sm != nil {
			return sm, nil
		}
		return nil, errors.Errorf("slot-[%d] is nil", sid)
	}
	return nil, errors.Errorf("slot-[%d] doesn't exist", sid)
}

func (ctx *context) getSlotMappingsByGroupId(gid int) []*models.SlotMapping {
	var slots = []*models.SlotMapping{}
	for _, m := range ctx.slots {
		if m.GroupId == gid || m.Action.TargetId == gid {
			slots = append(slots, m)
		}
	}
	return slots
}

func (ctx *context) getSlotsOfGroup(gid int) []int {
	var slots = make([]int, 0, 4)
	for _, m := range ctx.slots {
		if m.GroupId == gid || m.Action.TargetId == gid {
			slots = append(slots, m.Id)
		}
	}
	return slots
}

func (ctx *context) maxSlotActionIndex() (maxIndex int) {
	for _, m := range ctx.slots {
		if m.Action.State != models.ActionNothing {
			maxIndex = math2.MaxInt(maxIndex, m.Action.Index)
		}
	}
	return maxIndex
}

func (ctx *context) isSlotLocked(m *models.SlotMapping) bool {
	switch m.Action.State {
	case models.ActionNothing, models.ActionPending:
		return ctx.isGroupLocked(m.GroupId)
	case models.ActionPreparing, models.ActionWatching:
		return ctx.isGroupLocked(m.GroupId)
	case models.ActionPrepared:
		return true
	case models.ActionMigrating, models.ActionCleanup:
		return ctx.isGroupLocked(m.GroupId) || ctx.isGroupLocked(m.Action.TargetId)
	case models.ActionFinished:
		return ctx.isGroupLocked(m.Action.TargetId)
	default:
		log.Panicf("slot-[%d] action state is invalid:\n%s", m.Id, m.Encode())
	}
	return false
}

func (ctx *context) toSlot(m *models.SlotMapping, p *models.Proxy) *models.Slot {
	slot := &models.Slot{
		Id:     m.Id,
		Locked: ctx.isSlotLocked(m),

		ForwardMethod: ctx.method,
	}
	if m.Stopped {
		return slot
	}
	switch m.Action.State {
	case models.ActionNothing, models.ActionPending:
		slot.BackendAddr = ctx.getGroupMaster(m.GroupId)
		slot.BackendAddrGroupId = m.GroupId
		slot.ReplicaGroups = ctx.toReplicaGroups(m.GroupId, p)
	case models.ActionPreparing, models.ActionWatching:
		slot.BackendAddr = ctx.getGroupMaster(m.GroupId)
		slot.BackendAddrGroupId = m.GroupId
	case models.ActionPrepared, models.ActionMigrating, models.ActionCleanup:
		slot.BackendAddr = ctx.getGroupMaster(m.Action.TargetId)
		slot.BackendAddrGroupId = m.Action.TargetId
		slot.MigrateFrom = ctx.getGroupMaster(m.GroupId)
		slot.MigrateFromGroupId = m.GroupId
	case models.ActionFinished:
		slot.BackendAddr = ctx.getGroupMaster(m.Action.TargetId)
		slot.BackendAddrGroupId = m.Action.TargetId
	default:
		log.Panicf("slot-[%d] action state is invalid:\n%s", m.Id, m.Encode())
	}
	return slot
}

func (ctx *context) lookupIPAddr(addr string) net.IP {
	ctx.hosts.Lock()
	defer ctx.hosts.Unlock()
	ip, ok := ctx.hosts.m[addr]
	if !ok {
		if tcpAddr := utils.ResolveTCPAddrTimeout(addr, 50*time.Millisecond); tcpAddr != nil {
			ctx.hosts.m[addr] = tcpAddr.IP
			return tcpAddr.IP
		} else {
			ctx.hosts.m[addr] = nil
			return nil
		}
	}
	return ip
}

func (ctx *context) lookupPika(addr string) (pidInGroup int, _ *models.Group, _ error) {
	for _, g := range ctx.group {
		for pid, s := range g.Servers {
			if strings.ReplaceAll(s.Addr, "localhost", "127.0.0.1") == addr {
				return pid, g, nil
			}
		}
	}
	return -1, nil, errors.Errorf("can't find pika addr: '%s'", addr)
}

func (ctx *context) toReplicaGroups(gid int, p *models.Proxy) [][]string {
	g := ctx.group[gid]
	switch {
	case g == nil:
		return nil
	case g.Promoting.State != models.ActionNothing:
		return nil
	case len(g.Servers) <= 1:
		return nil
	}
	var dc string
	var ip net.IP
	if p != nil {
		dc = p.DataCenter
		ip = ctx.lookupIPAddr(p.AdminAddr)
	}
	getPriority := func(s *models.GroupServer, pid int) int {
		if ip == nil || dc != s.DataCenter || (ctx.config.IsBackendReadSlavesOnly() && pid == 0) {
			return 2
		}
		if ip.Equal(ctx.lookupIPAddr(s.Addr)) {
			return 0
		} else {
			return 1
		}
	}
	var (
		groups [3][]string
	)
	for pid, s := range g.Servers {
		if s.ReplicaGroup {
			p := getPriority(s, pid)
			groups[p] = append(groups[p], s.Addr)
		}
	}
	var replicas [][]string
	for _, l := range groups {
		if len(l) != 0 {
			replicas = append(replicas, l)
		}
	}
	return replicas
}

func (ctx *context) toSlotSlice(slots []*models.SlotMapping, p *models.Proxy) []*models.Slot {
	var slice = make([]*models.Slot, len(slots))
	for i, m := range slots {
		slice[i] = ctx.toSlot(m, p)
	}
	return slice
}

func (ctx *context) getGroup(gid int) (*models.Group, error) {
	if g := ctx.group[gid]; g != nil {
		return g, nil
	}
	return nil, errors.Errorf("group-[%d] doesn't exist", gid)
}

func (ctx *context) getGroupIndex(g *models.Group, addr string) (int, error) {
	for i, x := range g.Servers {
		if x.Addr == addr {
			return i, nil
		}
	}
	return -1, errors.Errorf("group-[%d] doesn't have server-[%s]", g.Id, addr)
}

func (ctx *context) getGroupByServer(addr string) (*models.Group, int, error) {
	for _, g := range ctx.group {
		for i, x := range g.Servers {
			if x.Addr == addr {
				return g, i, nil
			}
		}
	}
	return nil, -1, errors.Errorf("server-[%s] doesn't exist", addr)
}

func (ctx *context) getAddrToGroup() map[string]struct {
	*models.Group
	PidInGroup int
} {
	m := make(map[string]struct {
		*models.Group
		PidInGroup int
	})
	for _, g := range ctx.group {
		for i, x := range g.Servers {
			m[x.Addr] = struct {
				*models.Group
				PidInGroup int
			}{Group: g, PidInGroup: i}
		}
	}
	return m
}

func (ctx *context) maxSyncActionIndex() (maxIndex int) {
	for _, g := range ctx.group {
		for _, x := range g.Servers {
			if x.Action.State == models.ActionPending {
				maxIndex = math2.MaxInt(maxIndex, x.Action.Index)
			}
		}
	}
	return maxIndex
}

func (ctx *context) minSyncActionIndex() string {
	var d *models.GroupServer
	for _, g := range ctx.group {
		for _, x := range g.Servers {
			if x.Action.State == models.ActionPending {
				if d == nil || x.Action.Index < d.Action.Index {
					d = x
				}
			}
		}
	}
	if d == nil {
		return ""
	}
	return d.Addr
}

func (ctx *context) getGroupMaster(gid int) string {
	if g := ctx.group[gid]; g != nil && len(g.Servers) != 0 {
		return g.Servers[0].Addr
	}
	return ""
}

func (ctx *context) getGroupSlaves(gid int) []string {
	if g := ctx.group[gid]; g != nil && len(g.Servers) > 1 {
		slaves := []string{}
		for i := 1; i < len(g.Servers); i++ {
			slaves = append(slaves, g.Servers[i].Addr)
		}
		return slaves
	}
	return []string{}
}

func (ctx *context) getGroupMasterSlaves(gid int) []string {
	return append([]string{ctx.getGroupMaster(gid)}, ctx.getGroupSlaves(gid)...)
}

func (ctx *context) getGroupSlavesMaster(gid int) []string {
	return append(ctx.getGroupSlaves(gid), ctx.getGroupMaster(gid))
}

func (ctx *context) getGroupMasters() map[int]string {
	var masters = make(map[int]string)
	for _, g := range ctx.group {
		if len(g.Servers) != 0 {
			masters[g.Id] = g.Servers[0].Addr
		}
	}
	return masters
}

func (ctx *context) getGroupIds() map[int]bool {
	var groups = make(map[int]bool)
	for _, g := range ctx.group {
		groups[g.Id] = true
	}
	return groups
}

func (ctx *context) isGroupInUse(gid int) bool {
	for _, m := range ctx.slots {
		if m.GroupId == gid || m.Action.TargetId == gid {
			return true
		}
	}
	return false
}

func (ctx *context) isGroupLocked(gid int) bool {
	if g := ctx.group[gid]; g != nil {
		switch g.Promoting.State {
		case models.ActionNothing:
			return false
		case models.ActionPending:
			return false
		case models.ActionPreparing:
			return false
		case models.ActionWatching:
			return false
		case models.ActionPrepared:
			return true
		case models.ActionPromoting:
			return true
		case models.ActionFinished:
			return false
		default:
			log.Panicf("invalid state of group-[%d] = %s", g.Id, g.Encode())
		}
	}
	return false
}

func (ctx *context) isGroupPromoting(gid int) bool {
	if g := ctx.group[gid]; g != nil {
		return g.Promoting.State != models.ActionNothing
	}
	return false
}

func (ctx *context) getProxy(token string) (*models.Proxy, error) {
	if p := ctx.proxy[token]; p != nil {
		return p, nil
	}
	return nil, errors.Errorf("proxy-[%s] doesn't exist", token)
}

func (ctx *context) maxProxyId() (maxId int) {
	for _, p := range ctx.proxy {
		maxId = math2.MaxInt(maxId, p.Id)
	}
	return maxId
}

func (ctx *context) OughtMaster(m *models.SlotMapping, g *models.Group) (string, error) {
	switch g.Id {
	case m.GroupId:
		return g.GetMaster()
	case m.Action.TargetId:
		sg, err := ctx.getGroup(m.GroupId)
		if err != nil {
			return "", err
		}
		switch m.Action.State {
		case models.ActionPending, models.ActionPreparing:
			return "", nil
		case models.ActionWatching, models.ActionPrepared:
			return sg.GetMaster()
		case models.ActionMigrating, models.ActionCleanup, models.ActionFinished:
			return g.GetMaster()
		default:
			return "", errors.Errorf("invalid state '%s'", m.Action.State)
		}
	default:
		return "", errors.Errorf("group %d neither source %d nor target %d", g.Id, m.GroupId, m.Action.TargetId)
	}
}
