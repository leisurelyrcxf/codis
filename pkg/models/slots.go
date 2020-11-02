// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package models

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/CodisLabs/codis/pkg/utils/errors"
	"github.com/CodisLabs/codis/pkg/utils/pika"
	"github.com/CodisLabs/codis/pkg/utils/rpc"
	"github.com/CodisLabs/codis/pkg/utils/trace"
)

const (
	ForwardSync = iota
	ForwardSemiAsync
)

//const MaxSlotNum = 1024

type Slot struct {
	Id     int  `json:"id"`
	Locked bool `json:"locked,omitempty"`

	BackendAddr        string `json:"backend_addr,omitempty"`
	BackendAddrGroupId int    `json:"backend_addr_group_id,omitempty"`
	MigrateFrom        string `json:"migrate_from,omitempty"`
	MigrateFromGroupId int    `json:"migrate_from_group_id,omitempty"`

	ForwardMethod int `json:"forward_method,omitempty"`

	ReplicaGroups [][]string `json:"replica_groups,omitempty"`
}

func ParseForwardMethod(s string) (int, bool) {
	switch strings.ToUpper(s) {
	default:
		return ForwardSync, false
	case "SYNC":
		return ForwardSync, true
	case "SEMI-ASYNC":
		return ForwardSemiAsync, true
	}
}

type SlotMigrationProgress struct {
	Err *struct {
		Cause string      `json:"cause"`
		Stack trace.Stack `json:"Stack,omitempty"`
	} `json:"err,omitempty"`
	Main struct {
		SourceMaster string              `json:"source_master"`
		TargetMaster *pika.SlaveReplInfo `json:"target_master"`
	} `json:"main"`
	Backup struct {
		TargetMaster string               `json:"target_master"`
		TargetSlaves []pika.SlaveReplInfo `json:"target_slaves"`
	} `json:"backup"`
	RollbackTimes int `json:"rollback_times"`
}

func NewSlotMigrationProgress(sourceMaster, targetMaster string, rollbackTimes int, err error) SlotMigrationProgress {
	p := SlotMigrationProgress{}
	p.Main.SourceMaster = sourceMaster
	p.Backup.TargetMaster = targetMaster
	p.RollbackTimes = rollbackTimes
	p.Err = convertToSlotMigrationProgressErr(err)
	return p
}

func (p SlotMigrationProgress) IsEmpty() bool {
	return p.Main.SourceMaster == "" && p.Main.TargetMaster == nil &&
		p.Backup.TargetMaster == "" && len(p.Backup.TargetSlaves) == 0 &&
		p.Err == nil && p.RollbackTimes == 0
}

func convertToSlotMigrationProgressErr(err error) *struct {
	Cause string      `json:"cause"`
	Stack trace.Stack `json:"Stack,omitempty"`
} {
	if err == nil {
		return nil
	}

	ret := &struct {
		Cause string      `json:"cause"`
		Stack trace.Stack `json:"Stack,omitempty"`
	}{}
	switch err := err.(type) {
	case *errors.TracedError:
		ret.Cause = fmt.Sprintf("%v", err.Cause)
		ret.Stack = err.Stack
	case *rpc.RemoteError:
		ret.Cause = err.Cause
		ret.Stack = err.Stack
	default:
		ret.Cause = fmt.Sprintf("%v", err)
	}
	return ret
}

type CachedSlotInfo struct {
	pika.SlotInfo
	expire time.Time
}

func NewCachedSlotInfo(slotInfo pika.SlotInfo, ttl time.Duration) *CachedSlotInfo {
	return &CachedSlotInfo{
		SlotInfo: slotInfo,
		expire:   time.Now().Add(ttl),
	}
}

func (c *CachedSlotInfo) IsExpired() bool {
	return time.Since(c.expire) > 0
}

type SlotMapping struct {
	Id      int `json:"id"`
	GroupId int `json:"group_id"`

	Action struct {
		Index            int    `json:"index,omitempty"`
		State            string `json:"state,omitempty"`
		TargetId         int    `json:"target_id,omitempty"`
		Resharding       bool   `json:"resharding,omitempty"`
		SourceMaxSlotNum int    `json:"source_max_slot_num,omitempty"`

		Info struct {
			SourceMaster         string                 `json:"source_master,omitempty"`
			TargetMaster         string                 `json:"target_master,omitempty"`
			StateStart           *time.Time             `json:"state_start,omitempty"`
			Progress             *SlotMigrationProgress `json:"progress,omitempty"`
			SourceMasterSlotInfo *CachedSlotInfo        `json:"-"`
			TargetMasterSlotInfo *CachedSlotInfo        `json:"-"`
		} `json:"info"`
	} `json:"action"`
	Stopped bool `json:"stopped,omitempty"`
}

func (m *SlotMapping) GetSourceMasterSlot() int {
	if !m.Action.Resharding {
		return m.Id
	}
	return m.Id % m.Action.SourceMaxSlotNum
}

func (m *SlotMapping) GetStateStart() time.Time {
	if ss := m.Action.Info.StateStart; ss != nil {
		return *ss
	}
	return time.Time{}
}

func (m *SlotMapping) UpdateState(state string) *SlotMapping {
	switch state {
	case ActionNothing, ActionPending, ActionPreparing, ActionWatching, ActionPrepared, ActionMigrating, ActionCleanup, ActionFinished:
		break
	default:
		panic(fmt.Sprintf("invalid state %s", state))
	}
	m.Action.State = state
	return m
}

func (m *SlotMapping) UpdateStateStart() *SlotMapping {
	t := time.Now()
	m.Action.Info.StateStart = &t
	return m
}

func (m *SlotMapping) ClearAction() *SlotMapping {
	*m = SlotMapping{
		Id:      m.Id,
		GroupId: m.Action.TargetId,
	}
	return m
}

func (m *SlotMapping) ClearActionInfo() *SlotMapping {
	// Write like this so dev will never forget to clear new members
	m.Action.Info = struct {
		SourceMaster         string                 `json:"source_master,omitempty"`
		TargetMaster         string                 `json:"target_master,omitempty"`
		StateStart           *time.Time             `json:"state_start,omitempty"`
		Progress             *SlotMigrationProgress `json:"progress,omitempty"`
		SourceMasterSlotInfo *CachedSlotInfo        `json:"-"`
		TargetMasterSlotInfo *CachedSlotInfo        `json:"-"`
	}{}
	return m
}

func (m *SlotMapping) Encode() []byte {
	return jsonEncode(m.copyAndClearProgress())
}

func (m *SlotMapping) String() string {
	if m == nil {
		return "<nil>"
	}
	b, err := json.Marshal(m.copyAndClearProgress())
	if err != nil {
		return fmt.Sprintf("{SlotMappingStringErr:%v}", err)
	}
	return string(b)
}

func (m *SlotMapping) copyAndClearProgress() *SlotMapping {
	if m == nil {
		return m
	}
	tm := *m
	tm.Action.Info.Progress = nil
	return &tm
}

func (m *SlotMapping) ClearCachedSlotInfo(masterAddrs ...string) {
	for _, addr := range masterAddrs {
		if addr == m.Action.Info.SourceMaster {
			m.Action.Info.SourceMasterSlotInfo = nil
		} else if addr == m.Action.Info.TargetMaster {
			m.Action.Info.TargetMasterSlotInfo = nil
		}
	}
}
