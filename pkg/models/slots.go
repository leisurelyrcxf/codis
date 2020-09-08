// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package models

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

const (
	ForwardSync = iota
	ForwardSemiAsync
)

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

type SlotMapping struct {
	Id      int `json:"id"`
	GroupId int `json:"group_id"`

	Action struct {
		Index    int    `json:"index,omitempty"`
		State    string `json:"state,omitempty"`
		TargetId int    `json:"target_id,omitempty"`

		Info struct {
			SourceMaster string     `json:"source_addr,omitempty"`
			TargetMaster string     `json:"target_addr,omitempty"`
			Progress     string     `json:"progress,omitempty"`
			StateStart   *time.Time `json:"state_start,omitempty"`
		} `json:"info"`
	} `json:"action"`
}

func (m *SlotMapping) GetStateStart() time.Time {
	if ss := m.Action.Info.StateStart; ss != nil {
		return *ss
	}
	return time.Time{}
}

func (m *SlotMapping) UpdateStateStart() {
	t := time.Now()
	m.Action.Info.StateStart = &t
}

func (m *SlotMapping) ClearAction() {
	*m = SlotMapping{
		Id:      m.Id,
		GroupId: m.Action.TargetId,
	}
}

func (m *SlotMapping) ClearActionInfo() {
	// Write like this so dev will never forget to clear new members
	m.Action.Info = struct {
		SourceMaster string     `json:"source_addr,omitempty"`
		TargetMaster string     `json:"target_addr,omitempty"`
		Progress     string     `json:"progress,omitempty"`
		StateStart   *time.Time `json:"state_start,omitempty"`
	}{}
}

func (m *SlotMapping) Encode() []byte {
	return jsonEncode(m)
}

func (m *SlotMapping) String() string {
	b, err := json.Marshal(m)
	if err != nil {
		return fmt.Sprintf("{SlotMappingMarshallErr:%v}", err)
	}
	return string(b)
}
