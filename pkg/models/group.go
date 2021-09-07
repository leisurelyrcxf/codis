// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package models

import (
	"github.com/CodisLabs/codis/pkg/utils/errors"
)

const MaxGroupId = 9999

type Group struct {
	Id      int            `json:"id"`
	Servers []*GroupServer `json:"servers"`

	Promoting struct {
		Index           int    `json:"index,omitempty"`
		State           string `json:"state,omitempty"`
		CreatedReplLink bool   `json:"created_repl_link"`
	} `json:"promoting"`

	OutOfSync bool `json:"out_of_sync"`
}

func (g *Group) GetMaster() (string, error) {
	if len(g.Servers) == 0 {
		return "", errors.Errorf("no servers in group %d", g.Id)
	}
	return g.Servers[0].Addr, nil
}

type GroupServer struct {
	Addr       string `json:"server"`
	DataCenter string `json:"datacenter"`

	Action struct {
		Index int    `json:"index,omitempty"`
		State string `json:"state,omitempty"`
	} `json:"action"`

	ReplicaGroup bool `json:"replica_group"`
}

func (g *Group) ClearPromoting() *Group {
	g.Promoting = struct {
		Index           int    `json:"index,omitempty"`
		State           string `json:"state,omitempty"`
		CreatedReplLink bool   `json:"created_repl_link"`
	}{}
	return g
}

func (g *Group) Encode() []byte {
	return jsonEncode(g)
}
