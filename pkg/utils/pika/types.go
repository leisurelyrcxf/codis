package pika

import (
	"errors"
	"fmt"
	"math"
	"strings"
)

var ErrSlaveNotFound = errors.New("slave not found")

// BinlogOffset represents for binlog offset.
type BinlogOffset struct {
	FileNum uint64
	Offset  uint64
}

// Compare compares two BinlogOffsets.
// If bigger return 1, less return -1, equal return 0.
func (bo *BinlogOffset) Compare(another *BinlogOffset) int {
	if another == nil {
		if bo == nil {
			return 0
		}
		return 1
	}
	if bo == nil {
		return -1
	}
	if bo.FileNum < another.FileNum {
		return -1
	}
	if bo.FileNum > another.FileNum {
		return 1
	}
	if bo.Offset < another.Offset {
		return -1
	}
	if bo.Offset > another.Offset {
		return 1
	}
	return 0
}

// SlaveReplInfo
type SlaveReplInfo struct {
	Addr   string
	Lag    uint64
	Status SlaveStatus
}

func (s SlaveReplInfo) IsEmpty() bool {
	return s.Addr == ""
}

func (s SlaveReplInfo) Desc(desc string) string {
	return fmt.Sprintf("[%s] Addr: %s, Lag: %d, Status: %s", desc, s.Addr, s.Lag, s.Status)
}

// SlotInfo
type SlotInfo struct {
	Slot int

	BinlogOffset

	Role Role

	// If Role.IsSlave()
	MasterAddr string

	// If Role.IsMaster()
	ConnectedSlaves int
	SlaveReplInfos  []SlaveReplInfo
}

// HasSlaves return if this slot has slaves
func (i SlotInfo) HasSlaves() bool {
	return i.ConnectedSlaves > 0 || len(i.SlaveReplInfos) > 0
}

func (i *SlotInfo) FindSlaveReplInfo(slaveAddr string) (SlaveReplInfo, error) {
	slaveAddr = strings.Replace(slaveAddr, "localhost", "127.0.0.1", 1)
	for _, slaveReplInfo := range i.SlaveReplInfos {
		if slaveReplInfo.Addr == slaveAddr {
			return slaveReplInfo, nil
		}
	}
	return SlaveReplInfo{}, ErrSlaveNotFound
}

func (i *SlotInfo) SyncedSlaves() (okSlaves []SlaveReplInfo) {
	for _, slave := range i.SlaveReplInfos {
		if slave.Status == SlaveStatusBinlogSync {
			okSlaves = append(okSlaves, slave)
		}
	}
	return
}

func (i *SlotInfo) GetMinReplLag() uint64 {
	minLag := uint64(math.MaxUint64)
	for _, slaveReplInfo := range i.SlaveReplInfos {
		if slaveReplInfo.Status != SlaveStatusBinlogSync {
			continue
		}
		if slaveReplInfo.Lag < minLag {
			minLag = slaveReplInfo.Lag
		}
	}
	return minLag
}

// Role represents role of pika
type Role string

// Role types
const (
	RoleUnknown     Role = ""
	RoleMaster      Role = "master"
	RoleSlave       Role = "slave"
	RoleMasterSlave Role = "master_slave"
)

// DeSlave method, this happens when a pika slaves of no:one
func (r Role) DeSlave() Role {
	switch r {
	case RoleUnknown, RoleMaster:
		return r
	case RoleSlave:
		return RoleUnknown
	case RoleMasterSlave:
		return RoleMaster
	default:
		return RoleUnknown
	}
}

// DeMaster method, this happens when a pika unlinked its slaves
func (r Role) DeMaster() Role {
	switch r {
	case RoleUnknown, RoleSlave:
		return r
	case RoleMaster:
		return RoleUnknown
	case RoleMasterSlave:
		return RoleSlave
	default:
		return RoleUnknown
	}
}

// Update method
func (r *Role) Update(another Role) {
	if r == nil {
		return
	}
	switch *r {
	case RoleUnknown:
		*r = another
	case RoleMaster:
		if another == RoleSlave || another == RoleMasterSlave {
			*r = RoleMasterSlave
		}
	case RoleSlave:
		if another == RoleMaster || another == RoleMasterSlave {
			*r = RoleMasterSlave
		}
	}
}

// IsMaster method
func (r Role) IsMaster() bool {
	return r == RoleMaster || r == RoleMasterSlave
}

// IsSlave method
func (r Role) IsSlave() bool {
	return r == RoleSlave || r == RoleMasterSlave
}

// ParseRole parse pika role from string representation
func ParseRole(roleStr string) Role {
	roleStr = strings.ToLower(roleStr)
	if roleStr == string(RoleMaster) {
		return RoleMaster
	}
	if roleStr == string(RoleSlave) {
		return RoleSlave
	}
	if roleStr == string(RoleMasterSlave) {
		return RoleMasterSlave
	}
	return RoleUnknown
}

// String method
func (r Role) String() string {
	if r == RoleUnknown {
		return "unknown"
	}
	return string(r)
}

// SlaveStatus represent binlog status
type SlaveStatus string

// SlaveStatus types
const (
	SlaveStatusUnknown    SlaveStatus = ""
	SlaveStatusBinlogSync SlaveStatus = "SlaveBinlogSync"
	SlaveStatusNotSync    SlaveStatus = "SlaveNotSync"
)

// ParseSlaveBinlogStatus parse binlog status
func ParseSlaveBinlogStatus(str string) SlaveStatus {
	if str == string(SlaveStatusBinlogSync) {
		return SlaveStatusBinlogSync
	}
	if str == string(SlaveStatusNotSync) {
		return SlaveStatusNotSync
	}
	return SlaveStatus(str) // TODO fix this
}

// String method
func (r SlaveStatus) String() string {
	if r == SlaveStatusUnknown {
		return "unknown"
	}
	return string(r)
}
