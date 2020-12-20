package pika

import (
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/CodisLabs/codis/pkg/utils/errors"
	"github.com/CodisLabs/codis/pkg/utils/log"
)

const ErrMsgLagNotMatch = "lag not match"

var (
	ErrSlaveNotFound = errors.New("slave not found")

	InvalidSlaveReplInfo = func(addr string) SlaveReplInfo {
		return SlaveReplInfo{
			Addr:   addr,
			Lag:    math.MaxUint64,
			Status: SlaveStatusUnknown,
		}
	}
)

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

func (s SlaveReplInfo) GapReached(gap uint64) error {
	if s.Status != SlaveStatusBinlogSync {
		return errors.Errorf("slave status not match, exp: %s, actual: %s", SlaveStatusBinlogSync, s.Status)
	}

	if s.Lag > gap {
		return errors.Errorf("%s,lag(%d)>gap(%d)", ErrMsgLagNotMatch, s.Lag, gap)
	}

	return nil
}

func (s SlaveReplInfo) IsBinlogSynced() bool {
	return s.Addr != "" && s.Status == SlaveStatusBinlogSync
}

// SlotInfo
type SlotInfo struct {
	Slot int

	BinlogOffset

	Role Role

	// If Role.IsSlave()
	MasterAddr string

	// If Role.IsMaster()
	SlaveReplInfos []SlaveReplInfo
}

// NewSlotInfo generates slot info for newly created slot.
func NewSlotInfo(slot int) SlotInfo {
	return SlotInfo{Slot: slot}
}

// HasSlaves return if this slot has slaves
func (i SlotInfo) HasSlaves() bool {
	return len(i.SlaveReplInfos) > 0
}

func (i SlotInfo) SlaveAddrs() (slaveAddrs []string) {
	for _, slaveReplInfo := range i.SlaveReplInfos {
		slaveAddrs = append(slaveAddrs, slaveReplInfo.Addr)
	}
	return slaveAddrs
}

// FindSlaveReplInfo find slave repl info
func (i SlotInfo) FindSlaveReplInfo(slaveAddr string) (SlaveReplInfo, error) {
	slaveAddr = strings.Replace(slaveAddr, "localhost", "127.0.0.1", 1)
	for _, slaveReplInfo := range i.SlaveReplInfos {
		if slaveReplInfo.Addr == slaveAddr {
			return slaveReplInfo, nil
		}
	}
	return InvalidSlaveReplInfo(slaveAddr), ErrSlaveNotFound
}

func (i SlotInfo) IsSlaveLinked(slaveAddr string) bool {
	_, err := i.FindSlaveReplInfo(slaveAddr)
	return err == nil
}

func (i SlotInfo) IsLinkedToMaster(masterAddr string) bool {
	return i.MasterAddr == masterAddr
}

func (i SlotInfo) LinkedSlaves(slaveAddrs []string) (linkedSlaves []string) {
	for _, slave := range slaveAddrs {
		if i.IsSlaveLinked(slave) {
			linkedSlaves = append(linkedSlaves, slave)
		}
	}
	return
}

// BinlogSyncedSlaves returns slaves which are in normal sync state to master
func (i SlotInfo) BinlogSyncedSlaves() (okSlaves []SlaveReplInfo) {
	for _, slave := range i.SlaveReplInfos {
		if slave.IsBinlogSynced() {
			okSlaves = append(okSlaves, slave)
		}
	}
	return
}

func (i SlotInfo) GetMinReplLag(slaveAddrs []string) uint64 {
	minLag := uint64(math.MaxUint64)
	for _, slave := range slaveAddrs {
		slaveReplInfo, err := i.FindSlaveReplInfo(slave)
		if err != nil {
			continue
		}
		if slaveReplInfo.Lag < minLag {
			minLag = slaveReplInfo.Lag
		}
	}
	return minLag
}

func (i SlotInfo) GapReached(slaveAddr string, gap uint64) error {
	slaveReplInfo, err := i.FindSlaveReplInfo(slaveAddr)
	if err != nil {
		return errors.Errorf("%v: slaveAddr:%s,slot:%d", err, slaveAddr, i.Slot)
	}
	if err := slaveReplInfo.GapReached(gap); err != nil {
		return errors.Errorf("%v: slaveAddr:%s,slot:%d", err, slaveAddr, i.Slot)
	}
	return nil
}

// BecomeMaster generates new proper pika slot info after becoming master. TODO test against this
func (i SlotInfo) BecomeMaster() SlotInfo {
	return SlotInfo{
		Slot:           i.Slot,
		BinlogOffset:   i.BinlogOffset,
		Role:           i.Role.DeSlave(),
		MasterAddr:     "",
		SlaveReplInfos: i.SlaveReplInfos,
	}
}

// UnlinkSlaves generates new proper pika slot info after slaves unlinked. TODO test against this
func (i SlotInfo) UnlinkSlaves() SlotInfo {
	return SlotInfo{
		Slot:           i.Slot,
		BinlogOffset:   i.BinlogOffset,
		Role:           i.Role.DeMaster(),
		MasterAddr:     i.MasterAddr,
		SlaveReplInfos: nil,
	}
}

type SlotsInfo map[int]SlotInfo

func (ssi SlotsInfo) Slots() (slots []int) {
	for s := range ssi {
		slots = append(slots, s)
	}
	sort.Ints(slots)
	return
}

func (ssi SlotsInfo) GetSlaveAddrs() []string {
	slaveAddrs := make(map[string]struct{})
	for _, slotInfo := range ssi {
		for _, slaveReplInfo := range slotInfo.SlaveReplInfos {
			slaveAddrs[slaveReplInfo.Addr] = struct{}{}
		}
	}
	var slaveAddrList []string
	for slaveAddr := range slaveAddrs {
		slaveAddrList = append(slaveAddrList, slaveAddr)
	}
	sort.Strings(slaveAddrList)
	return slaveAddrList
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
	Role := Role(strings.ToLower(roleStr))
	if Role == RoleMaster {
		return RoleMaster
	}
	if Role == RoleSlave {
		return RoleSlave
	}
	if Role == RoleMasterSlave {
		return RoleMasterSlave
	}
	log.Errorf("[ParseRole] unknown pika role %s", roleStr)
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
	ss := SlaveStatus(str)
	if ss == SlaveStatusBinlogSync {
		return SlaveStatusBinlogSync
	}
	if ss == SlaveStatusNotSync {
		return SlaveStatusNotSync
	}
	log.Errorf("[ParseSlaveBinlogStatus] unknown slave status %s", str)
	return ss
	// return SlaveStatusUnknown TODO fix this after we get full list of this
}

// String method
func (r SlaveStatus) String() string {
	if r == SlaveStatusUnknown {
		return "unknown"
	}
	return string(r)
}
