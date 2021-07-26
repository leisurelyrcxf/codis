package common

import (
	"fmt"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/CodisLabs/codis/pkg/utils/assert"
)

type t struct{}

func (x *t) String() string {
	if x == nil {
		return "slave"
	}
	return "master"
}

type Role *t

// No way to create other values for type Role outside the package
var (
	RoleSlave  Role = nil
	RoleMaster Role = &t{}
)

type RoleWithMaster struct {
	Role       Role
	MasterAddr string
	Timestamp  time.Time
}

func (r *RoleWithMaster) Different(another *RoleWithMaster) bool {
	return r.Role != another.Role || r.MasterAddr != another.MasterAddr
}

func newSlave(masterAddr string, ts time.Time) *RoleWithMaster {
	return &RoleWithMaster{
		Role:       RoleSlave,
		MasterAddr: masterAddr,
		Timestamp:  ts,
	}
}

func newMaster(ts time.Time) *RoleWithMaster {
	return &RoleWithMaster{
		Role:       RoleMaster,
		MasterAddr: "",
		Timestamp:  ts,
	}
}

type RoleState struct {
	role *RoleWithMaster
}

func NewRoleState() RoleState {
	return RoleState{role: newSlave("", time.Now())}
}

func (r *RoleState) SetMaster(ts time.Time) (old, new_ *RoleWithMaster, success bool) {
	return r.setIfTimestampBigger(newMaster(ts))
}

func (r *RoleState) SetSlave(masterAddr string, ts time.Time) (old, new_ *RoleWithMaster, success bool) {
	return r.setIfTimestampBigger(newSlave(masterAddr, ts))
}

func (r *RoleState) setIfTimestampBigger(new_ *RoleWithMaster) (oldRole, newRole *RoleWithMaster, success bool) {
	for old := r.load(); old.Timestamp.Before(new_.Timestamp); old = r.load() {
		if atomic.CompareAndSwapPointer((*unsafe.Pointer)(unsafe.Pointer(&r.role)), unsafe.Pointer(old), unsafe.Pointer(new_)) {
			return old, new_, true
		}
	}
	return nil, new_, false
}

func (r RoleState) String() string {
	d := r.load()
	role := "master"
	if d.Role == RoleSlave {
		role = "slave"
	}
	return fmt.Sprintf("('%s', '%s', %s)", role, d.MasterAddr, d.Timestamp)
}

func (r *RoleState) IsMaster() bool {
	d := r.load()
	return d.Role == RoleMaster
}

func (r *RoleState) IsSlave(pMasterAddr *string) bool {
	d := r.load()
	if pMasterAddr != nil {
		*pMasterAddr = d.MasterAddr
	}
	return d.Role == RoleSlave
}

// Used only for testing
func (r *RoleState) SetMasterWhetherRoleChanged() bool {
	old_, new_, success := r.SetMaster(time.Now())
	assert.Must(success)
	return new_.Role != old_.Role
}

// Used only for testing
func (r *RoleState) SetSlaveWhetherRoleChanged(masterAddr string) bool {
	old_, new_, success := r.SetSlave(masterAddr, time.Now())
	assert.Must(success)
	return new_.Role != old_.Role
}

func (r *RoleState) load() *RoleWithMaster {
	return (*RoleWithMaster)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&r.role))))
}
