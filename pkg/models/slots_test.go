package models

import (
	"fmt"
	"testing"

	"github.com/CodisLabs/codis/pkg/utils/assert"
)

func TestSlots(t *testing.T) {
	m := &SlotMapping{}
	m.Id = 1
	m.GroupId = 1
	m.Action.Index = 1
	m.Action.State = ActionPending
	m.Action.TargetId = 2
	p := NewSlotMigrationProgress("127.0.0.1", "127.0.0.2", fmt.Errorf("first error"))
	m.Action.Info.Progress = &p
	m.Action.Info.TargetMaster = "127.0.0.1:2333"
	assert.Must(m.Action.Info.Progress.Err.Cause == "first error")
	tm := m.copyAndClearProgress()
	assert.Must(tm.Action.Info.Progress == nil)
	assert.Must(tm.Action.State == ActionPending)
	assert.Must(m.Action.Info.Progress != nil)

	m.ClearActionInfo()
	assert.Must(m.Action.Info.TargetMaster == "")
	assert.Must(m.Action.Info.Progress == nil)
	assert.Must(m.Action.State == ActionPending)
	assert.Must(m.Action.Index == 1)

	m.ClearAction()
	assert.Must(m.Action.Index == 0)
	assert.Must(m.Action.State == ActionNothing)
	assert.Must(m.Action.TargetId == 0)
	assert.Must(m.Action.Info.Progress == nil)
}
