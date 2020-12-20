package pika

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"

	"github.com/CodisLabs/codis/pkg/utils/log"

	"github.com/CodisLabs/codis/pkg/utils/errors"
)

var (
	// ErrInvalidSlotGroup invalid slot group
	ErrInvalidSlotGroup = errors.New("invalid slot group")
	// ErrInvalidSlaveOfForceOption error
	ErrInvalidSlaveOfForceOption = errors.New("invalid slaveof force option")

	ErrMsgSlotAlreadyLinked = "Already Linked To"
)

type Slots []int

func (slots Slots) String() string {
	return fmt.Sprintf("%v", GroupedSlots(slots))
}

type SlotGroup struct {
	From int
	To   int
}

func NewSlotGroup(from, to int) SlotGroup {
	return SlotGroup{
		From: from,
		To:   to,
	}
}

func MakeSlotGroup(slot int) SlotGroup {
	return SlotGroup{
		From: slot,
		To:   slot,
	}
}

func (sg SlotGroup) Validate() error {
	if sg.From < 0 || sg.To < 0 || sg.From > sg.To {
		return ErrInvalidSlotGroup
	}
	return nil
}

func (sg SlotGroup) Len() int {
	return sg.To - sg.From + 1
}

func (sg SlotGroup) String() string {
	if sg.From == sg.To {
		return fmt.Sprintf("%d", sg.From)
	}
	return fmt.Sprintf("%d-%d", sg.From, sg.To)
}

func (sg SlotGroup) ToList() []int {
	slots := []int{}
	for slot := sg.From; slot <= sg.To; slot++ {
		slots = append(slots, slot)
	}
	return slots
}

type SlotGroups []SlotGroup

func (sgs SlotGroups) SlotCount() (count int) {
	for _, sg := range sgs {
		if err := sg.Validate(); err != nil {
			return math.MaxInt32
		}
		count += sg.Len()
	}
	return
}

func GroupedSlotsExcluding(slots []int, excludes []int) []SlotGroup {
	if len(excludes) == 0 {
		return GroupedSlots(slots)
	}
	var (
		newSlots  []int
		mExcludes = map[int]struct{}{}
	)
	for _, exclude := range excludes {
		mExcludes[exclude] = struct{}{}
	}

	for _, slot := range slots {
		if _, ok := mExcludes[slot]; !ok {
			newSlots = append(newSlots, slot)
		}
	}

	return GroupedSlots(newSlots)
}

func GroupedSlots(slots []int) (slotGroups []SlotGroup) {
	sort.Ints(slots)

	var csg *SlotGroup
	for _, slot := range slots {
		if csg == nil {
			csg = &SlotGroup{
				From: slot,
				To:   slot,
			}
		} else {
			if slot == csg.To+1 || slot == csg.To {
				csg.To = slot
			} else {
				slotGroups = append(slotGroups, *csg)
				csg.From = slot
				csg.To = slot
			}
		}
	}
	if csg != nil {
		slotGroups = append(slotGroups, *csg)
	}
	return slotGroups
}

type SlaveOfMasterJobKey struct {
	MasterAddr string
	Force      bool
}

type SlaveOfMasterJob struct {
	SlaveOfMasterJobKey

	Slot        int
	ForceOption SlaveOfForceOption
	Skip        bool
	Err         error
}

func NewSlaveOfMasterJob(masterAddr string, slot int, opt SlaveOfForceOption) *SlaveOfMasterJob {
	return &SlaveOfMasterJob{
		SlaveOfMasterJobKey: SlaveOfMasterJobKey{
			MasterAddr: masterAddr,
		},
		Slot:        slot,
		ForceOption: opt,
	}
}

// ReplJobs is collection of *ReplicationJob
type SlaveOfMasterJobs []*SlaveOfMasterJob

// GroupJobs group jobs by job key.
func (jobs SlaveOfMasterJobs) GroupJobs() map[SlaveOfMasterJobKey]SlaveOfMasterJobs {
	m := make(map[SlaveOfMasterJobKey]SlaveOfMasterJobs)
	for _, j := range jobs {
		m[j.SlaveOfMasterJobKey] = append(m[j.SlaveOfMasterJobKey], j)
	}
	return m
}

// Slots returns slots of ReplJobs
func (jobs SlaveOfMasterJobs) Slots() []int {
	slots := make([]int, 0, len(jobs))
	for _, j := range jobs {
		slots = append(slots, j.Slot)
	}
	sort.Ints(slots)
	return slots
}

type ReplState string

var (
	allRelStates []ReplState
	newReplState = func(str string) ReplState {
		s := ReplState(str)
		allRelStates = append(allRelStates, s)
		return s
	}
	ReplStateNoConnect   = newReplState("kNoConnect")
	_                    = newReplState("kTryConnect")
	_                    = newReplState("kTryDBSync")
	_                    = newReplState("kWaitDBSync")
	_                    = newReplState("kWaitReply")
	_                    = newReplState("kConnected")
	ReplStateError       = newReplState("kError")
	ReplStateDBNoConnect = newReplState("kDBNoConnect")
)

func ParseReplState(replStateString string, replState *ReplState) bool {
	for _, st := range allRelStates {
		if st == ReplState(replStateString) {
			*replState = st
			return true
		}
	}
	log.Errorf("[ParseReplState] unknown repl state: '%v'", replStateString)
	return false
}

func (s ReplState) IsConnected() bool {
	return s != ReplStateNoConnect && s != ReplStateDBNoConnect && s != ReplStateError
}

type SlotReplState struct {
	Slot int
	ReplState
}

type SlotsReplState []SlotReplState

func (ss SlotsReplState) SplitConnectedAndNonConnected() (connectedSlots, nonConnectedSlots []int) {
	for _, s := range ss {
		if s.IsConnected() {
			connectedSlots = append(connectedSlots, s.Slot)
		} else {
			nonConnectedSlots = append(nonConnectedSlots, s.Slot)
		}
	}
	return
}

func ParseAlreadyLinkedErrMsg(msg string) (SlotsReplState, error) {
	lines := trimmedSplit(msg, "||")
	if len(lines) == 0 {
		return nil, errors.Errorf("ParseAlreadyLinkedErrMsg: no lines exist")
	}
	slotsState := make([]SlotReplState, 0, len(lines))
	for _, line := range lines {
		ss, err := parseAlreadyLinkedErrMsgLine(line)
		if err != nil {
			return nil, err
		}
		slotsState = append(slotsState, ss)
	}
	return slotsState, nil
}

func parseAlreadyLinkedErrMsgLine(line string) (SlotReplState, error) {
	const (
		anchorSlot      = "Slot "
		anchorReplState = "repl state: '"
	)
	idx1 := strings.Index(line, anchorSlot)
	idx2 := strings.Index(line, ErrMsgSlotAlreadyLinked)
	idx3 := strings.Index(line, anchorReplState)
	idx4 := strings.LastIndex(line, "'")

	var srs SlotReplState
	if idx1 != -1 && idx2 != -1 && idx1 < idx2 && idx3 != -1 && idx4 != -1 && idx3 < idx4 &&
		parseSlot(line[idx1+len(anchorSlot):idx2], &srs.Slot) &&
		ParseReplState(line[idx3+len(anchorReplState):idx4], &srs.ReplState) {
		return srs, nil
	}
	return SlotReplState{}, errors.Errorf("invalid already linked err message: '%s'", line)
}

func parseSlot(slotString string, slot *int) bool {
	var err error
	*slot, err = strconv.Atoi(strings.TrimSpace(slotString))
	return err == nil
}

type SlaveOfForceOption int

const (
	// SlaveOfNonForce never uses 'force'.
	SlaveOfNonForce SlaveOfForceOption = iota
	// SlaveOfMayForce uses 'force' opportunistically.
	// If slave's binlog is greater than master then use 'force', otherwise don't use.
	SlaveOfMayForce
	// SlaveOfForceForce always uses 'force'.
	SlaveOfForceForce
)

var slaveOfForceOptions = []SlaveOfForceOption{SlaveOfNonForce, SlaveOfMayForce, SlaveOfForceForce}

const (
	SlaveOfNonForceDesc     = "non_force"
	SlaveOfMayForceDesc     = "may_force"
	SlaveOfForceForceDesc   = "force_force"
	SlaveofForceInvalidDesc = "invalid_slaveof_force_opt"
)

var SlaveOfForceOptionDesc = map[SlaveOfForceOption]string{
	SlaveOfNonForce:   SlaveOfNonForceDesc,
	SlaveOfMayForce:   SlaveOfMayForceDesc,
	SlaveOfForceForce: SlaveOfForceForceDesc,
}

func ParseSlaveOfForceOption(i int) (SlaveOfForceOption, error) {
	opt := SlaveOfForceOption(i)
	switch opt {
	case SlaveOfNonForce:
		return SlaveOfNonForce, nil
	case SlaveOfMayForce:
		return SlaveOfMayForce, nil
	case SlaveOfForceForce:
		return SlaveOfForceForce, nil
	default:
		return opt, errors.Errorf("%v: %d", ErrInvalidSlaveOfForceOption, i)
	}
}

func SlaveOfForceOptionUsage() string {
	usages := make([]string, 0, len(slaveOfForceOptions))
	for _, opt := range slaveOfForceOptions {
		usages = append(usages, fmt.Sprintf("%d(%s)", int(opt), opt.String()))
	}
	return strings.Join(usages, ", ")
}

// CanForce indicates whether force is allowed.
func (o SlaveOfForceOption) CanForce() bool {
	return o >= SlaveOfMayForce
}

// ForceForce indicates whether force is force used.
func (o SlaveOfForceOption) ForceForce() bool {
	return o >= SlaveOfForceForce
}

func (o SlaveOfForceOption) Validate() error {
	if _, ok := SlaveOfForceOptionDesc[o]; !ok {
		return errors.Errorf("%v: %d", ErrInvalidSlaveOfForceOption, int(o))
	}
	return nil
}

func (o SlaveOfForceOption) String() string {
	if desc := SlaveOfForceOptionDesc[o]; desc != "" {
		return desc
	}
	return SlaveofForceInvalidDesc
}
