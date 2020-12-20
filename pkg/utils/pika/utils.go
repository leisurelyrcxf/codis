package pika

import (
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"

	"github.com/CodisLabs/codis/pkg/utils/errors"

	"github.com/CodisLabs/codis/pkg/utils/log"
)

var (
	SlaveRegExp                = regexp.MustCompile(`^slave\[\d+\]$`)
	MasterSlaveSeparatorRegExp = regexp.MustCompile(`^---(-+)$`)
	// ErrSlotNotExists error
	ErrSlotNotExists = errors.New("slot not exists")
	// ErrInvalidSlotInfo invalid slot info
	ErrInvalidSlotInfo    = errors.New("invalid slot info")
	genErrInvalidSlotInfo = func(err error) error { return errors.Wrap(ErrInvalidSlotInfo, err) }

	// ErrSlaveReplInfoIsNil const
	ErrSlaveReplInfoIsNil = errors.New("slave repl info is nil")
	// ErrInvalidSlotOffset invalid slot offset
	ErrInvalidSlotOffset = errors.New("invalid slot offset")
	// ErrNotSlaveButHaveMaster invalid master info error
	ErrNotSlaveButHaveMaster = errors.New("not slave but have master address info")
	// ErrNotMasterButHaveSlaves invalid master info error
	ErrNotMasterButHaveSlaves = errors.New("not master but have slaves")
	// ErrSlaveNoMaster invalid master info error
	ErrSlaveNoMaster = errors.New("role is slave but doesn't have master addr")
	// ErrMasterNoSlaves invalid master info error
	ErrMasterNoSlaves = errors.New("role is master but doesn't have any slaves")
)

// ParseSlotsInfo parse slots info string
func ParseSlotsInfo(slotsInfoString string) (slotInfos map[int]SlotInfo, err error) {
	slotInfos = make(map[int]SlotInfo)

	slotInfoStrings := trimmedSplit(slotsInfoString, "\r\n\r\n")
	for _, slotInfoString := range slotInfoStrings {
		slotInfo, err := ParseSlotInfo(slotInfoString)
		if err != nil {
			log.Errorf("[parseSlotInfos] failed to parse slot info string '%s'", slotInfoString)
			return slotInfos, err
		}
		slotInfos[slotInfo.Slot] = slotInfo
	}
	return slotInfos, nil
}

// parseSlotInfo parse slot info string, example slot info string:
// "(db0:0) binlog_offset=0 0,safety_purge=none\r\n\r\n(db0:1) binlog_offset=0 0,safety_purge=none\r\n  Role: Slave\r\n  master: 10.213.20.33:9200"
func ParseSlotInfo(slotInfoString string) (info SlotInfo, err error) {
	if strings.TrimSpace(slotInfoString) == "" {
		return SlotInfo{}, ErrSlotNotExists
	}

	var slotConnectedSlaves int
	info.Slot = -1
	info.FileNum = math.MaxUint64
	info.Offset = math.MaxUint64

	items := trimmedSplit(slotInfoString, "\r\n")
	var slaveReplInfo *SlaveReplInfo
	for _, item := range items {
		colonIndex := strings.Index(item, ":")
		if colonIndex == -1 {
			if !MasterSlaveSeparatorRegExp.MatchString(item) {
				log.Errorf("[parseSlotInfo] no ':' in item '%s'", item)
			}
			continue
		}
		key, value := strings.TrimSpace(item[:colonIndex]), strings.TrimSpace(item[colonIndex+1:])
		if SlaveRegExp.MatchString(key) {
			if !info.Role.IsMaster() {
				log.Errorf("[parseSlotInfo] not master but have slaves")
				return info, genErrInvalidSlotInfo(ErrNotMasterButHaveSlaves)
			}
			if slaveReplInfo != nil {
				info.SlaveReplInfos = append(info.SlaveReplInfos, *slaveReplInfo)
			}
			if value == "" {
				log.Errorf("[parseSlotInfo] slave addr is empty")
				return info, genErrInvalidSlotInfo(errors.New("slave addr is empty"))
			}
			slaveReplInfo = &SlaveReplInfo{Addr: value, Lag: math.MaxUint64}
			continue
		}
		switch key {
		case "lag":
			if slaveReplInfo == nil {
				log.Errorf("[parseSlotInfo] slave repl info is nil")
				return info, genErrInvalidSlotInfo(ErrSlaveReplInfoIsNil)
			}
			var lag uint64
			if lag, err = strconv.ParseUint(value, 10, 64); err != nil {
				log.Errorf("[parseSlotInfo] lag parse fail, lag: %s, err: %v", value, err)
				return info, err
			}
			slaveReplInfo.Lag = lag
		case "replication_status":
			if slaveReplInfo == nil {
				log.Errorf("[parseSlotInfo] slave repl info is nil")
				return info, genErrInvalidSlotInfo(ErrSlaveReplInfoIsNil)
			}
			slaveReplInfo.Status = ParseSlaveBinlogStatus(value)
		case "(db0":
			// "111) binlog_offset=0 0,safety_purge=none"
			slotInfoParts := trimmedSplit(value, ")")
			if len(slotInfoParts) < 2 { // nolint:gomnd
				log.Errorf("[parseSlotInfo] expect 2 parts of slotInfo, but met %v", slotInfoParts)
				return info, genErrInvalidSlotInfo(errors.New(fmt.Sprintf("len(slotInfoParts) < 2(value: '%s')", value)))
			}
			slotS, value := slotInfoParts[0], slotInfoParts[1] // "111", "binlog_offset=0 0,safety_purge=none"
			var slotInt64 int64
			if slotInt64, err = strconv.ParseInt(slotS, 10, 64); err != nil {
				log.Errorf("[parseSlotInfo] failed to parser slot, slot string: '%s', err: '%v'", slotS, err)
				return info, err
			}
			info.Slot = int(slotInt64)

			binlogS := trimmedSplit(value, ",")[0] // "binlog_offset=0 0"
			binlogParts := trimmedSplit(binlogS, "=")
			if len(binlogParts) != 2 { // nolint:gomnd
				log.Errorf("[parseSlotInfo] expecting 2 parts of binlog_offset, but met %v", binlogParts)
				return info, genErrInvalidSlotInfo(ErrInvalidSlotOffset)
			}

			fileNumOffsetS := binlogParts[1] // "0 0"
			fileNumOffsetParts := trimmedSplit(fileNumOffsetS, " ")
			if len(fileNumOffsetParts) != 2 { // nolint:gomnd
				log.Errorf("[parseSlotInfo] expecting 2 parts of fileNumOffset string, but met %v", fileNumOffsetParts)
				return info, genErrInvalidSlotInfo(ErrInvalidSlotOffset)
			}

			fileNumS, offsetS := fileNumOffsetParts[0], fileNumOffsetParts[1]
			if info.FileNum, err = strconv.ParseUint(fileNumS, 10, 64); err != nil {
				log.Errorf("[parseSlotInfo] file number parse failed, file number string: '%s', err: '%v'", fileNumS, err)
				return info, err
			}
			if info.Offset, err = strconv.ParseUint(offsetS, 10, 64); err != nil {
				log.Errorf("[parseSlotInfo] offset parse failed, offset string: '%s', err: '%v'", offsetS, err)
				return info, err
			}
		case "Role":
			info.Role.Update(ParseRole(value))
		case "master":
			if !info.Role.IsSlave() {
				log.Errorf("[parseSlotInfo] not slave but have master addr")
				return info, genErrInvalidSlotInfo(ErrNotSlaveButHaveMaster)
			}
			info.MasterAddr = value
		case "connected_slaves":
			var connectedSlaves int64
			if connectedSlaves, err = strconv.ParseInt(value, 10, 64); err != nil {
				log.Errorf("[parseSlotInfo] parse connected_slaves failed, string: '%s', err: '%v'", value, err)
				return info, err
			}
			slotConnectedSlaves = int(connectedSlaves)
		}
	}

	if slaveReplInfo != nil {
		info.SlaveReplInfos = append(info.SlaveReplInfos, *slaveReplInfo)
	}

	if info.Slot == -1 {
		log.Errorf("[parseSlotInfo] can't parse slot")
		return info, genErrInvalidSlotInfo(errors.New("info.Slot == -1"))
	}
	if info.FileNum == math.MaxUint64 || info.Offset == math.MaxUint64 {
		log.Errorf("[parseSlotInfo] can't parse slot binlog offset")
		return info, genErrInvalidSlotInfo(ErrInvalidSlotOffset)
	}
	if len(info.SlaveReplInfos) != slotConnectedSlaves {
		err := errors.Errorf("len(info.SlaveReplInfos)(%d) != slotConnectedSlaves(%d)", len(info.SlaveReplInfos), slotConnectedSlaves)
		log.Errorf("[parseSlotInfo] %v", err)
		return info, genErrInvalidSlotInfo(err)
	}
	for _, slave := range info.SlaveReplInfos {
		if slave.Status == SlaveStatusNotSync && slave.Lag != math.MaxUint64 {
			err := errors.Errorf(" slave.Status == SlaveStatusNotSync && slave.Lag(%d) != math.MaxUint64", slave.Lag)
			log.Errorf("[parseSlotInfo] %v", err)
			return info, genErrInvalidSlotInfo(err)
		}
	}
	if info.Role.IsMaster() && len(info.SlaveReplInfos) == 0 {
		return info, genErrInvalidSlotInfo(ErrMasterNoSlaves)
	}
	if info.Role.IsSlave() && info.MasterAddr == "" {
		return info, genErrInvalidSlotInfo(ErrSlaveNoMaster)
	}
	return info, nil
}

func trimmedSplit(str string, sep string) []string {
	parts := strings.Split(str, sep)
	trimmedParts := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmedPart := strings.TrimSpace(part)
		if trimmedPart != "" {
			trimmedParts = append(trimmedParts, trimmedPart)
		}
	}
	return trimmedParts
}
