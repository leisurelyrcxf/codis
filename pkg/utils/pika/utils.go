package pika

import (
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"

	"github.com/CodisLabs/codis/pkg/utils/log"
)

var (
	pikaSlaveRegExp            = regexp.MustCompile(`^slave\[\d+\]$`)
	masterSlaveSeparatorRegExp = regexp.MustCompile(`^---(-+)$`)
	// ErrSlotNotExists error
	ErrSlotNotExists = fmt.Errorf("slot not exists")
	// ErrInvalidSlotInfo invalid slot info
	ErrInvalidSlotInfo = fmt.Errorf("invalid slot info")
	// ErrInvalidSlaveReplInfo invalid slave replication info
	ErrInvalidSlaveReplInfo = fmt.Errorf("invalid slave replication info")
	// ErrInvalidSlotOffset invalid slot offset
	ErrInvalidSlotOffset = fmt.Errorf("invalid slot offset")
	// ErrNotSlaveButHaveMasterAddr invalid master info error
	ErrNotSlaveButHaveMasterAddr = fmt.Errorf("not slave but have master address info")
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

	info.Slot = -1
	info.FileNum = math.MaxUint64
	info.Offset = math.MaxUint64

	items := trimmedSplit(slotInfoString, "\r\n")
	var slaveReplInfo *SlaveReplInfo
	for _, item := range items {
		colonIndex := strings.Index(item, ":")
		if colonIndex == -1 {
			if !masterSlaveSeparatorRegExp.MatchString(item) {
				log.Errorf("[parseSlotInfo] no ':' in item '%s'", item)
			}
			continue
		}
		key, value := strings.TrimSpace(item[:colonIndex]), strings.TrimSpace(item[colonIndex+1:])
		if info.Role.IsMaster() && pikaSlaveRegExp.MatchString(key) {
			slaveReplInfo = &SlaveReplInfo{Addr: value, Lag: math.MaxUint64}
			continue
		}
		switch key {
		case "lag":
			if slaveReplInfo == nil {
				log.Errorf("[parseSlotInfo] slave repl info is nil")
				return info, ErrInvalidSlaveReplInfo
			}
			if slaveReplInfo.Lag, err = strconv.ParseUint(value, 10, 64); err != nil {
				log.Errorf("[parseSlotInfo] lag parse fail, lag: %s, err: %v", value, err)
				return info, err
			}
			info.SlaveReplInfos = append(info.SlaveReplInfos, *slaveReplInfo)
			slaveReplInfo = nil
		case "replication_status":
			if slaveReplInfo == nil {
				log.Errorf("[parseSlotInfo] slave repl info is nil")
				return info, ErrInvalidSlaveReplInfo
			}
			slaveReplInfo.Status = ParseSlaveBinlogStatus(value)
		case "(db0":
			// "111) binlog_offset=0 0,safety_purge=none"
			slotInfoParts := trimmedSplit(value, ")")
			if len(slotInfoParts) < 2 { // nolint:gomnd
				log.Errorf("[parseSlotInfo] expect 2 parts of slotInfo, but met %v", slotInfoParts)
				return info, ErrInvalidSlotInfo
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
				return info, ErrInvalidSlotOffset
			}

			fileNumOffsetS := binlogParts[1] // "0 0"
			fileNumOffsetParts := trimmedSplit(fileNumOffsetS, " ")
			if len(fileNumOffsetParts) != 2 { // nolint:gomnd
				log.Errorf("[parseSlotInfo] expecting 2 parts of fileNumOffset string, but met %v", fileNumOffsetParts)
				return info, ErrInvalidSlotOffset
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
				return info, ErrNotSlaveButHaveMasterAddr
			}
			info.MasterAddr = value
		case "connected_slaves":
			var connectedSlaves int64
			if connectedSlaves, err = strconv.ParseInt(value, 10, 64); err != nil {
				log.Errorf("[parseSlotInfo] parse connected_slaves failed, string: '%s', err: '%v'", value, err)
				return info, err
			}
			info.ConnectedSlaves = int(connectedSlaves)
		}
	}

	if info.Slot == -1 {
		log.Errorf("[parseSlotInfo] can't parse slot")
		return info, ErrInvalidSlotInfo
	}
	if info.FileNum == math.MaxUint64 || info.Offset == math.MaxUint64 {
		log.Errorf("[parseSlotInfo] can't parse slot binlog offset")
		return info, ErrInvalidSlotOffset
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
