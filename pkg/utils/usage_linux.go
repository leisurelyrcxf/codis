// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

// +build linux

package utils

import (
	"bufio"
	"fmt"
	"github.com/CodisLabs/codis/pkg/utils/assert"
	"github.com/CodisLabs/codis/pkg/utils/log"
	"os"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/CodisLabs/codis/pkg/utils/errors"
)

/*
#include <unistd.h>
*/
import "C"

var physicalMemorySize int64

type Usage struct {
	Utime  time.Duration `json:"utime"`
	Stime  time.Duration `json:"stime"`
	Cutime time.Duration `json:"cutime"`
	Cstime time.Duration `json:"cstime"`

	NumThreads int `json:"num_threads"`

	VmSize int64 `json:"vm_size"`
	VmRss  int64 `json:"vm_rss"`
}

func (u *Usage) MemTotal() int64 {
	if u == nil {
		return 0
	}
	return u.VmRss
}

func (u *Usage) MemPercentage() float64 {
	if u == nil {
		return 0.0
	}
	return float64(u.VmRss) / float64(physicalMemorySize)
}

func (u *Usage) CPUTotal() time.Duration {
	return time.Duration(u.Utime + u.Stime + u.Cutime + u.Cstime)
}

func GetUsage() (*Usage, error) {
	f, err := os.Open("/proc/self/stat")
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer f.Close()

	var ignore struct {
		s string
		d int64
	}

	r := bufio.NewReader(f)
	u := &Usage{}
	if _, err := fmt.Fscanf(r, "%d %s %s %d %d %d",
		&ignore.d, &ignore.s, &ignore.s, &ignore.d, &ignore.d, &ignore.d); err != nil {
		return nil, errors.Trace(err)
	}
	if _, err := fmt.Fscanf(r, "%d %d %d",
		&ignore.d, &ignore.d, &ignore.d); err != nil {
		return nil, errors.Trace(err)
	}
	if _, err := fmt.Fscanf(r, "%d %d %d %d",
		&ignore.d, &ignore.d, &ignore.d, &ignore.d); err != nil {
		return nil, errors.Trace(err)
	}

	var ticks struct {
		u int64
		s int64
	}
	unit := time.Second / time.Duration(C.sysconf(C._SC_CLK_TCK))

	if _, err := fmt.Fscanf(r, "%d %d",
		&ticks.u, &ticks.s); err != nil {
		return nil, errors.Trace(err)
	}
	u.Utime = time.Duration(ticks.u) * unit
	u.Stime = time.Duration(ticks.s) * unit

	if _, err := fmt.Fscanf(r, "%d %d",
		&ticks.u, &ticks.s); err != nil {
		return nil, errors.Trace(err)
	}
	u.Cutime = time.Duration(ticks.u) * unit
	u.Cstime = time.Duration(ticks.s) * unit

	if _, err := fmt.Fscanf(r, "%d %d %d %d %d",
		&ignore.d, &ignore.d, &u.NumThreads, &ignore.d, &ignore.d); err != nil {
		return nil, errors.Trace(err)
	}
	if _, err := fmt.Fscanf(r, "%d %d",
		&u.VmSize, &u.VmRss); err != nil {
		return nil, errors.Trace(err)
	}
	u.VmRss *= int64(syscall.Getpagesize())
	return u, nil
}

func init() {
	f, err := os.Open("/proc/meminfo")
	if err != nil {
		log.PanicErrorf(err, "can't open /proc/meminfo file")
	}
	defer f.Close()

	reader := bufio.NewReader(f)

	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			log.PanicErrorf(err, "can't find physical memory info")
		}
		splits := strings.Split(line, ":")
		if splits[0] == "MemTotal" {
			memStrVal := splits[1]
			kbIdx := strings.Index(memStrVal, "kB")
			if kbIdx == -1 {
				log.Panicf("can't find physical memory info, expect kB in value")
			}
			memStrVal = memStrVal[:kbIdx]
			memStrVal = strings.TrimSpace(memStrVal)
			memSize, err := strconv.ParseInt(memStrVal, 10, 64)
			if err != nil {
				log.PanicErrorf(err, "malformed memory size")
			}
			physicalMemorySize = memSize * 1024
			log.Infof("physical memory size: %d bytes", physicalMemorySize)
			break
		}
	}

	assert.Must(physicalMemorySize > 0)
}
