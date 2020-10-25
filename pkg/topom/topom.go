// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package topom

import (
	"container/list"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/CodisLabs/codis/pkg/utils/pika"

	"github.com/CodisLabs/codis/pkg/proxy"
	"github.com/CodisLabs/codis/pkg/utils/assert"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/CodisLabs/codis/pkg/models"
	"github.com/CodisLabs/codis/pkg/utils"
	"github.com/CodisLabs/codis/pkg/utils/errors"
	"github.com/CodisLabs/codis/pkg/utils/log"
	"github.com/CodisLabs/codis/pkg/utils/math2"
	"github.com/CodisLabs/codis/pkg/utils/redis"
	"github.com/CodisLabs/codis/pkg/utils/rpc"
	"github.com/CodisLabs/codis/pkg/utils/sync2/atomic2"
)

const (
	errMsgRollback      = "rollback-ed to 'preparing'"
	errMsgReplLinkNotOK = "repl link not ok"

	watchReplLinkOKTimeout = 15 * time.Second

	DefaultSlotActionRollbackWaitPeriod = 15 // in seconds
	MaxRollbackTimes                    = 5
	MinSlotActionGap                    = 50000
)

type Topom struct {
	mu sync.Mutex

	xauth string
	model *models.Topom
	store *models.Store
	cache struct {
		hooks list.List
		slots []*models.SlotMapping
		group map[int]*models.Group
		proxy map[string]*models.Proxy

		sentinel *models.Sentinel
	}

	exit struct {
		C chan struct{}
	}

	config *Config
	online bool
	closed bool

	ladmin net.Listener

	action struct {
		redisp *redis.Pool

		interval           atomic2.Int64
		gap                atomic2.Int64
		rollbackWaitPeriod atomic2.Int64 // in seconds
		disabled           atomic2.Bool

		progress struct {
			status atomic.Value
		}
		slotsProgress []models.SlotMigrationProgress
		executor      atomic2.Int64
	}

	stats struct {
		redisp *redis.Pool

		servers map[string]*RedisStats
		proxies map[string]*ProxyStats
	}

	ha struct {
		redisp *redis.Pool

		monitor *redis.Sentinel
		masters map[int]string
	}
}

var ErrClosedTopom = errors.New("use of closed topom")

func New(client models.Client, config *Config) (*Topom, error) {
	if err := config.Validate(); err != nil {
		return nil, errors.Trace(err)
	}
	if err := models.ValidateProduct(config.ProductName); err != nil {
		return nil, errors.Trace(err)
	}
	s := &Topom{}
	s.config = config
	s.exit.C = make(chan struct{})
	s.action.redisp = redis.NewPool(config.ProductAuth, config.MigrationTimeout.Duration())
	s.action.slotsProgress = make([]models.SlotMigrationProgress, s.config.MaxSlotNum)
	s.action.progress.status.Store("")

	s.ha.redisp = redis.NewPool("", time.Second*5)

	s.model = &models.Topom{
		StartTime: time.Now().String(),
	}
	s.model.ProductName = config.ProductName
	s.model.Pid = os.Getpid()
	s.model.Pwd, _ = os.Getwd()
	if b, err := exec.Command("uname", "-a").Output(); err != nil {
		log.WarnErrorf(err, "run command uname failed")
	} else {
		s.model.Sys = strings.TrimSpace(string(b))
	}
	s.store = models.NewStore(client, config.ProductName, config.MaxSlotNum)

	s.stats.redisp = redis.NewPool(config.ProductAuth, time.Second*5)
	s.stats.servers = make(map[string]*RedisStats)
	s.stats.proxies = make(map[string]*ProxyStats)
	s.SetSlotActionInterval(0)
	s.SetSlotActionGap(int(s.config.MigrationGap))
	s.action.rollbackWaitPeriod.Set(DefaultSlotActionRollbackWaitPeriod)

	if err := s.setup(config); err != nil {
		s.Close()
		return nil, err
	}

	log.Warnf("create new topom:\n%s", s.model.Encode())

	go s.serveAdmin()

	return s, nil
}

func (s *Topom) setup(config *Config) error {
	if l, err := net.Listen("tcp", config.AdminAddr); err != nil {
		return errors.Trace(err)
	} else {
		s.ladmin = l

		var localIp string
		var er error
		if s.config.CoordinatorName == "filesystem" {
			localIp = utils.HostIPs[0]
		} else {
			if s.config.CoordinatorAddr == "" {
				return errors.New("CoordinatorAddr must not empty")
			}
			dialAddr := strings.Split(s.config.CoordinatorAddr, ",")
			if localIp, er = utils.GetOutboundIP(dialAddr[0]); er != nil {
				return er
			}
			log.Infof("Dial CoordinatorAddr:%v,localIp:%v", dialAddr[0], localIp)
		}

		localAddr := strings.Split(s.config.AdminAddr, ":")
		if len(localAddr) != 2 {
			return errors.New("AdminAddr not correct")
		}

		s.model.AdminAddr = fmt.Sprintf("%s:%s", localIp, localAddr[1])
	}

	s.model.Token = rpc.NewToken(
		config.ProductName,
		s.model.AdminAddr,
	)
	s.xauth = rpc.NewXAuth(config.ProductName)

	return nil
}

func (s *Topom) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	close(s.exit.C)

	if s.ladmin != nil {
		s.ladmin.Close()
	}
	for _, p := range []*redis.Pool{
		s.action.redisp, s.stats.redisp, s.ha.redisp,
	} {
		if p != nil {
			p.Close()
		}
	}

	defer s.store.Close()

	if s.online {
		if err := s.store.Release(); err != nil {
			log.ErrorErrorf(err, "store: release lock of %s failed", s.config.ProductName)
			return errors.Errorf("store: release lock of %s failed", s.config.ProductName)
		}
	}
	return nil
}

func (s *Topom) Start(routines bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosedTopom
	}
	if s.online {
		return nil
	} else {
		var w <-chan struct{}
		var err error
		if w, err = s.store.AcquireEphemeral(s.model); err != nil {
			log.ErrorErrorf(err, "store: acquire lock of %s failed", s.config.ProductName)
			return errors.Errorf("store: acquire lock of %s failed", s.config.ProductName)
		}
		s.online = true

		go func() {
			<-w
			log.Warnf("[Start] Refresh distributed lock failed, close topom...")
			_ = s.Close()
		}()
	}

	if !routines {
		return nil
	}
	ctx, err := s.newContext()
	if err != nil {
		return err
	}
	s.rewatchSentinels(ctx.sentinel.Servers)

	go func() {
		for !s.IsClosed() {
			if s.IsOnline() {
				w, _ := s.RefreshRedisStats(time.Second)
				if w != nil {
					w.Wait()
				}
			}
			time.Sleep(time.Second)
		}
	}()

	go func() {
		for !s.IsClosed() {
			if s.IsOnline() {
				w, _ := s.RefreshProxyStats(time.Second)
				if w != nil {
					w.Wait()
				}
			}
			time.Sleep(time.Second)
		}
	}()

	go func() {
		for !s.IsClosed() {
			if s.IsOnline() {
				if err := s.ProcessSlotAction(); err != nil && !strings.Contains(err.Error(), pika.ErrMsgLagNotMatch) {
					log.WarnErrorf(err, "process slot action failed: %v", err)
					time.Sleep(time.Second * 5)
				}
			}
			if us := s.GetSlotActionInterval(); us != 0 {
				time.Sleep(time.Microsecond * time.Duration(us))
			}
		}
	}()

	go func() {
		for !s.IsClosed() {
			if s.IsOnline() {
				if err := s.ProcessSyncAction(); err != nil {
					log.WarnErrorf(err, "process sync action failed")
					time.Sleep(time.Second * 5)
				}
			}
			time.Sleep(time.Second)
		}
	}()

	go func() {
		// Avoid dead-lock, otherwise no need to put in go-routine.
		s.collectPrometheusMetrics()
	}()

	return nil
}

func (s *Topom) XAuth() string {
	return s.xauth
}

func (s *Topom) Model() *models.Topom {
	return s.model
}

var ErrNotOnline = errors.New("topom is not online")

func (s *Topom) newContext() (*context, error) {
	if s.closed {
		return nil, ErrClosedTopom
	}
	if s.online {
		if err := s.refillCache(); err != nil {
			return nil, err
		} else {
			ctx := &context{}
			ctx.slots = s.cache.slots
			ctx.group = s.cache.group
			ctx.proxy = s.cache.proxy
			ctx.sentinel = s.cache.sentinel
			ctx.hosts.m = make(map[string]net.IP)
			ctx.method, _ = models.ParseForwardMethod(s.config.MigrationMethod)
			ctx.config = s.config
			return ctx, nil
		}
	} else {
		return nil, ErrNotOnline
	}
}

func (s *Topom) Stats() (*Stats, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return nil, err
	}

	stats := &Stats{}
	stats.Closed = s.closed

	stats.Slots = ctx.slots

	stats.Group.Models = models.SortGroup(ctx.group)
	stats.Group.Stats = map[string]*RedisStats{}
	for _, g := range ctx.group {
		for _, x := range g.Servers {
			if v := s.stats.servers[x.Addr]; v != nil {
				stats.Group.Stats[x.Addr] = v
			}
		}
	}

	stats.Proxy.Models = models.SortProxy(ctx.proxy)
	stats.Proxy.Stats = s.stats.proxies

	stats.SlotAction.Interval = s.action.interval.Int64()
	stats.SlotAction.Gap = s.action.gap.Int64()
	stats.SlotAction.RollbackWaitPeriod = s.action.rollbackWaitPeriod.Int64()
	stats.SlotAction.Disabled = s.action.disabled.Bool()
	stats.SlotAction.Progress.Status = s.action.progress.status.Load().(string)
	for slot, progress := range s.action.slotsProgress {
		progress := progress
		if progress.IsEmpty() {
			stats.Slots[slot].Action.Info.Progress = nil
		} else {
			stats.Slots[slot].Action.Info.Progress = &progress
		}
	}
	stats.SlotAction.Executor = s.action.executor.Int64()

	stats.HA.Model = ctx.sentinel
	stats.HA.Stats = map[string]*RedisStats{}
	for _, server := range ctx.sentinel.Servers {
		if v := s.stats.servers[server]; v != nil {
			stats.HA.Stats[server] = v
		}
	}
	stats.HA.Masters = make(map[string]string)
	if s.ha.masters != nil {
		for gid, addr := range s.ha.masters {
			stats.HA.Masters[strconv.Itoa(gid)] = addr
		}
	}
	stats.Backend.ReadSlavesOnly = s.Config().IsBackendReadSlavesOnly()
	return stats, nil
}

type Stats struct {
	Closed bool `json:"closed"`

	Slots []*models.SlotMapping `json:"slots"`

	Group struct {
		Models []*models.Group        `json:"models"`
		Stats  map[string]*RedisStats `json:"stats"`
	} `json:"group"`

	Proxy struct {
		Models []*models.Proxy        `json:"models"`
		Stats  map[string]*ProxyStats `json:"stats"`
	} `json:"proxy"`

	SlotAction struct {
		Interval           int64 `json:"interval"`
		Gap                int64 `json:"gap"`
		RollbackWaitPeriod int64 `json:"rollback_wait_period"`
		Disabled           bool  `json:"disabled"`

		Progress struct {
			Status string `json:"status"`
		} `json:"progress"`

		Executor int64 `json:"executor"`
	} `json:"slot_action"`

	HA struct {
		Model   *models.Sentinel       `json:"model"`
		Stats   map[string]*RedisStats `json:"stats"`
		Masters map[string]string      `json:"masters"`
	} `json:"sentinels"`

	Backend struct {
		ReadSlavesOnly bool `json:"read_slaves_only"`
	} `json:"backend"`
}

func (s *Topom) Config() *Config {
	return s.config
}

func (s *Topom) IsOnline() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.online && !s.closed
}

func (s *Topom) IsClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

func (s *Topom) GetSlotActionInterval() int {
	return s.action.interval.AsInt()
}

func (s *Topom) SetSlotActionInterval(us int) {
	us = math2.MinMaxInt(us, 100*1000, 1000*1000)
	s.action.interval.Set(int64(us))
	log.Warnf("set slot action interval = %d", us)
}

func (s *Topom) GetSlotActionGap() uint64 {
	return uint64(s.action.gap.Int64())
}

func (s *Topom) SetSlotActionGap(gap int) {
	s.SetSlotActionGapRaw(math2.MaxInt(MinSlotActionGap, gap))
}

func (s *Topom) SetSlotActionGapRaw(gap int) {
	s.action.gap.Set(int64(gap))
	log.Warnf("set slot action gap = %d", gap)
}

func (s *Topom) GetSlotActionRollbackWaitPeriod() time.Duration {
	return time.Duration(s.action.rollbackWaitPeriod.Int64()) * time.Second
}

func (s *Topom) SetSlotActionRollbackWaitPeriod(seconds int) {
	if seconds < 0 {
		seconds = 0
	}
	s.action.rollbackWaitPeriod.Set(int64(seconds))
	log.Warnf("set slot action rollback wait period = %d seconds", seconds)
}

func (s *Topom) GetSlotActionDisabled() bool {
	return s.action.disabled.Bool()
}

func (s *Topom) SetSlotActionDisabled(value bool) {
	s.action.disabled.Set(value)
	log.Warnf("set slot action disabled = %t", value)
}

func (s *Topom) Slots() ([]*models.Slot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return nil, err
	}
	return ctx.toSlotSlice(ctx.slots, nil), nil
}

func (s *Topom) SlaveOfMaster(addr string, slots []int, force bool) error {
	if len(slots) == 0 {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	ctx, err := s.newContext()
	if err != nil {
		return err
	}
	for _, slot := range slots {
		if _, err := ctx.getSlotMapping(slot); err != nil {
			return err
		}
	}

	pid, g, err := ctx.lookupPika(addr)
	if err != nil {
		return err
	}
	if pid == 0 {
		return nil
	}

	masterAddr, slaveAddr := g.Servers[0].Addr, addr
	masterSlotsInfo, err := s.getPikaSlotsInfo(masterAddr)
	if err != nil {
		return errors.Errorf("can't get master %s slots info: '%v'", masterAddr, err)
	}
	for _, slot := range slots {
		if _, ok := masterSlotsInfo[slot]; !ok {
			return errors.Errorf("slot %d doesn't exist on master %s", slot, masterAddr)
		}
	}
	return s.withRedisClient(slaveAddr, func(client *redis.Client) (err error) {
		for _, slot := range slots {
			if masterSlotsInfo[slot].IsLinked(slaveAddr) {
				continue
			}
			if slaveOfErr := client.SlaveOf(masterAddr, slot, force, false); slaveOfErr != nil {
				log.Errorf("[SlaveOfMaster] %s slave of master %s failed: %v", slaveAddr, masterAddr, slaveOfErr)
				err = slaveOfErr
			}
		}
		return
	})
}

func (s *Topom) Reload() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.newContext()
	if err != nil {
		return err
	}
	defer s.dirtyCacheAll()
	return nil
}

func (s *Topom) serveAdmin() {
	if s.IsClosed() {
		return
	}
	defer s.Close()

	log.Warnf("admin start service on %s", s.ladmin.Addr())

	eh := make(chan error, 1)
	go func(l net.Listener) {
		h := http.NewServeMux()
		h.Handle("/", newApiServer(s))
		hs := &http.Server{Handler: h}
		eh <- hs.Serve(l)
	}(s.ladmin)

	select {
	case <-s.exit.C:
		log.Warnf("admin shutdown")
	case err := <-eh:
		log.ErrorErrorf(err, "admin exit on error")
	}
}

type Overview struct {
	Version string        `json:"version"`
	Compile string        `json:"compile"`
	Config  *Config       `json:"config,omitempty"`
	Model   *models.Topom `json:"model,omitempty"`
	Stats   *Stats        `json:"stats,omitempty"`
}

func (s *Topom) Overview() (*Overview, error) {
	if stats, err := s.Stats(); err != nil {
		return nil, err
	} else {
		return &Overview{
			Version: utils.Version,
			Compile: utils.Compile,
			Config:  s.Config(),
			Model:   s.Model(),
			Stats:   stats,
		}, nil
	}
}

func (p *Topom) collectPrometheusMetrics() {
	type proxyFieldGetter func(*ProxyStats) interface{}

	const (
		LabelProductName = "product_name"
		LabelAddr        = "addr"
		LabelPid         = "pid_in_group"
		NanValue         = -1.0
		LabelCmdName     = "cmd_name"
	)

	var (
		proxyNamespace = "codis_proxy"

		proxyHealthMetrics = map[string]string{
			"up": "whether proxy's status is up",
		}

		proxyMetrics = map[string]string{
			"ops_total":                "total operations",
			"ops_fails":                "total failed operations",
			"ops_redis_errors":         "redis errors number",
			"ops_qps":                  "operations QPS",
			"sessions_total":           "total session number",
			"sessions_alive":           "alive session number",
			"sessions_max":             "max session number",
			"rusage_mem":               "rusage memory",
			"rusage_mem_percentage":    "rusage memory percentage",
			"rusage_cpu":               "rusage CPU",
			"runtime_gc_num":           "runtime GC number",
			"runtime_gc_total_pausems": "runtime GC total pausems",
			"runtime_num_procs":        "runtime proc number",
			"runtime_num_goroutines":   "runtime goroutine number",
			"runtime_num_cgo_call":     "runtime cgo call number",
			"runtime_num_mem_offheap":  "runtime memory off-heap number",
			"runtime_heap_alloc":       "runtime heap alloc",
			"runtime_heap_sys":         "runtime heap sys",
			"runtime_heap_idle":        "runtime heap idle",
			"runtime_heap_inuse":       "runtime heap inuse",
			"runtime_heap_objects":     "runtime heap objects",
			"runtime_general_alloc":    "runtime general alloc",
			"runtime_general_sys":      "runtime general sys",
			"runtime_general_lookups":  "runtime general lookups",
			"runtime_general_mallocs":  "runtime general mallocs",
			"runtime_general_frees":    "runtime general frees",
		}

		proxyGauges = make(map[string]*prometheus.GaugeVec)

		emptyProxyStats   = &proxy.Stats{}
		emptyRuntimeStats = &proxy.RuntimeStats{}

		proxyStatsGetter = func(ps *ProxyStats) *proxy.Stats {
			if ps == nil || ps.Stats == nil {
				return emptyProxyStats
			}
			return ps.Stats
		}

		proxyRStatsGetter = func(ps *ProxyStats) *proxy.RuntimeStats {
			if ps == nil || ps.Stats == nil || ps.Stats.Runtime == nil {
				return emptyRuntimeStats
			}
			return ps.Stats.Runtime
		}

		proxyFieldGetters = map[string]proxyFieldGetter{
			"ops_total":                func(ps *ProxyStats) interface{} { return proxyStatsGetter(ps).Ops.Total },
			"ops_fails":                func(ps *ProxyStats) interface{} { return proxyStatsGetter(ps).Ops.Fails },
			"ops_redis_errors":         func(ps *ProxyStats) interface{} { return proxyStatsGetter(ps).Ops.Redis.Errors },
			"ops_qps":                  func(ps *ProxyStats) interface{} { return proxyStatsGetter(ps).Ops.QPS },
			"sessions_total":           func(ps *ProxyStats) interface{} { return proxyStatsGetter(ps).Sessions.Total },
			"sessions_alive":           func(ps *ProxyStats) interface{} { return proxyStatsGetter(ps).Sessions.Alive },
			"sessions_max":             func(ps *ProxyStats) interface{} { return proxyStatsGetter(ps).Sessions.Max },
			"rusage_mem":               func(ps *ProxyStats) interface{} { return proxyStatsGetter(ps).Rusage.Mem },
			"rusage_mem_percentage":    func(ps *ProxyStats) interface{} { return proxyStatsGetter(ps).Rusage.MemPercentage },
			"rusage_cpu":               func(ps *ProxyStats) interface{} { return proxyStatsGetter(ps).Rusage.CPU },
			"runtime_gc_num":           func(ps *ProxyStats) interface{} { return proxyRStatsGetter(ps).GC.Num },
			"runtime_gc_total_pausems": func(ps *ProxyStats) interface{} { return proxyRStatsGetter(ps).GC.TotalPauseMs },
			"runtime_num_procs":        func(ps *ProxyStats) interface{} { return proxyRStatsGetter(ps).NumProcs },
			"runtime_num_goroutines":   func(ps *ProxyStats) interface{} { return proxyRStatsGetter(ps).NumGoroutines },
			"runtime_num_cgo_call":     func(ps *ProxyStats) interface{} { return proxyRStatsGetter(ps).NumCgoCall },
			"runtime_num_mem_offheap":  func(ps *ProxyStats) interface{} { return proxyRStatsGetter(ps).MemOffheap },
			"runtime_heap_alloc":       func(ps *ProxyStats) interface{} { return proxyRStatsGetter(ps).Heap.Alloc },
			"runtime_heap_sys":         func(ps *ProxyStats) interface{} { return proxyRStatsGetter(ps).Heap.Sys },
			"runtime_heap_idle":        func(ps *ProxyStats) interface{} { return proxyRStatsGetter(ps).Heap.Idle },
			"runtime_heap_inuse":       func(ps *ProxyStats) interface{} { return proxyRStatsGetter(ps).Heap.Inuse },
			"runtime_heap_objects":     func(ps *ProxyStats) interface{} { return proxyRStatsGetter(ps).Heap.Objects },
			"runtime_general_alloc":    func(ps *ProxyStats) interface{} { return proxyRStatsGetter(ps).General.Alloc },
			"runtime_general_sys":      func(ps *ProxyStats) interface{} { return proxyRStatsGetter(ps).General.Sys },
			"runtime_general_lookups":  func(ps *ProxyStats) interface{} { return proxyRStatsGetter(ps).General.Lookups },
			"runtime_general_mallocs":  func(ps *ProxyStats) interface{} { return proxyRStatsGetter(ps).General.Mallocs },
			"runtime_general_frees":    func(ps *ProxyStats) interface{} { return proxyRStatsGetter(ps).General.Frees },
		}
	)

	var (
		cmdNamespace = "codis_proxy_cmd"

		cmdMetrics = map[string]string{
			"user_seconds": "cmd user seconds",
			"total":        "cmd total",
			"failure":      "cmd failure",
		}

		cmdGauges = make(map[string]*prometheus.GaugeVec)
	)

	var (
		redisNamespace = p.config.Mode

		redisHealthMetrics = map[string]string{
			"up": "0",
			"ok": "0",
		}

		redisMetrics = map[string]string{
			"aof_current_rewrite_time_sec": "-1",
			"aof_enabled":                  "0",
			//"aof_last_bgrewrite_status": "ok",
			"aof_last_rewrite_time_sec": "-1",
			//"aof_last_write_status": "ok",
			"aof_rewrite_in_progress":     "0",
			"aof_rewrite_scheduled":       "0",
			"arch_bits":                   "64",
			"blocked_clients":             "0",
			"client_biggest_input_buf":    "0",
			"client_longest_output_list":  "0",
			"cluster_enabled":             "0",
			"connected_clients":           "1",
			"connected_slaves":            "0",
			"evicted_keys":                "0",
			"expired_keys":                "0",
			"hz":                          "10",
			"instantaneous_input_kbps":    "0.03",
			"instantaneous_ops_per_sec":   "1",
			"instantaneous_output_kbps":   "1.38",
			"keyspace_hits":               "0",
			"keyspace_misses":             "0",
			"latest_fork_usec":            "0",
			"loading":                     "0",
			"lru_clock":                   "16750322",
			"master_repl_offset":          "0",
			"maxmemory":                   "0",
			"maxclients":                  "0",
			"mem_fragmentation_ratio":     "2.44",
			"migrate_cached_sockets":      "0",
			"process_id":                  "31493",
			"pubsub_channels":             "0",
			"pubsub_patterns":             "0",
			"rdb_bgsave_in_progress":      "0",
			"rdb_changes_since_last_save": "0",
			"rdb_current_bgsave_time_sec": "-1",
			//"rdb_last_bgsave_status": "ok",
			"rdb_last_bgsave_time_sec":       "-1",
			"rdb_last_save_time":             "1577029758",
			"redis_git_dirty":                "0",
			"rejected_connections":           "0",
			"repl_backlog_active":            "0",
			"repl_backlog_first_byte_offset": "0",
			"repl_backlog_histlen":           "0",
			"repl_backlog_size":              "1048576",
			//"role": "master",
			"sync_full":                  "0",
			"sync_partial_err":           "0",
			"sync_partial_ok":            "0",
			"tcp_port":                   "56379",
			"total_commands_processed":   "2096",
			"total_connections_received": "4",
			"total_net_input_bytes":      "56590",
			"total_net_output_bytes":     "2362378",
			"total_system_memory":        "16668811264",
			"uptime_in_days":             "0",
			"uptime_in_seconds":          "1652",
			"used_cpu_sys":               "1.31",
			"used_cpu_sys_children":      "0.00",
			"used_cpu_user":              "0.81",
			"used_cpu_user_children":     "0.00",
			"used_memory":                "2293544",
			"used_memory_lua":            "37888",
			"used_memory_peak":           "2294568",
			"used_memory_rss":            "5595136",
			"db_size":                    "0",
			"log_size":                   "0",
		}

		redisGauges = make(map[string]*prometheus.GaugeVec)
	)

	{
		var proxyGaugeList []prometheus.Collector
		proxyGaugeCollector := func(metrics map[string]string) {
			for gaugeName, gaugeHelper := range metrics {
				gaugeVec := prometheus.NewGaugeVec(
					prometheus.GaugeOpts{
						Namespace: proxyNamespace,
						Name:      gaugeName,
						Help:      gaugeHelper,
					}, []string{
						LabelProductName,
						LabelAddr,
					},
				)
				proxyGauges[gaugeName] = gaugeVec
				proxyGaugeList = append(proxyGaugeList, gaugeVec)
			}
		}
		proxyGaugeCollector(proxyHealthMetrics)
		proxyGaugeCollector(proxyMetrics)
		prometheus.MustRegister(proxyGaugeList...)
	}

	{
		var cmdGaugeList []prometheus.Collector
		cmdGaugeCollector := func(metrics map[string]string) {
			for gaugeName, gaugeHelper := range metrics {
				gaugeVec := prometheus.NewGaugeVec(
					prometheus.GaugeOpts{
						Namespace: cmdNamespace,
						Name:      gaugeName,
						Help:      gaugeHelper,
					}, []string{
						LabelProductName,
						LabelAddr,
						LabelCmdName,
					},
				)
				cmdGauges[gaugeName] = gaugeVec
				cmdGaugeList = append(cmdGaugeList, gaugeVec)
			}
		}

		cmdGaugeCollector(cmdMetrics)
		prometheus.MustRegister(cmdGaugeList...)
	}

	{
		var redisGaugeList []prometheus.Collector
		redisGaugeCollector := func(metrics map[string]string) {
			for gaugeName := range metrics {
				gaugeVec := prometheus.NewGaugeVec(
					prometheus.GaugeOpts{
						Namespace: redisNamespace,
						Name:      gaugeName,
						Help:      "",
					}, []string{
						LabelProductName,
						LabelAddr,
						LabelPid,
					},
				)
				redisGauges[gaugeName] = gaugeVec
				redisGaugeList = append(redisGaugeList, gaugeVec)
			}
		}
		redisGaugeCollector(redisHealthMetrics)
		redisGaugeCollector(redisMetrics)
		prometheus.MustRegister(redisGaugeList...)
	}

	var (
		productName = p.Model().ProductName
		period      = p.config.PrometheusReportPeriod.Duration()
	)

	period = math2.MaxDuration(time.Second, period)

	firstRun := true

	p.startMetricsReporter(period, func() error {
		stats, err := p.Stats()
		if err != nil {
			return err
		}

		{
			// Proxy metrics
			for _, pm := range stats.Proxy.Models {
				addr := pm.ProxyAddr
				proxyGaugeUp := proxyGauges["up"].With(prometheus.Labels{LabelProductName: productName, LabelAddr: addr})

				var ps *ProxyStats
				if stats.Proxy.Stats == nil {
					proxyGaugeUp.Set(0)
				} else {
					ps = stats.Proxy.Stats[pm.Token]
					switch {
					case ps == nil:
						proxyGaugeUp.Set(0)
					case ps.Error != nil:
						proxyGaugeUp.Set(0)
					case ps.Timeout || ps.Stats == nil:
						proxyGaugeUp.Set(0)
					default:
						if ps.Stats.Online && !ps.Stats.Closed {
							proxyGaugeUp.Set(1)
						} else {
							proxyGaugeUp.Set(0)
						}
					}
				}

				var floatType = reflect.TypeOf(float64(0))
				for metric := range proxyMetrics {
					getter := proxyFieldGetters[metric]
					assert.Must(getter != nil)

					v := reflect.Indirect(reflect.ValueOf(getter(ps)))
					if !v.Type().ConvertibleTo(floatType) {
						panic(fmt.Sprintf("type %T can't be converted to float", v.Type()))
					}
					fv := v.Convert(floatType).Float()

					proxyGauges[metric].With(prometheus.Labels{LabelProductName: productName, LabelAddr: addr}).Set(fv)
				}

				if ps != nil && ps.Stats != nil {
					for _, cmd := range ps.Stats.Ops.Cmd {
						cmdGauges["user_seconds"].With(prometheus.Labels{LabelProductName: productName, LabelAddr: addr, LabelCmdName: cmd.OpStr}).Set(float64(cmd.Usecs))
						cmdGauges["total"].With(prometheus.Labels{LabelProductName: productName, LabelAddr: addr, LabelCmdName: cmd.OpStr}).Set(float64(cmd.Calls))
						cmdGauges["failure"].With(prometheus.Labels{LabelProductName: productName, LabelAddr: addr, LabelCmdName: cmd.OpStr}).Set(float64(cmd.Fails))
					}
				}

			}
		}

		{
			// Redis metrics
			for _, g := range stats.Group.Models {
				for i, x := range g.Servers {
					pid := fmt.Sprintf("%d", i)
					var addr = x.Addr

					rs := stats.Group.Stats[addr]
					redisGaugeUp := redisGauges["up"].With(prometheus.Labels{LabelProductName: productName, LabelAddr: addr, LabelPid: pid})
					redisGaugeOK := redisGauges["ok"].With(prometheus.Labels{LabelProductName: productName, LabelAddr: addr, LabelPid: pid})

					switch {
					case rs == nil:
						redisGaugeUp.Set(0)
						redisGaugeOK.Set(0)
					case rs.Error != nil:
						redisGaugeUp.Set(0)
						redisGaugeOK.Set(0)
					case rs.Timeout || rs.Stats == nil:
						redisGaugeUp.Set(0)
						redisGaugeOK.Set(0)
					default:
						redisGaugeUp.Set(1)
						if i == 0 {
							if rs.Stats["master_addr"] != "" {
								redisGaugeOK.Set(0)
							} else {
								redisGaugeOK.Set(1)
							}
						} else {
							if rs.Stats["master_addr"] != g.Servers[0].Addr {
								redisGaugeOK.Set(0)
							} else {
								switch rs.Stats["master_link_status"] {
								default:
									redisGaugeOK.Set(0)
								case "up":
									redisGaugeOK.Set(1)
								case "down":
									redisGaugeOK.Set(0)
								}
							}
						}
					}

					for metric := range redisMetrics {
						var val float64

						if rs == nil || rs.Stats == nil {
							val = NanValue
						} else {
							if strVal, ok := rs.Stats[metric]; !ok {
								if firstRun {
									log.Warnf("metric '%s' doesn't exist", metric)
								}
								val = NanValue
							} else {
								val, err = strconv.ParseFloat(strVal, 64)
								if err != nil {
									log.ErrorErrorf(err, "redis Metric as float64 failed, string value: '%s', metric: '%s'", strVal, metric)
									val = NanValue
								}
							}
						}

						redisGauges[metric].With(prometheus.Labels{LabelProductName: productName, LabelAddr: addr,
							LabelPid: pid}).Set(val)
					}
				}
			}
		}

		firstRun = false
		return nil
	}, func() {
		for _, proxyGauge := range proxyGauges {
			proxyGauge.Reset()
		}
		for _, cmdGauge := range cmdGauges {
			cmdGauge.Reset()
		}
		for _, redisGauge := range redisGauges {
			redisGauge.Reset()
		}
	})
}

func (p *Topom) startMetricsReporter(d time.Duration, do func() error, cleanup func()) {
	go func() {
		if cleanup != nil {
			defer cleanup()
		}
		var ticker = time.NewTicker(d)
		defer ticker.Stop()
		var delay = &DelayExp2{
			Min: 1, Max: 15,
			Unit: time.Second,
		}
		for !p.IsClosed() {
			<-ticker.C
			if err := do(); err != nil {
				log.WarnErrorf(err, "report metrics failed")
				delay.SleepWithCancel(p.IsClosed)
			} else {
				delay.Reset()
			}
		}
	}()
}
