// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package topom

import (
	"container/list"
	"fmt"
	"github.com/CodisLabs/codis/pkg/proxy"
	"github.com/CodisLabs/codis/pkg/utils/assert"
	"github.com/prometheus/client_golang/prometheus"
	"math"
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

	"github.com/CodisLabs/codis/pkg/models"
	"github.com/CodisLabs/codis/pkg/utils"
	"github.com/CodisLabs/codis/pkg/utils/errors"
	"github.com/CodisLabs/codis/pkg/utils/log"
	"github.com/CodisLabs/codis/pkg/utils/math2"
	"github.com/CodisLabs/codis/pkg/utils/redis"
	"github.com/CodisLabs/codis/pkg/utils/rpc"
	"github.com/CodisLabs/codis/pkg/utils/sync2/atomic2"
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

		interval atomic2.Int64
		disabled atomic2.Bool

		progress struct {
			status atomic.Value
		}
		executor atomic2.Int64
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
		s.ladmin.Addr().String(),
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
		if err := s.store.Acquire(s.model); err != nil {
			log.ErrorErrorf(err, "store: acquire lock of %s failed", s.config.ProductName)
			return errors.Errorf("store: acquire lock of %s failed", s.config.ProductName)
		}
		s.online = true
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
				if err := s.ProcessSlotAction(); err != nil {
					log.WarnErrorf(err, "process slot action failed")
					time.Sleep(time.Second * 5)
				}
			}
			time.Sleep(time.Second)
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
	stats.SlotAction.Disabled = s.action.disabled.Bool()
	stats.SlotAction.Progress.Status = s.action.progress.status.Load().(string)
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
		Interval int64 `json:"interval"`
		Disabled bool  `json:"disabled"`

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
	us = math2.MinMaxInt(us, 0, 1000*1000)
	s.action.interval.Set(int64(us))
	log.Warnf("set action interval = %d", us)
}

func (s *Topom) GetSlotActionDisabled() bool {
	return s.action.disabled.Bool()
}

func (s *Topom) SetSlotActionDisabled(value bool) {
	s.action.disabled.Set(value)
	log.Warnf("set action disabled = %t", value)
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
	type proxyFieldGetter func(*Stats, string) interface{}

	const LabelAddr = "addr"

	var (
		period time.Duration

		model        = p.Model()
		replacer     = strings.NewReplacer(".", "_", ":", "_", "-", "_")
		segs         = []string{
			replacer.Replace(model.ProductName),
		}
		namespacePrefix = strings.Join(segs, "_")
	)

	var (
		namespaceProxy = namespacePrefix + "_proxy"

		proxyGaugeHelpers = map[string]string{
			"ops_total": "total operations",
			"ops_fails": "total failed operations",
			"ops_redis_errors": "redis errors number",
			"ops_qps": "operations QPS",
			"sessions_total": "total session number",
			"sessions_alive": "alive session number",
			"rusage_mem": "rusage memory",
			"rusage_cpu": "rusage CPU",
			"runtime_gc_num": "runtime GC number",
			"runtime_gc_total_pausems": "runtime GC total pausems",
			"runtime_num_procs": "runtime proc number",
			"runtime_num_goroutines": "runtime goroutine number",
			"runtime_num_cgo_call": "runtime cgo call number",
			"runtime_num_mem_offheap": "runtime memory off-heap number",
		}

		proxyGaugeVecs     = make(map[string]*prometheus.GaugeVec)
		proxyAddrs           []string
		proxyAddrTokenMap  = make(map[string]string)
		emptyProxyStats    = &proxy.Stats{}
		emptyRuntimeStats  = &proxy.RuntimeStats{}

		proxyAddrsGetter = func(s *Stats) {
			for _, pm := range s.Proxy.Models {
				_, exist := proxyAddrTokenMap[pm.ProxyAddr]
				if !exist {
					proxyAddrs = append(proxyAddrs, pm.ProxyAddr)
					proxyAddrTokenMap[pm.ProxyAddr] = pm.Token
				}
			}
		}

		proxyStatsGetter = func(stats *Stats, addr string) *proxy.Stats {
			if stats == nil {
				return emptyProxyStats
			}
			proxyStats := stats.Proxy.Stats[proxyAddrTokenMap[addr]]
			if proxyStats == nil {
				return emptyProxyStats
			}
			if proxyStats.Stats == nil {
				return emptyProxyStats
			}
			return proxyStats.Stats
		}

		proxyRuntimeStatsGetter = func(stats *Stats, addr string) *proxy.RuntimeStats {
			if stats == nil {
				return emptyRuntimeStats
			}
			proxyStats := stats.Proxy.Stats[proxyAddrTokenMap[addr]]
			if proxyStats == nil {
				return emptyRuntimeStats
			}
			if proxyStats.Stats == nil {
				return emptyRuntimeStats
			}
			runtimeStats := proxyStats.Stats.Runtime
			if runtimeStats == nil {
				return emptyRuntimeStats
			}
			return runtimeStats
		}

		proxyFieldGetters = map[string]proxyFieldGetter{
			"ops_total":                func(stats *Stats, addr string) interface{} { return proxyStatsGetter(stats, addr).Ops.Total },
			"ops_fails":                func(stats *Stats, addr string) interface{} { return proxyStatsGetter(stats, addr).Ops.Fails },
			"ops_redis_errors":         func(stats *Stats, addr string) interface{} { return proxyStatsGetter(stats, addr).Ops.Redis.Errors },
			"ops_qps":                  func(stats *Stats, addr string) interface{} { return proxyStatsGetter(stats, addr).Ops.QPS },
			"sessions_total":           func(stats *Stats, addr string) interface{} { return proxyStatsGetter(stats, addr).Sessions.Total },
			"sessions_alive":           func(stats *Stats, addr string) interface{} { return proxyStatsGetter(stats, addr).Sessions.Alive },
			"rusage_mem":               func(stats *Stats, addr string) interface{} { return proxyStatsGetter(stats, addr).Rusage.Mem },
			"rusage_cpu":               func(stats *Stats, addr string) interface{} { return proxyStatsGetter(stats, addr).Rusage.CPU },
			"runtime_gc_num":           func(stats *Stats, addr string) interface{} { return proxyRuntimeStatsGetter(stats, addr).GC.Num },
			"runtime_gc_total_pausems": func(stats *Stats, addr string) interface{} { return proxyRuntimeStatsGetter(stats, addr).GC.TotalPauseMs },
			"runtime_num_procs":        func(stats *Stats, addr string) interface{} { return proxyRuntimeStatsGetter(stats, addr).NumProcs },
			"runtime_num_goroutines":   func(stats *Stats, addr string) interface{} { return proxyRuntimeStatsGetter(stats, addr).NumGoroutines },
			"runtime_num_cgo_call":     func(stats *Stats, addr string) interface{} { return proxyRuntimeStatsGetter(stats, addr).NumCgoCall },
			"runtime_num_mem_offheap":  func(stats *Stats, addr string) interface{} { return proxyRuntimeStatsGetter(stats, addr).MemOffheap },
		}
	)

	var (
		namespaceRedis = namespacePrefix + "_redis"

		redisMetrics = map[string]string {
			"aof_current_rewrite_time_sec": "-1",
			"aof_enabled": "0",
			//"aof_last_bgrewrite_status": "ok",
			"aof_last_rewrite_time_sec": "-1",
			//"aof_last_write_status": "ok",
			"aof_rewrite_in_progress": "0",
			"aof_rewrite_scheduled": "0",
			"arch_bits": "64",
			"blocked_clients": "0",
			"client_biggest_input_buf": "0",
			"client_longest_output_list": "0",
			"cluster_enabled": "0",
			"connected_clients": "1",
			"connected_slaves": "0",
			"evicted_keys": "0",
			"expired_keys": "0",
			"hz": "10",
			"instantaneous_input_kbps": "0.03",
			"instantaneous_ops_per_sec": "1",
			"instantaneous_output_kbps": "1.38",
			"keyspace_hits": "0",
			"keyspace_misses": "0",
			"latest_fork_usec": "0",
			"loading": "0",
			"lru_clock": "16750322",
			"master_repl_offset": "0",
			"maxmemory": "0",
			"maxmemory_human": "0B",
			"mem_fragmentation_ratio": "2.44",
			"migrate_cached_sockets": "0",
			"process_id": "31493",
			"pubsub_channels": "0",
			"pubsub_patterns": "0",
			"rdb_bgsave_in_progress": "0",
			"rdb_changes_since_last_save": "0",
			"rdb_current_bgsave_time_sec": "-1",
			//"rdb_last_bgsave_status": "ok",
			"rdb_last_bgsave_time_sec": "-1",
			"rdb_last_save_time": "1577029758",
			"redis_git_dirty": "0",
			"rejected_connections": "0",
			"repl_backlog_active": "0",
			"repl_backlog_first_byte_offset": "0",
			"repl_backlog_histlen": "0",
			"repl_backlog_size": "1048576",
			//"role": "master",
			"sync_full": "0",
			"sync_partial_err": "0",
			"sync_partial_ok": "0",
			"tcp_port": "56379",
			"total_commands_processed": "2096",
			"total_connections_received": "4",
			"total_net_input_bytes": "56590",
			"total_net_output_bytes": "2362378",
			"total_system_memory": "16668811264",
			"total_system_memory_human": "15.52G",
			"uptime_in_days": "0",
			"uptime_in_seconds": "1652",
			"used_cpu_sys": "1.31",
			"used_cpu_sys_children": "0.00",
			"used_cpu_user": "0.81",
			"used_cpu_user_children": "0.00",
			"used_memory": "2293544",
			"used_memory_human": "2.19M",
			"used_memory_lua": "37888",
			"used_memory_lua_human": "37.00K",
			"used_memory_peak": "2294568",
			"used_memory_peak_human": "2.19M",
			"used_memory_rss": "5595136",
			"used_memory_rss_human": "5.34M",
		}

		redisGaugeVecs  = make(map[string]*prometheus.GaugeVec)
		redisAddrs      []string
		redisAddrMap    = make(map[string]struct{})

		redisAddrsGetter = func(s *Stats) {
			for redisAddr, _ := range s.Group.Stats {
				_, exist := redisAddrMap[redisAddr]
				if !exist {
					redisAddrs = append(redisAddrs, redisAddr)
					redisAddrMap[redisAddr] = struct{}{}
				}
			}
		}

		redisMetricAsFloat64 = func(strVal string) (float64, error) {
			if len(strVal) == 0 {
				return math.NaN(), fmt.Errorf("empty string")
			}
			eta := 1.0
			strVal = strings.ToLower(strVal)
			switch strVal[len(strVal) - 1] {
			case 'b':
				strVal = strVal[:len(strVal) - 1]
			case 'k':
				eta = 1000.0
				strVal = strVal[:len(strVal) - 1]
			case 'm':
				eta = 1000 * 1000
				strVal = strVal[:len(strVal) - 1]
			case 'g':
				eta = 1000 * 1000 * 1000
				strVal = strVal[:len(strVal) - 1]
			default:
				break
			}
			if len(strVal) == 0 {
				return math.NaN(), fmt.Errorf("empty string")
			}
			base, err := strconv.ParseFloat(strVal, 64)
			if err != nil {
				return math.NaN(), err
			}
			return base * eta, nil
		}

		redisFieldGetter = func(stats *Stats, addr string, metric string) float64 {
			if stats == nil || stats.Group.Stats == nil {
				return math.NaN()
			}
			redisStats := stats.Group.Stats[addr]
			if redisStats == nil {
				return math.NaN()
			}
			s := redisStats.Stats
			if s == nil {
				return math.NaN()
			}
			strVal, ok := s[metric]
			if !ok {
				log.Errorf("metric '%s' doesn't exist", metric)
				return math.NaN()
			}
			fVal, err := redisMetricAsFloat64(strVal)
			if err != nil {
				log.ErrorErrorf(err, "redisMetricAsFloat64() failed, string value: '%s', metric: '%s'", strVal, metric)
			}
			return fVal
		}
	)

	{
		var proxyGaugeList []prometheus.Collector
		for gaugeName, gaugeHelper := range proxyGaugeHelpers {
			gaugeVec := prometheus.NewGaugeVec(
				prometheus.GaugeOpts{
					Namespace: namespaceProxy,
					Name:      gaugeName,
					Help:      gaugeHelper,
				}, []string{
					LabelAddr,
				},
			)
			proxyGaugeVecs[gaugeName] = gaugeVec
			proxyGaugeList = append(proxyGaugeList, gaugeVec)
		}
		prometheus.MustRegister(proxyGaugeList...)
	}

	{
		var redisGaugeList []prometheus.Collector
		for gaugeName, _ := range redisMetrics {
			gaugeVec := prometheus.NewGaugeVec(
				prometheus.GaugeOpts{
					Namespace: namespaceRedis,
					Name:      gaugeName,
					Help:      "",
				}, []string{
					LabelAddr,
				},
			)
			redisGaugeVecs[gaugeName] = gaugeVec
			redisGaugeList = append(redisGaugeList, gaugeVec)
		}
		prometheus.MustRegister(redisGaugeList...)
	}

	period = p.config.PrometheusReportPeriod.Duration()
	period = math2.MaxDuration(time.Second, period)
	p.startMetricsReporter(period, func() error {
		stats, err := p.Stats()
		if err != nil {
			return err
		}

		{
			proxyAddrsGetter(stats)

			var floatType = reflect.TypeOf(float64(0))
			for gaugeName, gaugeVec := range proxyGaugeVecs {
				getter, ok := proxyFieldGetters[gaugeName]
				assert.Must(ok)
				for _, proxyAddr := range proxyAddrs {
					v := reflect.Indirect(reflect.ValueOf(getter(stats, proxyAddr)))
					if !v.Type().ConvertibleTo(floatType) {
						// Impossible here.
						panic(fmt.Sprintf("type %T can't be converted to float", v.Type()))
					}
					fv := v.Convert(floatType)

					assert.Must(gaugeVec != nil)
					gaugeVec.With(prometheus.Labels{LabelAddr: proxyAddr}).Set(fv.Float())
				}
			}
		}

		{
			redisAddrsGetter(stats)
			for gaugeName, gaugeVec := range redisGaugeVecs {
				for _, redisAddr := range redisAddrs {
					fVal := redisFieldGetter(stats, redisAddr, gaugeName)
					gaugeVec.With(prometheus.Labels{LabelAddr: redisAddr}).Set(fVal)
				}
			}
		}
		return nil
	}, func() {
		for _, proxyGaugeVec := range proxyGaugeVecs {
			proxyGaugeVec.Reset()
		}
		for _, redisGaugeVec := range redisGaugeVecs {
			redisGaugeVec.Reset()
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
