// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package redis

import (
	"container/list"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/CodisLabs/codis/pkg/utils"

	"github.com/CodisLabs/codis/pkg/utils/log"

	"github.com/CodisLabs/codis/pkg/utils/pika"

	"github.com/CodisLabs/codis/pkg/utils/errors"
	"github.com/CodisLabs/codis/pkg/utils/math2"

	redigo "github.com/garyburd/redigo/redis"
)

const (
	// PikaRemoteNodeError const
	PikaRemoteNodeError = "connect remote node error"
	// PikaConcurrentSlotOpError const
	PikaConcurrentSlotOpError = "Slot in syncing or a change operation is under way, retry later"
)

var (
	ErrCommandNotSupported = fmt.Errorf("command not supported")
)

func IsRetryablePikaSlotError(err error) bool {
	errMsg := err.Error()
	return strings.Contains(errMsg, PikaRemoteNodeError) || strings.Contains(errMsg, PikaConcurrentSlotOpError)
}

func DoPikaSlotOp(f func() (interface{}, error)) (v interface{}, _ error) {
	return v, utils.WithRetryEx(time.Second, time.Second*3, func() (err error) {
		v, err = f()
		return
	}, IsRetryablePikaSlotError)
}

type Client struct {
	conn redigo.Conn
	Addr string
	Auth string

	Database int

	LastUse time.Time
	Timeout time.Duration

	Pipeline struct {
		Send, Recv uint64
	}

	closed bool
}

func NewClientNoAuth(addr string, timeout time.Duration) (*Client, error) {
	return NewClient(addr, "", timeout)
}

func NewClient(addr string, auth string, timeout time.Duration) (*Client, error) {
	c, err := redigo.Dial("tcp", addr, []redigo.DialOption{
		redigo.DialConnectTimeout(math2.MinDuration(time.Second, timeout)),
		redigo.DialPassword(auth),
		redigo.DialReadTimeout(timeout), redigo.DialWriteTimeout(timeout),
	}...)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &Client{
		conn: c, Addr: addr, Auth: auth,
		LastUse: time.Now(), Timeout: timeout,
	}, nil
}

func (c *Client) Close() error {
	c.closed = true
	return c.conn.Close()
}

func (c *Client) isRecyclable() bool {
	switch {
	case c.conn.Err() != nil:
		return false
	case c.Pipeline.Send != c.Pipeline.Recv:
		return false
	case c.Timeout != 0 && c.Timeout <= time.Since(c.LastUse):
		return false
	}
	return true
}

func (c *Client) Do(cmd string, args ...interface{}) (interface{}, error) {
	r, err := c.conn.Do(cmd, args...)
	if err != nil {
		c.Close()
		return nil, errors.Trace(err)
	}
	c.LastUse = time.Now()

	if err, ok := r.(redigo.Error); ok {
		return nil, errors.Trace(err)
	}
	return r, nil
}

func (c *Client) ReconnectIfNeeded() error {
	if !c.closed {
		return nil
	}
	conn, err := redigo.Dial("tcp", c.Addr, []redigo.DialOption{
		redigo.DialConnectTimeout(math2.MinDuration(time.Second, c.Timeout)),
		redigo.DialPassword(c.Auth),
		redigo.DialReadTimeout(c.Timeout), redigo.DialWriteTimeout(c.Timeout),
	}...)
	if err != nil {
		return errors.Trace(err)
	}
	c.conn = conn
	c.LastUse = time.Now()
	return nil
}

func (c *Client) Good() error {
	if _, err := c.Do("PING"); err != nil {
		c.Close()
		return errors.Trace(err)
	}
	return nil
}

func (c *Client) Send(cmd string, args ...interface{}) error {
	if err := c.conn.Send(cmd, args...); err != nil {
		c.Close()
		return errors.Trace(err)
	}
	c.Pipeline.Send++
	return nil
}

func (c *Client) Flush() error {
	if err := c.conn.Flush(); err != nil {
		c.Close()
		return errors.Trace(err)
	}
	return nil
}

func (c *Client) Receive() (interface{}, error) {
	r, err := c.conn.Receive()
	if err != nil {
		c.Close()
		return nil, errors.Trace(err)
	}
	c.Pipeline.Recv++

	c.LastUse = time.Now()

	if err, ok := r.(redigo.Error); ok {
		return nil, errors.Trace(err)
	}
	return r, nil
}

func (c *Client) Select(database int) error {
	if c.Database == database {
		return nil
	}
	_, err := c.Do("SELECT", database)
	if err != nil {
		c.Close()
		return errors.Trace(err)
	}
	c.Database = database
	return nil
}

func (c *Client) Shutdown() error {
	_, err := c.Do("SHUTDOWN")
	if err != nil {
		c.Close()
		return errors.Trace(err)
	}
	return nil
}

func (c *Client) Info() (map[string]string, error) {
	text, err := redigo.String(c.Do("INFO"))
	if err != nil {
		return nil, errors.Trace(err)
	}
	info := make(map[string]string)
	for _, line := range strings.Split(text, "\n") {
		kv := strings.SplitN(line, ":", 2)
		if len(kv) != 2 {
			continue
		}
		if key := strings.TrimSpace(kv[0]); key != "" {
			info[key] = strings.TrimSpace(kv[1])
		}
	}
	return info, nil
}

func (c *Client) InfoKeySpace() (map[int]string, error) {
	text, err := redigo.String(c.Do("INFO", "keyspace"))
	if err != nil {
		return nil, errors.Trace(err)
	}
	info := make(map[int]string)
	for _, line := range strings.Split(text, "\n") {
		kv := strings.SplitN(line, ":", 2)
		if len(kv) != 2 {
			continue
		}
		if key := strings.TrimSpace(kv[0]); key != "" && strings.HasPrefix(key, "db") {
			n, err := strconv.Atoi(key[2:])
			if err != nil {
				return nil, errors.Trace(err)
			}
			info[n] = strings.TrimSpace(kv[1])
		}
	}
	return info, nil
}

func (c *Client) InfoFull() (map[string]string, error) {
	if info, err := c.Info(); err != nil {
		return nil, errors.Trace(err)
	} else {
		host := info["master_host"]
		port := info["master_port"]
		if host != "" || port != "" {
			info["master_addr"] = net.JoinHostPort(host, port)
		}
		r, err := c.Do("CONFIG", "GET", "maxmemory")
		if err != nil {
			return nil, errors.Trace(err)
		}
		p, err := redigo.Values(r, nil)
		if err != nil || len(p) != 2 {
			return nil, errors.Errorf("invalid response = %v", r)
		}
		v, err := redigo.Int(p[1], nil)
		if err != nil {
			return nil, errors.Errorf("invalid response = %v", r)
		}
		info["maxmemory"] = strconv.Itoa(v)

		r, err = c.Do("CONFIG", "GET", "maxclients")
		if err != nil {
			return nil, errors.Trace(err)
		}
		p, err = redigo.Values(r, nil)
		if err != nil || len(p) != 2 {
			return nil, errors.Errorf("invalid response = %v", r)
		}
		v, err = redigo.Int(p[1], nil)
		if err != nil {
			return nil, errors.Errorf("invalid response = %v", r)
		}
		info["maxclients"] = strconv.Itoa(v)
		return info, nil
	}
}

func (c *Client) BecomeMasterAllSlots() error {
	slotsInfo, err := c.PkSlotsInfo()
	if err != nil {
		return errors.Trace(err)
	}
	if len(slotsInfo) == 0 {
		return nil
	}
	_ = c.ReconnectIfNeeded()
	if err := c.SlaveOfAllSlots("NO:ONE", slotsInfo.Slots(), false, false); err != nil {
		return errors.Trace(err)
	}
	return nil
	//host, port, err := net.SplitHostPort(master)
	//if err != nil {
	//	return errors.Trace(err)
	//}
	//c.Send("MULTI")
	//c.Send("CONFIG", "SET", "masterauth", c.Auth)
	//c.Send("SLAVEOF", host, port)
	//c.Send("CONFIG", "REWRITE")
	//c.Send("CLIENT", "KILL", "TYPE", "normal")
	//values, err := redigo.Values(c.Do("EXEC"))
	//if err != nil {
	//	return errors.Trace(err)
	//}
	//for _, r := range values {
	//	if err, ok := r.(redigo.Error); ok {
	//		return errors.Trace(err)
	//	}
	//}
	//return nil
}

func (c *Client) MigrateSlot(slot int, target string) (int, error) {
	host, port, err := net.SplitHostPort(target)
	if err != nil {
		return 0, errors.Trace(err)
	}
	mseconds := int(c.Timeout / time.Millisecond)
	if reply, err := c.Do("SLOTSMGRTTAGSLOT", host, port, mseconds, slot); err != nil {
		return 0, errors.Trace(err)
	} else {
		p, err := redigo.Ints(redigo.Values(reply, nil))
		if err != nil || len(p) != 2 {
			return 0, errors.Errorf("invalid response = %v", reply)
		}
		return p[1], nil
	}
}

type MigrateSlotAsyncOption struct {
	MaxBulks int
	MaxBytes int
	NumKeys  int
	Timeout  time.Duration
}

func (c *Client) MigrateSlotAsync(slot int, target string, option *MigrateSlotAsyncOption) (int, error) {
	host, port, err := net.SplitHostPort(target)
	if err != nil {
		return 0, errors.Trace(err)
	}
	if reply, err := c.Do("SLOTSMGRTTAGSLOT-ASYNC", host, port, int(option.Timeout/time.Millisecond),
		option.MaxBulks, option.MaxBytes, slot, option.NumKeys); err != nil {
		return 0, errors.Trace(err)
	} else {
		p, err := redigo.Ints(redigo.Values(reply, nil))
		if err != nil || len(p) != 2 {
			return 0, errors.Errorf("invalid response = %v", reply)
		}
		return p[1], nil
	}
}

func (c *Client) SlotsInfo() (map[int]int, error) {
	if reply, err := c.Do("SLOTSINFO"); err != nil {
		return nil, errors.Trace(err)
	} else {
		infos, err := redigo.Values(reply, nil)
		if err != nil {
			return nil, errors.Trace(err)
		}
		slots := make(map[int]int)
		for i, info := range infos {
			p, err := redigo.Ints(info, nil)
			if err != nil || len(p) != 2 {
				return nil, errors.Errorf("invalid response[%d] = %v", i, info)
			}
			slots[p[0]] = p[1]
		}
		return slots, nil
	}
}

func (c *Client) Role() (string, error) {
	if reply, err := c.Do("ROLE"); err != nil {
		return "", err
	} else {
		values, err := redigo.Values(reply, nil)
		if err != nil {
			return "", errors.Trace(err)
		}
		if len(values) == 0 {
			return "", errors.Errorf("invalid response = %v", reply)
		}
		role, err := redigo.String(values[0], nil)
		if err != nil {
			return "", errors.Errorf("invalid response[0] = %v", values[0])
		}
		return strings.ToUpper(role), nil
	}
}

func (c *Client) SlotInfo(slot int, full bool) (pika.SlotInfo, error) {
	infoStr := "info"
	if full {
		infoStr = "infofull"
	}
	infoReply, err := c.Do("pkcluster", infoStr, "slot", slot)
	if err != nil {
		return pika.InvalidSlotInfo, err
	}
	infoReplyStr, err := redigo.String(infoReply, nil)
	if err != nil {
		return pika.InvalidSlotInfo, err
	}
	return pika.ParseSlotInfoRaw(infoReplyStr, full)
}

func (c *Client) PkSlotsInfo() (pika.SlotsInfo, error) {
	infoReply, err := c.Do("pkcluster", "info", "slot")
	if err != nil {
		return nil, err
	}
	infoReplyStr, err := redigo.String(infoReply, nil)
	if err != nil {
		return nil, err
	}
	return pika.ParseSlotsInfo(infoReplyStr)
}

func (c *Client) BecomeMaster(slot int) error {
	err, _ := c.slaveOfSlotsInternal("no:one", []int{slot}, strconv.Itoa(slot), false, false, true)
	return err
}

func (c *Client) SlaveOf(masterAddr string, slot int, force, resharding bool) error {
	err, _ := c.slaveOfSlotsInternal(masterAddr, []int{slot}, strconv.Itoa(slot), force, resharding, true)
	return err
}

func (c *Client) SlaveOfSlots(masterAddr string, slots pika.SlotGroup, force, resharding bool) error {
	err, _ := c.slaveOfSlotsInternal(masterAddr, slots.ToList(), slots.String(), force, resharding, true)
	return err
}

func (c *Client) SlaveOfAllSlots(masterAddr string, slaveSlots []int, force, resharding bool) error {
	err, _ := c.slaveOfSlotsInternal(masterAddr, slaveSlots, "all", force, resharding, true)
	return err
}

func (c *Client) slaveOfSlotsInternal(masterAddr string, slots []int, slotsDesc string, force, resharding bool, logging bool) (err error, slavedOfSlots []int) {
	host, port, err := net.SplitHostPort(masterAddr)
	if err != nil {
		log.Warnf("[slaveOfSlotsInternal] split host %s err: '%v'", masterAddr, err)
		return err, nil
	}

	args := []interface{}{"slotsslaveof", host, port, slotsDesc}
	slaveOfOpt := GetSlaveOfOpt(force, resharding)
	if slaveOfOpt != "" {
		args = append(args, slaveOfOpt)
	}

	logf := func(err error, slavedOfSlots []int, desc string) {
		if !logging {
			return
		}
		if err != nil {
			log.Errorf("[slaveOfSlotsInternal] slots-[%s] %s %s slaveof %s failed: %v, newly slaveof-ed slots: %v", slotsDesc, c.Addr, slaveOfOpt, masterAddr, err, pika.Slots(slavedOfSlots))
		} else if desc != "" {
			log.Warnf("[slaveOfSlotsInternal] slots-[%s] %s %s slaveof %s: %s", slotsDesc, c.Addr, slaveOfOpt, masterAddr, desc)
		} else if len(slavedOfSlots) > 0 {
			log.Warnf("[slaveOfSlotsInternal] slots-[%s] %s %s slaveof %s succeeded, newly slaveof-ed slots: %v", slotsDesc, c.Addr, slaveOfOpt, masterAddr, pika.Slots(slavedOfSlots))
		}
	}
	defer func() {
		logf(err, slavedOfSlots, "")
	}()

	if _, err = DoPikaSlotOp(func() (interface{}, error) {
		return c.Do("pkcluster", args...)
	}); err == nil {
		return nil, slots
	}
	if !strings.Contains(err.Error(), pika.ErrMsgSlotAlreadyLinked) {
		return err, []int{}
	}

	linkedSlots, parseErr := pika.ParseAlreadyLinkedErrMsg(err.Error())
	if parseErr != nil {
		return errors.Wrap(err, parseErr), []int{}
	}
	connectedSlots, nonConnectedSlots := linkedSlots.SplitConnectedAndNonConnected()
	if len(nonConnectedSlots) > 0 {
		logf(nil, nil, fmt.Sprintf("slaveof no:one for linked but non-connected slots %v", pika.Slots(nonConnectedSlots)))
		for _, sg := range pika.GroupedSlots(nonConnectedSlots) {
			if connErr := c.ReconnectIfNeeded(); connErr != nil {
				return errors.Wrap(err, connErr), []int{}
			}
			if slaveOfErr, _ := c.slaveOfSlotsInternal("no:one", sg.ToList(), sg.String(), false, resharding, true); slaveOfErr != nil {
				return slaveOfErr, []int{}
			}
		}
	}

	if len(connectedSlots) > 0 {
		logf(nil, nil, fmt.Sprintf("skip already connected slots %v", pika.Slots(connectedSlots)))
	}
	newSlotGroups := pika.GroupedSlotsExcluding(slots, connectedSlots)
	if pika.SlotGroups(newSlotGroups).SlotCount() >= len(slots) && len(nonConnectedSlots) == 0 {
		return errors.Wrap(err, errors.Errorf("impossible: pika.SlotGroups(newSlotGroups).SlotCount() >= len(slots) && len(nonConnectedSlots) == 0, newSlotGroups: %v, slots: %v", newSlotGroups, pika.Slots(slots))), []int{}
	}
	err, slavedOfSlots = nil, make([]int, 0, len(newSlotGroups))
	for _, sg := range newSlotGroups {
		if connErr := c.ReconnectIfNeeded(); connErr != nil {
			return errors.Wrap(err, connErr), slavedOfSlots
		}
		slaveOfErr, partialSlavedOfSlots := c.slaveOfSlotsInternal(masterAddr, sg.ToList(), sg.String(), force, resharding, false)
		err, slavedOfSlots = errors.Wrap(err, slaveOfErr), append(slavedOfSlots, partialSlavedOfSlots...)
	}
	return err, slavedOfSlots
}

func GetSlaveOfOpt(force, resharding bool) string {
	if force {
		if resharding {
			return "force_resharding"
		}
		return "force"
	}
	if resharding {
		return "resharding"
	}
	return ""
}

func (c *Client) GetMaxSlotNum() (int, error) {
	r, err := c.Do("CONFIG", "GET", "default-slot-num")
	if err != nil {
		return -1, errors.Trace(err)
	}
	p, err := redigo.Values(r, nil)
	if err != nil || len(p) != 2 {
		return -1, errors.Errorf("invalid response = %v", r)
	}
	v, err := redigo.Int(p[1], nil)
	if err != nil {
		return -1, errors.Errorf("invalid response = %v", r)
	}
	return v, nil
}

func (c *Client) AddSlot(slot int) error {
	_, err := DoPikaSlotOp(func() (interface{}, error) {
		return c.Do("pkcluster", "addslots", slot)
	})
	return err
}

func (c *Client) DeleteSlot(slot int) error {
	_, err := DoPikaSlotOp(func() (interface{}, error) {
		return c.Do("pkcluster", "delslots", slot)
	})
	return err
}

func (c *Client) GetSlotDBSize(slot int) (int64, error) {
	slotInfo, err := c.SlotInfo(slot, true)
	if err != nil {
		if !strings.Contains(err.Error(), "Err unknown or unsupported command") {
			return 0, err
		}
		log.Warnf("[GetSlotDBSize] pkcluster infofull command not supported, use GetDBSize() instead")
		_ = c.ReconnectIfNeeded()
		dbSize, err := c.GetDBSize()
		if err != nil {
			return 0, err
		}
		slotsInfo, err := c.PkSlotsInfo()
		if err != nil {
			return 0, err
		}
		log.Warnf("[GetSlotDBSize] total db size %v, slots: %v", dbSize, slotsInfo.Slots())
		return dbSize / int64(len(slotsInfo)), nil
	}
	return slotInfo.DBSize, nil
}

func (c *Client) GetDBSize() (int64, error) {
	infos, err := c.Info()
	if err != nil {
		return 0, err
	}
	dbSizeStr, ok := infos["db_size"]
	if !ok {
		return 0, fmt.Errorf("db_size not found")
	}
	dbSize, err := strconv.ParseInt(dbSizeStr, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid db_size: '%s'", dbSizeStr)
	}
	return dbSize, nil
}

func (c *Client) CompactSlot(slot int) error {
	_, err := c.Do("pkcluster", "compactslot", strconv.Itoa(slot))
	if err != nil && strings.Contains(err.Error(), "Err unknown or unsupported command") {
		log.Errorf("%s, pika addr: %s", err.Error(), c.Addr)
		return ErrCommandNotSupported
	}
	return err
}

var ErrClosedPool = errors.New("use of closed redis pool")

type Pool struct {
	mu sync.Mutex

	auth string
	pool map[string]*list.List

	timeout time.Duration

	exit struct {
		C chan struct{}
	}

	closed bool
}

func NewPool(auth string, timeout time.Duration) *Pool {
	p := &Pool{
		auth: auth, timeout: timeout,
		pool: make(map[string]*list.List),
	}
	p.exit.C = make(chan struct{})

	if timeout != 0 {
		go func() {
			var ticker = time.NewTicker(time.Minute)
			defer ticker.Stop()
			for {
				select {
				case <-p.exit.C:
					return
				case <-ticker.C:
					p.Cleanup()
				}
			}
		}()
	}

	return p
}

func (p *Pool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil
	}
	p.closed = true
	close(p.exit.C)

	for addr, list := range p.pool {
		for i := list.Len(); i != 0; i-- {
			c := list.Remove(list.Front()).(*Client)
			c.Close()
		}
		delete(p.pool, addr)
	}
	return nil
}

func (p *Pool) Cleanup() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return ErrClosedPool
	}

	for addr, list := range p.pool {
		for i := list.Len(); i != 0; i-- {
			c := list.Remove(list.Front()).(*Client)
			if !c.isRecyclable() {
				c.Close()
			} else {
				list.PushBack(c)
			}
		}
		if list.Len() == 0 {
			delete(p.pool, addr)
		}
	}
	return nil
}

func (p *Pool) GetClient(addr string) (*Client, error) {
	c, err := p.getClientFromCache(addr)
	if err != nil || c != nil {
		return c, err
	}
	return NewClient(addr, p.auth, p.timeout)
}

func (p *Pool) getClientFromCache(addr string) (*Client, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil, ErrClosedPool
	}
	if list := p.pool[addr]; list != nil {
		for i := list.Len(); i != 0; i-- {
			c := list.Remove(list.Front()).(*Client)
			if !c.isRecyclable() {
				c.Close()
			} else {
				return c, nil
			}
		}
	}
	return nil, nil
}

func (p *Pool) PutClient(c *Client) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !c.isRecyclable() || p.closed {
		c.Close()
	} else {
		cache := p.pool[c.Addr]
		if cache == nil {
			cache = list.New()
			p.pool[c.Addr] = cache
		}
		cache.PushFront(c)
	}
}

func (p *Pool) Info(addr string) (_ map[string]string, err error) {
	c, err := p.GetClient(addr)
	if err != nil {
		return nil, err
	}
	defer p.PutClient(c)
	return c.Info()
}

func (p *Pool) InfoAll(addr string) (_ map[string]string, err error) {
	m, err := p.InfoFull(addr)
	if err != nil || m == nil || m["role"] != "master" {
		return m, err
	}

	c, err := p.GetClient(addr)
	if err != nil {
		return nil, err
	}
	defer p.PutClient(c)

	slotsInfo, err := c.PkSlotsInfo()
	if err != nil {
		return nil, err
	}
	slotsInfoBytes, err := json.Marshal(slotsInfo)
	if err != nil {
		return nil, err
	}
	m["slots_info"] = string(slotsInfoBytes)
	return m, nil
}

func (p *Pool) InfoFull(addr string) (_ map[string]string, err error) {
	c, err := p.GetClient(addr)
	if err != nil {
		return nil, err
	}
	defer p.PutClient(c)
	return c.InfoFull()
}

func (p *Pool) WithRedisClient(addr string, do func(*Client) error) error {
	if addr == "" {
		return errors.Errorf("WithRedisClient: addr is empty")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()

	for {
		poolErr, userErr := func() (error, error) {
			cli, err := p.GetClient(addr)
			if err != nil {
				return errors.Errorf("can't get redis client for %s: %v", addr, err), nil
			}
			defer p.PutClient(cli)

			if err := cli.Good(); err != nil {
				return errors.Errorf("redis client of %s not good: %v", addr, err), nil
			}

			return nil, do(cli)
		}()

		if poolErr == nil {
			return userErr
		}

		select {
		case <-ctx.Done():
			log.Errorf("WithRedisClient: %v", poolErr)
			return poolErr
		default:
			time.Sleep(200 * time.Millisecond)
		}
	}
}

func (p *Pool) GetPikaSlotInfo(addr string, slot int) (slotInfo pika.SlotInfo, _ error) {
	return slotInfo, p.WithRedisClient(addr, func(client *Client) (err error) {
		slotInfo, err = client.SlotInfo(slot, false)
		return
	})
}

func (p *Pool) GetPikaSlotsInfo(addr string) (slotsInfo map[int]pika.SlotInfo, _ error) {
	return slotsInfo, p.WithRedisClient(addr, func(client *Client) (err error) {
		slotsInfo, err = client.PkSlotsInfo()
		return
	})
}

func (p *Pool) GetPikasSlotsInfo(addrs []string) map[string]map[int]pika.SlotInfo {
	if len(addrs) == 1 {
		slotsInfo, err := p.GetPikaSlotsInfo(addrs[0])
		if err != nil {
			log.Warnf("[getPikasSlotInfo] can't get slots info for pika '%s'", addrs[0])
			return map[string]map[int]pika.SlotInfo{}
		}
		return map[string]map[int]pika.SlotInfo{addrs[0]: slotsInfo}
	}

	var (
		m     = make(map[string]map[int]pika.SlotInfo)
		mutex sync.Mutex
	)

	if len(addrs) == 0 {
		return m
	}

	var addrSet = make(map[string]struct{})
	for _, addr := range addrs {
		if addr != "" {
			addrSet[addr] = struct{}{}
		}
	}

	var wg sync.WaitGroup
	for pikaAddr := range addrSet {
		wg.Add(1)

		go func(addr string) {
			defer wg.Done()

			slotInfos, err := p.GetPikaSlotsInfo(addr)
			if err != nil {
				log.Warnf("[getPikasSlotInfo] can't get slots info for pika '%s', err: '%v'", addr, err)
				return
			}

			mutex.Lock()
			m[addr] = slotInfos
			mutex.Unlock()
		}(pikaAddr)
	}
	wg.Wait()

	return m
}

func (p *Pool) GetMaxSlotNums(addrs []string) map[string]int {
	var (
		m     = make(map[string]int)
		mutex sync.Mutex
	)

	if len(addrs) == 0 {
		return m
	}

	var addrSet = make(map[string]struct{})
	for _, addr := range addrs {
		if addr != "" {
			addrSet[addr] = struct{}{}
		}
	}

	var wg sync.WaitGroup
	for pikaAddr := range addrSet {
		wg.Add(1)

		go func(addr string) {
			defer wg.Done()

			var maxSlotNum int
			if err := p.WithRedisClient(addr, func(client *Client) (err error) {
				maxSlotNum, err = client.GetMaxSlotNum()
				return err
			}); err != nil {
				log.Warnf("[GetMaxSlotNums] can't get slots info for pika '%s', err: '%v'", addr, err)
				return
			}

			mutex.Lock()
			m[addr] = maxSlotNum
			mutex.Unlock()
		}(pikaAddr)
	}
	wg.Wait()

	return m
}

func (p *Pool) CompactSlot(addrs []string, slot int) error {
	if len(addrs) == 0 {
		return nil
	}

	var (
		wg   sync.WaitGroup
		errs = make([]error, len(addrs))
	)
	for i, addr := range addrs {
		wg.Add(1)

		go func(i int, addr string) {
			defer wg.Done()

			if errs[i] = p.WithRedisClient(addr, func(client *Client) (err error) {
				return client.CompactSlot(slot)
			}); errs[i] != nil {
				log.Warnf("[CompactSlot] can't get slots info for pika '%s', err: '%v'", addr, errs[i])
			}
		}(i, addr)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			return fmt.Errorf("compact slot of pika '%s' failed: '%v'", addrs[i], err)
		}
	}
	return nil
}

func (p *Pool) GetSlotDBSize(addr string, slot int) (dbSize int64, _ error) {
	return dbSize, p.WithRedisClient(addr, func(client *Client) (err error) {
		dbSize, err = client.GetSlotDBSize(slot)
		return err
	})
}

func (p *Pool) GetPikasSlotDBSize(addrs []string, slot int) ([]int64, error) {
	if len(addrs) == 0 {
		return nil, nil
	}

	var (
		wg      sync.WaitGroup
		dbSizes = make([]int64, len(addrs))
		errs    = make([]error, len(addrs))
	)
	for i, addr := range addrs {
		wg.Add(1)

		go func(i int, addr string) {
			defer wg.Done()

			var dbSize int64
			if errs[i] = p.WithRedisClient(addr, func(client *Client) (err error) {
				dbSize, err = client.GetSlotDBSize(slot)
				return err
			}); errs[i] == nil {
				dbSizes[i] = dbSize
			}
		}(i, addr)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			return nil, fmt.Errorf("get slot db size of pika '%s' failed: '%v'", addrs[i], err)
		}
	}
	return dbSizes, nil
}

func (p *Pool) AddSlotIfNotExists(addr string, slot int) error {
	err := p.WithRedisClient(addr, func(client *Client) error {
		return client.AddSlot(slot)
	})
	if err == nil {
		return nil
	}
	if _, getErr := p.GetPikaSlotInfo(addr, slot); getErr == nil {
		return nil // error pruning
	}
	return err
}

// Delete slot if exists
func (p *Pool) CleanSlotIfExists(addr string, slot int) error {
	err := p.WithRedisClient(addr, func(client *Client) error {
		return client.DeleteSlot(slot)
	})
	if err == nil {
		return nil
	}
	if _, getErr := p.GetPikaSlotInfo(addr, slot); getErr == pika.ErrSlotNotExists {
		return nil // error pruning
	}
	return err
}

func (p *Pool) SlaveOfAsync(masterAddr, slaveAddr string, slot int, force, resharding bool) error {
	return p.WithRedisClient(slaveAddr, func(client *Client) error {
		return client.SlaveOf(masterAddr, slot, force, resharding)
	})
}

func (p *Pool) BecomeMaster(slaveAddr string, slot int) error {
	return p.WithRedisClient(slaveAddr, func(client *Client) error {
		return client.BecomeMaster(slot)
	})
}

func (p *Pool) UnlinkAllSlaves(masterAddr string, slot int) error {
	masterSlotInfo, err := p.GetPikaSlotInfo(masterAddr, slot)
	if err != nil {
		return err
	}
	return p.UnlinkSlaves(masterAddr, masterSlotInfo.SlaveAddrs(), slot, func(masterAddr string, slot int) (pika.SlotInfo, error) {
		return p.GetPikaSlotInfo(masterAddr, slot)
	})
}

func (p *Pool) UnlinkSlaves(masterAddr string, slaveAddrs []string, slot int,
	masterSlotInfoGetter func(masterAddr string, slot int) (pika.SlotInfo, error)) (err error) {
	for _, slaveAddr := range slaveAddrs {
		if err = p.BecomeMaster(slaveAddr, slot); err != nil {
			return err
		}
	}
	if err = utils.WithRetry(time.Millisecond*100, time.Second*2, func() error {
		masterSlotInfo, err := masterSlotInfoGetter(masterAddr, slot)
		if err != nil {
			return err
		}
		for _, slaveAddr := range slaveAddrs {
			if masterSlotInfo.IsSlaveLinked(slaveAddr) {
				return fmt.Errorf("slot-[%d] detach failed: slave %s found on master %s", slot, slaveAddr, masterAddr)
			}
		}
		return nil
	}); err != nil {
		log.Errorf("[UnlinkSlaves] slot-[%d] detach failed: '%v'", slot, err)
	}
	return err
}

type InfoCache struct {
	mu sync.Mutex

	Auth string
	data map[string]map[string]string

	Timeout time.Duration
}

func (s *InfoCache) load(addr string) map[string]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.data != nil {
		return s.data[addr]
	}
	return nil
}

func (s *InfoCache) store(addr string, info map[string]string) map[string]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.data == nil {
		s.data = make(map[string]map[string]string)
	}
	if info != nil {
		s.data[addr] = info
	} else if s.data[addr] == nil {
		s.data[addr] = make(map[string]string)
	}
	return s.data[addr]
}

func (s *InfoCache) Get(addr string) map[string]string {
	info := s.load(addr)
	if info != nil {
		return info
	}
	info, _ = s.getSlow(addr)
	return s.store(addr, info)
}

func (s *InfoCache) GetRunId(addr string) string {
	return s.Get(addr)["run_id"]
}

func (s *InfoCache) getSlow(addr string) (map[string]string, error) {
	c, err := NewClient(addr, s.Auth, s.Timeout)
	if err != nil {
		return nil, err
	}
	defer c.Close()
	return c.Info()
}
