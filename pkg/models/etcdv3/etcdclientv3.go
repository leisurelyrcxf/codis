// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package etcdclientv3

import (
	"context"
	"fmt"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/CodisLabs/codis/pkg/models/common"
	"github.com/CodisLabs/codis/pkg/utils/assert"
	"github.com/CodisLabs/codis/pkg/utils/errors"
	"github.com/CodisLabs/codis/pkg/utils/log"
	"go.etcd.io/etcd/v3/clientv3"
)

var ErrClosedClient = errors.New("use of closed etcd client")
var ErrKeyNotExists = errors.New("key not exists")

type Client struct {
	sync.Mutex
	kapi clientv3.KV
	c    *clientv3.Client

	closed  bool
	timeout time.Duration

	cancel  context.CancelFunc
	context context.Context

	leaseID clientv3.LeaseID
}

func New(addrlist string, auth string, timeout time.Duration) (*Client, error) {
	endpoints := strings.Split(addrlist, ",")
	for i, s := range endpoints {
		if s != "" && !strings.HasPrefix(s, "http://") {
			endpoints[i] = "http://" + s
		}
	}
	if timeout <= 0 {
		timeout = time.Second * 5
	}

	config := clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: time.Second * 5,
	}

	if auth != "" {
		split := strings.SplitN(auth, ":", 2)
		if len(split) != 2 || split[0] == "" {
			return nil, errors.Errorf("invalid auth")
		}
		config.Username = split[0]
		config.Password = split[1]
	}

	c, err := clientv3.New(config)
	if err != nil {
		return nil, errors.Trace(err)
	}

	client := &Client{
		kapi: clientv3.NewKV(c), timeout: timeout, c: c,
	}
	client.context, client.cancel = context.WithCancel(context.Background())
	return client, nil
}

func (c *Client) Close() error {
	c.Lock()
	defer c.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	c.cancel()
	return c.c.Close()
}

func (c *Client) IsClosed() bool {
	c.Lock()
	defer c.Unlock()
	return c.closed
}

func (c *Client) newContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(c.context, c.timeout)
}

func (c *Client) Create(path string, data []byte) error {
	c.Lock()
	defer c.Unlock()
	if c.closed {
		return errors.Trace(ErrClosedClient)
	}

	ctx, cancel := c.newContext()
	defer cancel()
	log.Debugf("etcd create node %s", path)

	req := clientv3.OpPut(path, string(data))
	cond := clientv3.Compare(clientv3.Version(path), "=", 0)
	resp, err := c.kapi.Txn(ctx).If(cond).Then(req).Commit()
	if err != nil {
		log.Debugf("etcd create node %s failed: %s", path, err)
		return errors.Trace(err)
	}
	if !resp.Succeeded {
		err = common.ErrKeyAlreadyExists
		log.Debugf("etcd create node %s failed: %s", path, err)
		return err
	}
	log.Debugf("etcdclientv3 create OK")
	return nil
}

func (c *Client) Update(path string, data []byte) error {
	c.Lock()
	defer c.Unlock()
	if c.closed {
		return errors.Trace(ErrClosedClient)
	}
	cntx, cancel := c.newContext()
	defer cancel()
	log.Debugf("etcd update node %s", path)
	_, err := c.kapi.Put(cntx, path, string(data))
	if err != nil {
		log.Debugf("etcd update node %s failed: %s", path, err)
		return errors.Trace(err)
	}
	log.Debugf("etcd update OK")
	return nil
}

func (c *Client) Delete(path string) error {
	return c.delete(path, "delete node")
}

func (c *Client) Rmdir(dir string) error {
	return c.delete(dir, "rmdir", clientv3.WithPrefix())
}

func (c *Client) delete(path string, desc string, opts ...clientv3.OpOption) error {
	c.Lock()
	defer c.Unlock()
	if c.closed {
		return errors.Trace(ErrClosedClient)
	}
	log.Debugf("etcd %s %s", desc, path)
	cntx, cancel := c.newContext()
	defer cancel()
	if _, err := c.kapi.Delete(cntx, path, opts...); err != nil {
		log.Errorf("etcd %s %s failed: %s", desc, path, err)
		return errors.Trace(err)
	}
	log.Debugf("etcd %s %s OK", desc, path)
	return nil
}

func (c *Client) Read(path string, must bool) ([]byte, error) {
	c.Lock()
	defer c.Unlock()
	if c.closed {
		return nil, errors.Trace(ErrClosedClient)
	}
	cntx, cancel := c.newContext()
	defer cancel()
	// By default, etcd clientv3 use same config as old 'client.GetOptions{Quorum: true}'
	r, err := c.kapi.Get(cntx, path)
	if err != nil {
		log.Debugf("etcd read node %s failed: %s", path, err)
		return nil, errors.Trace(err)
	}
	// Otherwise should return error instead.
	if len(r.Kvs) == 0 {
		if must {
			return nil, ErrKeyNotExists
		}
		return nil, nil
	}
	return r.Kvs[0].Value, nil
}

func (c *Client) List(path string) ([]string, error) {
	c.Lock()
	defer c.Unlock()
	return c.listLocked(path)
}

func (c *Client) listLocked(path string) ([]string, error) {
	if c.closed {
		return nil, errors.Trace(ErrClosedClient)
	}
	cntx, cancel := c.newContext()
	defer cancel()
	opts := []clientv3.OpOption{
		clientv3.WithPrefix(),
		clientv3.WithKeysOnly(),
	}
	r, err := c.kapi.Get(cntx, path, opts...)
	switch {
	case err != nil:
		log.Debugf("etcd list node %s failed: %s", path, err)
		return nil, errors.Trace(err)
	default:
		var (
			listedPathSet = make(map[string]struct{})
			listedPaths   []string
		)
		for _, kv := range r.Kvs {
			key := string(kv.Key)
			if len(key) < len(path) {
				return nil, errors.Errorf("impossible: key_len(%d) < path_len(%d)", len(kv.Key), len(path))
			}
			remain := key[len(path):]
			if strings.HasPrefix(remain, "/") {
				remain = remain[1:]
			}
			if remain == "" {
				continue
			}
			if firstSlashIdx := strings.IndexByte(remain, '/'); firstSlashIdx != -1 {
				listedPathSet[remain[:firstSlashIdx]] = struct{}{}
			} else {
				listedPathSet[remain] = struct{}{}
			}
		}
		for listedPath := range listedPathSet {
			listedPaths = append(listedPaths, listedPath)
		}
		sort.Strings(listedPaths)
		for idx := range listedPaths {
			listedPaths[idx] = filepath.Join(path, listedPaths[idx])
		}
		return listedPaths, nil
	}
}

func (c *Client) CreateEphemeral(path string, data []byte) (<-chan struct{}, error) {
	return c.CreateEphemeralWithTimeout(path, data, c.timeout)
}

func (c *Client) CreateEphemeralWithTimeout(path string, data []byte, timeout time.Duration) (ch <-chan struct{}, err error) {
	c.Lock()
	defer c.Unlock()
	if c.closed {
		return nil, errors.Trace(ErrClosedClient)
	}
	cntxLease, cancelLease := c.newContext()
	defer cancelLease()
	log.Debugf("etcd create-ephemeral node %s", path)

	timeoutInSeconds := int64(timeout) / int64(time.Second)
	if timeoutInSeconds < 25 {
		timeoutInSeconds = 25
	}
	lease, err := c.c.Grant(cntxLease, timeoutInSeconds)
	if err != nil {
		log.Debugf("etcd create-lease for node %s failed: %s", path, err)
		return nil, errors.Trace(err)
	}

	defer func() {
		if err != nil {
			rCtx, rCancel := c.newContext()
			_, _ = c.c.Revoke(rCtx, lease.ID)
			rCancel()
		}
	}()

	cntx, cancel := c.newContext()
	defer cancel()

	req := clientv3.OpPut(path, string(data), clientv3.WithLease(lease.ID))
	cond := clientv3.Compare(clientv3.Version(path), "=", 0)
	var resp *clientv3.TxnResponse
	resp, err = c.kapi.Txn(cntx).If(cond).Then(req).Commit()
	if err != nil {
		log.Debugf("etcd create-ephemeral node %s failed: %s", path, err)
		return nil, errors.Trace(err)
	}
	if !resp.Succeeded {
		err = common.ErrKeyAlreadyExists
		log.Debugf("etcd create-ephemeral node %s failed: %s", path, err)
		return nil, err
	}
	log.Debugf("etcd create-ephemeral OK")
	return runRefreshEphemeral(c, lease.ID)
}

func runRefreshEphemeral(c *Client, leaseID clientv3.LeaseID) (<-chan struct{}, error) {
	ctx, cancel := context.WithCancel(clientv3.WithRequireLeader(c.context))
	keepAlive, err := c.c.KeepAlive(ctx, leaseID)
	if err != nil {
		cancel()
		return nil, err
	}
	if keepAlive == nil {
		cancel()
		return nil, fmt.Errorf("keepAlive is nil")
	}

	signal := make(chan struct{})
	go func() {
		defer close(signal)
		for range keepAlive {
			// eat messages until keep alive channel closes
		}
	}()
	cancel = nil // suppress go-vet warning
	return signal, nil
}

func (c *Client) MkDir(path string) error {
	return c.Create(path, []byte{})
}

// Simulate v2 createInOrder with transactions.
// Unfortunately etcd v3 doesn't have api to compare keys,
// we have to add a 10 bytes prefix to the stored data.
// You have to use ReadEphemeralInOrder() to retrieve the data.
func (c *Client) CreateEphemeralInOrder(path string, data []byte) (<-chan struct{}, string, error) {
	c.Lock()
	defer c.Unlock()
	if c.closed {
		return nil, "", errors.Trace(ErrClosedClient)
	}
	var err error
	cntxLease, cancelLease := c.newContext()
	defer cancelLease()
	log.Debugf("etcd create-ephemeral node %s", path)

	var lease *clientv3.LeaseGrantResponse
	lease, err = c.c.Grant(cntxLease, int64(c.timeout)/int64(time.Second))
	if err != nil {
		log.Debugf("etcd create-ephemeral get lease for node %s failed: %s", path, err)
		return nil, "", errors.Trace(err)
	}
	defer func() {
		if err != nil {
			rCtx, rCancel := c.newContext()
			_, _ = c.c.Revoke(rCtx, lease.ID)
			rCancel()
		}
	}()

	var key string
	cntx, cancel := c.newContext()
	defer cancel()

	for {
		var maxKey string
		maxKey, err = c.getMaxKeyLocked(path)
		if err != nil {
			log.Debugf("etcd create-ephemeral get prev-version for node %s failed: %s", path, err)
			return nil, "", err
		}
		var v int64 = -1
		if len(maxKey) == len(path)+10 {
			versionStr := maxKey[len(path)+1:]
			v, err = strconv.ParseInt(versionStr, 10, 64)
			if err != nil {
				log.Debugf("etcd create-ephemeral get previous version for node %s failed: %s", path, err)
				return nil, "", err
			}
		} else if len(maxKey) != len(path) {
			return nil, "", fmt.Errorf("corrupted dir '%s', contains non-formal key '%s'", path, maxKey)
		}
		v++
		if v >= 1000000000 {
			return nil, "", fmt.Errorf("version overflowed max 1000000000")
		}

		key = fmt.Sprintf("%s/%09d", path, v)
		// Add prefix to value so that we can compare the version of key with clientv3.Value(path),
		// unfortunately etcd v3 doesn't have api to compare keys.
		// Uses have to retrieve the data by themselves.
		val := fmt.Sprintf("%09d_%s", v, string(data))

		var resp *clientv3.TxnResponse
		req := clientv3.OpPut(key, val, clientv3.WithLease(lease.ID))
		cond := clientv3.Compare(clientv3.Value(path).WithPrefix(), "<", fmt.Sprintf("%09d", v))
		resp, err = c.kapi.Txn(cntx).If(cond).Then(req).Commit()
		if err != nil {
			log.Debugf("etcd create-ephemeral node %s failed: %s", path, err)
			return nil, "", errors.Trace(err)
		}
		if !resp.Succeeded {
			_, err = c.c.KeepAliveOnce(cntx, lease.ID)
			if err != nil {
				log.Debugf("etcd refresh-ephemeral node %s failed: %s", path, err)
				return nil, "", errors.Trace(err)
			}
			continue
		}
		break
	}
	log.Debugf("etcd create-ephemeral OK")
	c.leaseID = lease.ID
	signal, err := runRefreshEphemeral(c, lease.ID)
	return signal, key, err
}

func (c *Client) GetMinKey(path string) (string, error) {
	c.Lock()
	defer c.Unlock()
	return c.getMinKeyLocked(path)
}

func (c *Client) getMinKeyLocked(path string) (string, error) {
	return c.getMinMaxKeyLocked(path, clientv3.SortAscend)
}

func (c *Client) GetMaxKey(path string) (string, error) {
	c.Lock()
	defer c.Unlock()
	return c.getMaxKeyLocked(path)
}

func (c *Client) getMaxKeyLocked(path string) (string, error) {
	return c.getMinMaxKeyLocked(path, clientv3.SortDescend)
}

func (c *Client) getMinMaxKeyLocked(path string, sortOrder clientv3.SortOrder) (string, error) {
	if c.closed {
		return "", errors.Trace(ErrClosedClient)
	}
	cntx, cancel := c.newContext()
	defer cancel()
	opts := []clientv3.OpOption{
		clientv3.WithPrefix(),
		clientv3.WithSort(clientv3.SortByKey, sortOrder),
		clientv3.WithLimit(1),
	}
	r, err := c.kapi.Get(cntx, path, opts...)
	switch {
	case err != nil:
		log.Debugf("etcd list node %s failed: %s", path, err)
		return "", errors.Trace(err)
	default:
		var paths []string
		for _, kv := range r.Kvs {
			paths = append(paths, string(kv.Key))
		}
		if len(paths) == 0 {
			return "", ErrKeyNotExists
		}
		assert.Must(len(paths) == 1)
		return paths[0], nil
	}
}

func (c *Client) ReadEphemeralInOrder(path string, must bool) ([]byte, error) {
	data, err := c.Read(path, must)
	if err != nil {
		return data, err
	}
	if len(data) == 0 {
		return data, nil
	}
	if len(data) < 10 {
		return nil, fmt.Errorf("len(data) < 10, corrupted value")
	}
	return data[10:], nil
}

func (c *Client) WatchInOrder(path string) (<-chan struct{}, []string, error) {
	err := c.MkDir(path)
	if err != nil && err != common.ErrKeyAlreadyExists {
		return nil, nil, err
	}

	c.Lock()
	defer c.Unlock()
	if c.closed {
		return nil, nil, errors.Trace(ErrClosedClient)
	}

	log.Debugf("etcd watch-inorder node %s", path)

	paths, err := c.listLocked(path)
	if err != nil {
		return nil, nil, err
	}

	signal := make(chan struct{})
	watched := make(chan struct{})
	go func() {
		defer close(signal)

		cancellableCtx, canceller := context.WithCancel(clientv3.WithRequireLeader(c.context))
		// GC watched chan, otherwise will memory leak.
		defer canceller()
		ch := c.c.Watch(cancellableCtx, path, clientv3.WithPrefix())
		close(watched)
		for resp := range ch {
			for _, event := range resp.Events {
				log.Debugf("etcd watch-inorder node %s update, event: %v", path, event)
				return
			}
		}
	}()

	select {
	case <-time.After(c.timeout):
		return nil, nil, fmt.Errorf("watch timeouted")
	case <-watched:
		break
	}

	log.Debugf("etcd watch-inorder OK")
	return signal, paths, nil
}

func (c *Client) CAS(ctx, parent context.Context, path string, data []byte, ttl int64, deadlineLoopInterval time.Duration,
	onError func(desc, reason error) bool, beforeKeepAliveOnceFunc common.BeforeKeepAliveOnceFunc) (
	curData []byte, curRev int64, lease *common.Lease, err error) {
	c.Lock()
	defer c.Unlock()
	if c.closed {
		return nil, -1, nil, errors.Trace(ErrClosedClient)
	}

	log.Debugf("[CAS] cas node %s", path)
	leaseGrantTime := time.Now()
	leaseResp, err := c.c.Grant(ctx, ttl)
	if err != nil {
		log.Debugf("[CAS] etcd create-lease for node %s failed: %s", path, err)
		return nil, -1, nil, errors.Trace(err)
	}
	if leaseResp.Error != "" {
		log.Debugf("[CAS] etcd create-lease for node %s failed, leaseResp.Error: %s", path, leaseResp.Error)
		return nil, -1, nil, errors.Trace(errors.New(leaseResp.Error))
	}

	lease = common.NewLease(
		parent, c, leaseResp.ID, leaseResp.TTL,
		deadlineLoopInterval, leaseGrantTime,
		onError, beforeKeepAliveOnceFunc)
	defer func() {
		if err != nil {
			lease.Close()
			lease = nil
		}
	}()

	put := clientv3.OpPut(path, string(data), clientv3.WithLease(leaseResp.ID))
	get := clientv3.OpGet(path)
	cond := clientv3.Compare(clientv3.Version(path), "=", 0)
	txnResp, err := c.kapi.Txn(ctx).If(cond).Then(put).Else(get).Commit()
	if err != nil {
		log.Errorf("[CAS] etcd cas transaction for node %s failed: %s", path, err)
		return nil, -1, lease, errors.Trace(err)
	}
	if !txnResp.Succeeded {
		log.Infof("[CAS] etcd cas transaction node %s not succeed", path)
		kv := txnResp.Responses[0].GetResponseRange().Kvs[0] // don't worry, etcd lib do like this too...
		return kv.Value, kv.ModRevision, lease, common.ErrKeyAlreadyExists
	}
	return data, txnResp.Header.Revision, lease, nil
}

func (c *Client) KeepAliveOnce(ctx context.Context, lease *common.Lease, _ int64) (newTTL int64, kontinue bool, err error) {
	if c.IsClosed() {
		return 0, false, errors.Trace(ErrClosedClient)
	}

	resp, err := c.c.KeepAliveOnce(clientv3.WithRequireLeader(ctx), lease.ID.(clientv3.LeaseID))
	if err != nil {
		return -1, true, err
	}
	return resp.TTL, true, nil
}

func (c *Client) WatchOnce(ctx context.Context, path string, rev int64) error {
	c.Lock()
	if c.closed {
		c.Unlock()
		return errors.Trace(ErrClosedClient)
	}

	watchRev := rev + 1
	log.Debugf("etcdv3 watchOnce on key %s @rev %d", path, watchRev)

	cntx, cancel := context.WithCancel(clientv3.WithRequireLeader(ctx))
	ch := c.c.Watch(cntx, path, clientv3.WithRev(watchRev))
	c.Unlock()
	defer cancel()

	resp := <-ch
	if err := resp.Err(); err != nil {
		log.Warnf("etcdv3 watched key %s @rev %d failed: '%v'", path, watchRev, err)
		return err
	}

	if len(resp.Events) == 0 {
		log.Debugf("etcdv3 watched key %s @rev %d timeout ", path, watchRev)
		return ctx.Err()
	}
	log.Debugf("etcdv3 watched key %s @rev %d received events: %v", path, watchRev, resp.Events)
	return nil
}

func (c *Client) DeleteRevision(path string, rev int64) error {
	c.Lock()
	defer c.Unlock()
	if c.closed {
		return errors.Trace(ErrClosedClient)
	}

	log.Debugf("etcd delete %s @revision %d", path, rev)
	cntx, cancel := c.newContext()
	defer cancel()

	del := clientv3.OpDelete(path)
	cond := clientv3.Compare(clientv3.ModRevision(path), "=", rev)
	if _, err := c.kapi.Txn(cntx).If(cond).Then(del).Commit(); err != nil {
		log.Debugf("[CAS] etcd delete %s @revision %d failed: %v", path, rev, err)
		return err
	}
	return nil
}

func (c *Client) Revoke(ctx context.Context, lease *common.Lease) error {
	if c.IsClosed() {
		return errors.Trace(ErrClosedClient)
	}

	_, err := c.c.Revoke(ctx, lease.ID.(clientv3.LeaseID))
	return err
}
