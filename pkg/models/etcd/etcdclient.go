// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package etcdclient

import (
	"context"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/CodisLabs/codis/pkg/models/common"
	"github.com/CodisLabs/codis/pkg/utils/errors"
	"github.com/CodisLabs/codis/pkg/utils/log"
	"go.etcd.io/etcd/v3/client"
)

var ErrClosedClient = errors.New("use of closed etcd client")

var (
	ErrNotDir  = errors.New("etcd: not a dir")
	ErrNotFile = errors.New("etcd: not a file")
)

type Client struct {
	sync.Mutex
	kapi client.KeysAPI

	closed  bool
	timeout time.Duration

	cancel  context.CancelFunc
	context context.Context
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

	config := client.Config{
		Endpoints: endpoints, Transport: client.DefaultTransport,
		HeaderTimeoutPerRequest: time.Second * 5,
	}

	if auth != "" {
		split := strings.SplitN(auth, ":", 2)
		if len(split) != 2 || split[0] == "" {
			return nil, errors.Errorf("invalid auth")
		}
		config.Username = split[0]
		config.Password = split[1]
	}

	c, err := client.New(config)
	if err != nil {
		return nil, errors.Trace(err)
	}

	cli := &Client{
		kapi: client.NewKeysAPI(c), timeout: timeout,
	}
	cli.context, cli.cancel = context.WithCancel(context.Background())
	return cli, nil
}

func (c *Client) Close() error {
	c.Lock()
	defer c.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	c.cancel()
	return nil
}

func (c *Client) IsClosed() bool {
	c.Lock()
	defer c.Unlock()
	return c.closed
}

func (c *Client) newContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(c.context, c.timeout)
}

func isErrNoNode(err error) bool {
	if err != nil {
		if e, ok := err.(client.Error); ok {
			return e.Code == client.ErrorCodeKeyNotFound
		}
	}
	return false
}

func isErrNodeExists(err error) bool {
	if err != nil {
		if e, ok := err.(client.Error); ok {
			return e.Code == client.ErrorCodeNodeExist
		}
	}
	return false
}

func isErrRevisionNotFound(err error) bool {
	if err != nil {
		if e, ok := err.(client.Error); ok {
			return e.Code == client.ErrorCodeTestFailed && e.Message == "Compare failed"
		}
	}
	return false
}

func (c *Client) Mkdir(path string) error {
	c.Lock()
	defer c.Unlock()
	if c.closed {
		return errors.Trace(ErrClosedClient)
	}
	log.Debugf("etcd mkdir node %s", path)
	cntx, cancel := c.newContext()
	defer cancel()
	_, err := c.kapi.Set(cntx, path, "", &client.SetOptions{Dir: true, PrevExist: client.PrevNoExist})
	if err != nil && !isErrNodeExists(err) {
		log.Debugf("etcd mkdir node %s failed: %s", path, err)
		return errors.Trace(err)
	}
	log.Debugf("etcd mkdir OK")
	return nil
}

func (c *Client) Create(path string, data []byte) error {
	c.Lock()
	defer c.Unlock()
	if c.closed {
		return errors.Trace(ErrClosedClient)
	}
	cntx, cancel := c.newContext()
	defer cancel()
	log.Debugf("etcd create node %s", path)
	_, err := c.kapi.Set(cntx, path, string(data), &client.SetOptions{PrevExist: client.PrevNoExist})
	if err != nil {
		log.Debugf("etcd create node %s failed: %s", path, err)
		return errors.Trace(err)
	}
	log.Debugf("etcd create OK")
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
	_, err := c.kapi.Set(cntx, path, string(data), &client.SetOptions{PrevExist: client.PrevIgnore})
	if err != nil {
		log.Debugf("etcd update node %s failed: %s", path, err)
		return errors.Trace(err)
	}
	log.Debugf("etcd update OK")
	return nil
}

func (c *Client) Delete(path string) error {
	return c.delete(path, nil, "delete node")
}

func (c *Client) DeleteRevision(path string, rev int64) error {
	return c.delete(path, &client.DeleteOptions{PrevIndex: uint64(rev)}, fmt.Sprintf("delete node of revision %d", rev))
}

func (c *Client) Rmdir(dir string) error {
	return c.delete(dir, &client.DeleteOptions{Recursive: true, Dir: true}, "rmdir")
}

func (c *Client) delete(path string, opt *client.DeleteOptions, desc string) error {
	c.Lock()
	defer c.Unlock()
	if c.closed {
		return errors.Trace(ErrClosedClient)
	}
	cntx, cancel := c.newContext()
	defer cancel()
	log.Debugf("etcd %s %s", desc, path)
	_, err := c.kapi.Delete(cntx, path, opt)
	if err != nil && !isErrNoNode(err) && !isErrRevisionNotFound(err) {
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
	r, err := c.kapi.Get(cntx, path, &client.GetOptions{Quorum: true})
	switch {
	case err != nil:
		if isErrNoNode(err) && !must {
			return nil, nil
		}
		log.Debugf("etcd read node %s failed: %s", path, err)
		return nil, errors.Trace(err)
	case !r.Node.Dir:
		return []byte(r.Node.Value), nil
	default:
		log.Debugf("etcd read node %s failed: not a file", path)
		return nil, errors.Trace(ErrNotFile)
	}
}

func (c *Client) List(path string) ([]string, error) {
	return c.list(path, false)
}

func (c *Client) list(path string, must bool) ([]string, error) {
	c.Lock()
	defer c.Unlock()
	if c.closed {
		return nil, errors.Trace(ErrClosedClient)
	}
	cntx, cancel := c.newContext()
	defer cancel()
	r, err := c.kapi.Get(cntx, path, &client.GetOptions{Quorum: true})
	switch {
	case err != nil:
		if isErrNoNode(err) && !must {
			return nil, nil
		}
		log.Debugf("etcd list node %s failed: %s", path, err)
		return nil, errors.Trace(err)
	case !r.Node.Dir:
		log.Debugf("etcd list node %s failed: not a dir", path)
		return nil, errors.Trace(ErrNotDir)
	default:
		var paths []string
		for _, node := range r.Node.Nodes {
			paths = append(paths, node.Key)
		}
		return paths, nil
	}
}

func (c *Client) CreateEphemeral(path string, data []byte) (<-chan struct{}, error) {
	return c.CreateEphemeralWithTimeout(path, data, c.timeout)
}

func (c *Client) CreateEphemeralWithTimeout(path string, data []byte, timeout time.Duration) (<-chan struct{}, error) {
	c.Lock()
	defer c.Unlock()
	if c.closed {
		return nil, errors.Trace(ErrClosedClient)
	}
	cntx, cancel := c.newContext()
	defer cancel()
	log.Debugf("etcd create-ephemeral node %s", path)
	if timeout < 25*time.Second {
		timeout = 25 * time.Second
	}
	_, err := c.kapi.Set(cntx, path, string(data), &client.SetOptions{PrevExist: client.PrevNoExist, TTL: timeout})
	if err != nil {
		log.Debugf("etcd create-ephemeral node %s failed: %s", path, err)
		return nil, errors.Trace(err)
	}
	log.Debugf("etcd create-ephemeral OK")
	return runRefreshEphemeral(c, path, timeout), nil
}

func (c *Client) CreateEphemeralInOrder(path string, data []byte) (<-chan struct{}, string, error) {
	c.Lock()
	defer c.Unlock()
	if c.closed {
		return nil, "", errors.Trace(ErrClosedClient)
	}
	cntx, cancel := c.newContext()
	defer cancel()
	log.Debugf("etcd create-ephemeral-inorder node %s", path)
	r, err := c.kapi.CreateInOrder(cntx, path, string(data), &client.CreateInOrderOptions{TTL: c.timeout})
	if err != nil {
		log.Debugf("etcd create-ephemeral-inorder node %s failed: %s", path, err)
		return nil, "", errors.Trace(err)
	}
	node := r.Node.Key
	log.Debugf("etcd create-ephemeral-inorder OK, node = %s", node)
	return runRefreshEphemeral(c, node, c.timeout), node, nil
}

func runRefreshEphemeral(c *Client, path string, timeout time.Duration) <-chan struct{} {
	signal := make(chan struct{})
	go func() {
		defer close(signal)
		for {
			ctx, cancel := context.WithTimeout(c.context, timeout*2/5)
			if err := c.RefreshEphemeralWithRetry(ctx, path, timeout); err != nil {
				cancel()
				return
			} else {
				cancel()
				time.Sleep(timeout * 2 / 5)
			}
		}
	}()
	return signal
}

func (c *Client) RefreshEphemeralWithRetry(ctx context.Context, path string, timeout time.Duration) error {
	for {
		if err := c.RefreshEphemeral(path, timeout); err != nil {
			select {
			case <-ctx.Done():
				return err
			case <-time.After(100 * time.Millisecond):
			}
		} else {
			return nil
		}
	}
}

func (c *Client) RefreshEphemeral(path string, timeout time.Duration) error {
	c.Lock()
	defer c.Unlock()
	if c.closed {
		return errors.Trace(ErrClosedClient)
	}
	cntx, cancel := c.newContext()
	defer cancel()
	log.Debugf("etcd refresh-ephemeral node %s", path)
	_, err := c.kapi.Set(cntx, path, "", &client.SetOptions{PrevExist: client.PrevExist, Refresh: true, TTL: timeout})
	if err != nil {
		log.Debugf("etcd refresh-ephemeral node %s failed: %s", path, err)
		return errors.Trace(err)
	}
	log.Debugf("etcd refresh-ephemeral OK")
	return nil
}

func (c *Client) WatchInOrder(path string) (<-chan struct{}, []string, error) {
	if err := c.Mkdir(path); err != nil {
		return nil, nil, err
	}
	c.Lock()
	defer c.Unlock()
	if c.closed {
		return nil, nil, errors.Trace(ErrClosedClient)
	}
	log.Debugf("etcd watch-inorder node %s", path)
	cntx, cancel := c.newContext()
	defer cancel()
	r, err := c.kapi.Get(cntx, path, &client.GetOptions{Quorum: true, Sort: true})
	switch {
	case err != nil:
		log.Debugf("etcd watch-inorder node %s failed: %s", path, err)
		return nil, nil, errors.Trace(err)
	case !r.Node.Dir:
		log.Debugf("etcd watch-inorder node %s failed: not a dir", path)
		return nil, nil, errors.Trace(ErrNotDir)
	}
	var index = r.Index
	var paths []string
	for _, node := range r.Node.Nodes {
		paths = append(paths, node.Key)
	}
	signal := make(chan struct{})
	go func() {
		defer close(signal)
		watch := c.kapi.Watcher(path, &client.WatcherOptions{AfterIndex: index})
		for {
			r, err := watch.Next(c.context)
			switch {
			case err != nil:
				log.Debugf("etch watch-inorder node %s failed: %s", path, err)
				return
			case r.Action != "get":
				log.Debugf("etcd watch-inorder node %s update", path)
				return
			}
		}
	}()
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
	grantTime := time.Now()
	resp, err := c.kapi.Set(ctx, path, string(data), &client.SetOptions{
		PrevExist: client.PrevNoExist,
		TTL:       time.Duration(ttl) * time.Second,
	})
	if err != nil {
		log.Debugf("[CAS] cas node %s failed: %s", path, err)
		r, getErr := c.kapi.Get(ctx, path, &client.GetOptions{Quorum: true})
		switch {
		case getErr != nil:
			log.Debugf("[CAS] etcd read node %s failed: %s", path, getErr)
			return nil, -1, nil, errors.Trace(err)
		case !r.Node.Dir:
			return []byte(r.Node.Value), int64(r.Node.ModifiedIndex), nil, common.ErrKeyAlreadyExists
		default:
			log.Debugf("[CAS] etcd read node %s failed: not a file", path)
			return nil, -1, nil, errors.Trace(err)
		}
	}
	log.Debugf("etcd tryLock OK")
	if resp.Node.ModifiedIndex > math.MaxInt64 {
		return nil, -1, nil, errors.Errorf("int64 overflow")
	}
	return data, int64(resp.Node.ModifiedIndex), common.NewLease(
		parent, c, path, ttl, deadlineLoopInterval, grantTime,
		onError, beforeKeepAliveOnceFunc), nil
}

func (c *Client) WatchOnce(ctx context.Context, path string, rev int64) error {
	c.Lock()
	if c.closed {
		c.Unlock()
		return errors.Trace(ErrClosedClient)
	}
	log.Debugf("etcd watch-once node %s", path)

	watcher := c.kapi.Watcher(path, &client.WatcherOptions{AfterIndex: uint64(rev)})
	c.Unlock()

	// TODO seems this rev cmp is not safe, need more tests.
	for {
		r, err := watcher.Next(ctx)
		if err != nil {
			log.Debugf("etch watch-once node %s failed: %s", path, err)
			return err
		}
		if r.Action != "get" {
			log.Debugf("etcd watch-once node %s recv events '%s'", path, r.Action)
			return nil
		}
	}
}

func (c *Client) KeepAliveOnce(ctx context.Context, lease *common.Lease, ttl int64) (newTTL int64, kontinue bool, err error) {
	if c.IsClosed() {
		return 0, false, errors.Trace(ErrClosedClient)
	}

	if _, err := c.kapi.Set(ctx, lease.ID.(string), "", &client.SetOptions{
		PrevExist: client.PrevExist,
		Refresh:   true,
		TTL:       time.Duration(ttl) * time.Second},
	); err != nil {
		return -1, true, err
	}
	return ttl, true, nil
}

func (c *Client) Revoke(context.Context, *common.Lease) error {
	return nil
}
