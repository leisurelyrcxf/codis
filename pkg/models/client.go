// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package models

import (
	"context"
	"time"

	"github.com/CodisLabs/codis/pkg/models/common"

	etcdclient "github.com/CodisLabs/codis/pkg/models/etcd"
	etcdclientv3 "github.com/CodisLabs/codis/pkg/models/etcdv3"
	fsclient "github.com/CodisLabs/codis/pkg/models/fs"
	zkclient "github.com/CodisLabs/codis/pkg/models/zk"
	"github.com/CodisLabs/codis/pkg/utils/errors"
)

type Client interface {
	common.LockClient

	Create(path string, data []byte) error
	Update(path string, data []byte) error
	Delete(path string) error
	Rmdir(dir string) error

	Read(path string, must bool) ([]byte, error)
	List(path string) ([]string, error)

	Close() error

	WatchInOrder(path string) (<-chan struct{}, []string, error)

	CreateEphemeralWithTimeout(path string, data []byte, timeout time.Duration) (<-chan struct{}, error)
	CreateEphemeral(path string, data []byte) (<-chan struct{}, error)
	CreateEphemeralInOrder(path string, data []byte) (<-chan struct{}, string, error)
}

func NewClient(coordinator string, addrlist string, auth string, timeout time.Duration) (Client, error) {
	switch coordinator {
	case "zk", "zookeeper":
		return zkclient.New(addrlist, auth, timeout)
	case "etcd":
		return etcdclient.New(addrlist, auth, timeout)
	case "etcdv3":
		return etcdclientv3.New(addrlist, auth, timeout)
	case "fs", "filesystem":
		return fsclient.New(addrlist)
	}
	return nil, errors.Errorf("invalid coordinator name = %s", coordinator)
}

func WithLocked(
	ctx context.Context,
	client common.LockClient, path string, data []byte,
	cfg common.LockConfig, userCallbacks common.UserCallbacks,
	onLocked func(lockCtx context.Context) error) (err error) {
	l := common.NewDistributedLockImpl(client, path, data, cfg, userCallbacks)
	lockCtx, err := l.Lock(ctx)
	if err != nil {
		return err
	}
	defer l.Unlock()

	return onLocked(lockCtx)
}
