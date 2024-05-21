package gocache

import (
	"context"
	"fmt"
	"time"

	pb "github.com/neijuanxiaozi/gocache/gocachepb"
	"github.com/neijuanxiaozi/gocache/registry"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type client struct {
	name string // 要访问远端节点的路径   gocache/ip:port
}

func NewClient(peerAddr string) *client {
	return &client{name: peerAddr}
}

func (c *client) Fetch(group string, key string) ([]byte, error) {
	// 用etcd配置对象 创建一个etcd client
	cli, err := clientv3.New(defaultEtcdConfig)
	if err != nil {
		return nil, err
	}
	defer cli.Close()
	// 发现服务 获得与服务的连接
	conn, err := registry.EtcdDial(cli, c.name)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	// 创建grpc客户端对象
	grpcClient := pb.NewGoCacheClient(conn)
	// 创建一个带有超时时间的上下文 cancel是一个函数，调用它将会取消与该上下文关联的所有操作
	// 在实际应用中，通常会将 ctx 传递给需要执行的操作或函数，以便它们能够感知到超时信号，并在必要时停止执行。
	// 如果超时发生或者调用了 cancel 函数，
	// 任何接收了 ctx 的函数都应该能够检测到这个信号，并据此作出响应，比如停止阻塞操作、返回错误等。
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	// rpc调用
	resp, err := grpcClient.Get(ctx, &pb.GetRequest{Group: group, Key: key})
	if err != nil {
		return nil, fmt.Errorf("could not get %s/%s from peer %s", group, key, c.name)
	}
	return resp.Value, nil
}

var _ Fetcher = (*client)(nil)
