package gocache

import (
	"context"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/neijuanxiaozi/gocache/consistenthash"
	pb "github.com/neijuanxiaozi/gocache/gocachepb"
	"github.com/neijuanxiaozi/gocache/utils"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
)

const (
	defaultAddr     = "127.0.0.1:6324" // 当前节点默认地址
	defaultReplicas = 50               // hash环中真实节点对应虚拟节点个数
)

// etcd 默认配置对象 包含了etcd的ip和port etcd连接超时时间为5秒
var (
	defaultEtcdConfig = clientv3.Config{
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: 5 * time.Second,
	}
)

// gocache的网络模块中的服务端模块 负责等待其他节点的rpc请求 或者客户端的请求 server与cache是解耦的
type server struct {
	addr                              string                         // 当前节点的ip和port
	status                            bool                           // 当前节点是否运行
	stopSignal                        chan error                     // 通知registry revoke服务 作用未知
	mu                                sync.Mutex                     // 操作一致性哈希时 加锁
	consHash                          *consistenthash.Consistentency // 一致性hash
	clients                           map[string]*client             // 其他节点
	*pb.UnimplementedGroupCacheServer                                // 实现grpc需要
}

// 创建一个server实例
func NewServer(addr string) (*server, error) {
	if addr == "" {
		addr = defaultAddr
	}
	if !utils.ValidPeerAddr(addr) {
		return nil, fmt.Errorf("invalid addr %s, it should be x.x.x.x:port", addr)
	}
	return &server{addr: addr}, nil
}

// 将其他节点设置到hash环上
func (s *server) SetPeers(peersAddr ...string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// 创建一致性hash实例
	s.consHash = consistenthash.New(defaultReplicas, nil)
	// 将其他节点设置到hash环上
	s.consHash.Register(peersAddr...)
	// 创建客户端map 记录访问其他节点的客户端实例
	s.clients = make(map[string]*client)
	// 为访问其他节点 创建每个节点对应的客户端 用对应节点的客户端实例访问其他节点
	for _, peerAddr := range peersAddr {
		if !utils.ValidPeerAddr(peerAddr) {
			panic(fmt.Sprintf("[peer %s] invalid address format, it should be x.x.x.x:port", peerAddr))
		}
		s.clients[peerAddr] = NewClient(fmt.Sprintf("gocache/%s", peerAddr))
	}
}

// 用key在hash环上找到对应节点 并返回对应节点的客户端
func (s *server) Pick(key string) (Fetcher, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// 获取要访问节点的ip和端口
	peerAddr := s.consHash.GetPeer(key)
	// 要访问的节点是自己
	if peerAddr == s.addr {
		log.Printf("pick self, %s", s.addr)
		return nil, false
	}
	log.Printf("[cache %s] pick remote peer: %s", s.addr, peerAddr)
	// 返回对应节点的客户端实例
	return s.clients[peerAddr], true
}

// 断言server是否是Picker接口
var _ Picker = (*server)(nil)

// 启动start 将服务注册到etcd上 并监听客户端请求(注意是客户端的请求 不是其他节点的请求 其他节点的访问是通过grpc)
func (s *server) Start() error {
	s.mu.Lock()
	//已经启动过
	if s.status == true {
		s.mu.Unlock()
		return fmt.Errorf("server already started")
	}
	// -----------------启动服务----------------------
	// 1. 设置status为true 表示服务器已在运行
	// 2. 初始化stop channal,这用于通知registry stop keep alive
	// 3. 初始化tcp socket并开始监听
	// 4. 注册rpc服务至grpc 这样grpc收到request可以分发给server处理
	// 5. 将自己的服务名/Host地址注册至etcd 这样client可以通过etcd
	//    获取服务Host地址 从而进行通信。这样的好处是client只需知道服务名
	//    以及etcd的Host即可获取对应服务IP 无需写死至client代码中
	// ----------------------------------------------

	//设置当前节点运行状态为true
	s.status = true
	// 创建stop channal 作用未知
	s.stopSignal = make(chan error)
	port := strings.Split(s.addr, ":")[1]
	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		return fmt.Errorf("faild to listen: %v", err)
	}
	grpcServer := grpc.NewServer()
	pb.RegisterGroupCacheServer(grpcServer, s)

	// 注册服务至etcd
	go func() {
		//TODO
		err := registry.Register("gocache", s.addr, s.stopSignal)
		if err != nil {
			log.Fatalf(err.Error())
		}
		// 关闭管道
		close(s.stopSignal)
		// 关闭tcp listen  这里为什么要关闭
		err = lis.Close()
		if err != nil {
			log.Fatalf(err.Error())
		}
		log.Printf("[%s] Revoke service and close tcp socket ok.", s.addr)
	}()
	s.mu.Unlock()
	// 启动rpc服务
	if err := grpcServer.Serve(lis); err != nil && s.status {
		return fmt.Errorf("failed to server: %v", err)
	}
	return nil
}

// rpc方法
func (s *server) Get(ctx context.Context, in *pb.GetRequest) (*pb.GetResponse, error) {
	// 从请求参数中获取 group name和key
	group, key := in.GetGroup(), in.GetKey()
	// 创建rpc方法响应实例
	resp := &pb.GetResponse{}

	log.Printf("[gocache_svr %s] Recv RPC - (%s)/(%s)", s.addr, group, key)
	if key == "" {
		return resp, fmt.Errorf("empty key")
	}
	// 根据group name 拿到group
	g := GetGroup(group)
	if g == nil {
		return resp, fmt.Errorf("group is not found")
	}
	// 在group中根据key获得数据ByteView
	view, err := g.Get(key)
	if err != nil {
		return resp, err
	}
	// 赋值给resp
	resp.Value = view.ByteSlice()
	return resp, err
}

// Stop停止server
func (s *server) Stop() {
	s.mu.Lock()
	if !s.status {
		s.mu.Unlock()
		return
	}
	s.stopSignal <- nil // 发送停止keepalive信号
	s.status = false    // 设置server运行状态为stop
	s.clients = nil     // 清空客户端信息 有助于垃圾回收
	s.consHash = nil    // 清空一致性哈希信息 有助于垃圾回收
	s.mu.Unlock()
}
