package gocache

import (
	"fmt"
	"log"
	"sync"

	"github.com/neijuanxiaozi/gocache/consistenthash"
	"github.com/neijuanxiaozi/gocache/utils"
)

const (
	defaultAddr     = "127.0.0.1:6324"
	defaultReplicas = 50
)

// gocache的网络模块中的服务端模块 负责等待其他节点的rpc请求 或者客户端的请求 server与cache是解耦的
type server struct {
	addr       string                         // 当前节点的ip和port
	status     bool                           // 当前节点是否运行
	stopSignal chan error                     // 通知registry revoke服务 作用未知
	mu         sync.Mutex                     // 操作一致性哈希时 加锁
	consHash   *consistenthash.Consistentency // 一致性hash
	clients    map[string]*client             // 其他节点
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
	return nil

}
func (s *server) Stop() {

}
