// Group 是 GoCache 最核心的数据结构，负责与用户的交互，并且控制缓存值存储和获取的流程。
/*
                            是
接收 key --> 检查是否被缓存 -----> 返回缓存值 ⑴
                |  否                         是
                |-----> 是否应当从远程节点获取 -----> 与远程节点交互 --> 返回缓存值 ⑵
                            |  否
                            |-----> 调用`回调函数`，获取值并添加到缓存 --> 返回缓存值 ⑶

*/
package gocache

import (
	"fmt"
	"log"
	"sync"

	"github.com/neijuanxiaozi/gocache/singleflight"
)

var (
	mu     sync.RWMutex              // 全局变量 groups 的读写锁
	groups = make(map[string]*Group) // 全局变量 groups
)

// 定义接口 参数是string 返回值是[]byte
// 当从缓存中获取数据失败时的回调函数 用于从源获取数据
type Retriever interface {
	retrieve(key string) ([]byte, error)
}

// 定义函数类型 RetrieverFunc，并实现 Retriever 接口的 retrieve 方法。
type RetrieverFunc func(key string) ([]byte, error)

// 函数类型实现某一个接口，称之为接口型函数，方便使用者在调用时既能够传入函数作为参数，
// 也能够传入实现了该接口的结构体作为参数。
// 定义一个函数类型 F，并且实现接口 A 的方法，然后在这个方法中调用自己。
// 这是 Go 语言中将其他函数（参数返回值定义与 F 一致）转换为接口 A 的常用技巧。
func (f RetrieverFunc) retrieve(key string) ([]byte, error) {
	return f(key)
}

// 一个 Group 可以认为是一个缓存的命名空间，每个 Group 拥有一个唯一的名称 name。
// 比如可以创建三个 Group，缓存学生的成绩命名为 scores，缓存学生信息的命名为 info，缓存学生课程的命名为 courses。
type Group struct {
	name      string               // 每个 Group 拥有一个唯一的名称 name
	cache     *cache               // 即一开始实现的并发缓存
	retriever Retriever            // 即缓存未命中时获取源数据的回调(callback)
	server    Picker               // 将实现了 PeerPicker 接口的 HTTPPool(网络模块) 注入到 Group 中
	flight    *singleflight.Flight // 请求锁 保证同一个key的请求在同一时间只有一个 减少请求数量
}

// 构建函数 NewGroup 用来实例化 Group，并且将 group 存储在全局变量 groups 中
func NewGroup(name string, maxBytes int64, retriever Retriever) *Group {
	if retriever == nil {
		panic("Retriver is nil.")
	}
	g := &Group{
		name:      name,
		cache:     newCache(maxBytes),
		retriever: retriever,
		flight:    &singleflight.Flight{},
	}
	mu.Lock()
	groups[name] = g
	mu.Unlock()
	return g
}

// 获取名字对应的group
func GetGroup(name string) *Group {
	mu.RLock()
	g := groups[name]
	mu.RUnlock()
	return g
}

func DestoryGroup(name string) {
	g := GetGroup(name)
	if g != nil {
		server := g.server.(*server)
		server.Stop()
		delete(groups, name)
		log.Printf("Destory cache [%s %s]", name, server.addr)
	}

}

// Get 方法实现了上述所说的流程 ⑴ 和 ⑶。
// 流程 ⑴ ：从 cache 中查找缓存，如果存在则返回缓存值。
// 流程 ⑶ ：缓存不存在，则调用 load 方法，load 调用 getLocally（分布式场景下会调用 getFromPeer 从其他节点获取），
// getLocally 调用用户回调函数 g.getter.Get() 获取源数据，并且将源数据添加到缓存 mainCache 中（通过 populateCache 方法）
func (g *Group) Get(key string) (ByteView, error) {
	// key为空
	if key == "" {
		return ByteView{}, fmt.Errorf("key is required")
	}
	//缓存命中
	if v, ok := g.cache.get(key); ok {
		log.Println("[GoCache] hit")
		return v, nil
	}
	// 缓存未命中 去获取源数据
	return g.load(key)
}

// 缓存未命中时 用load获取源数据
func (g *Group) load(key string) (value ByteView, err error) {
	// 用loader.Fly去获取数据 保证同时时刻同一个key的请求只有一个
	view, err := g.flight.Fly(key, func() (interface{}, error) {
		// 从其他节点缓存获取数据
		if g.server != nil {
			if fetcher, ok := g.server.Pick(key); ok {
				bytes, err := fetcher.Fetch(g.name, key)
				if err == nil {
					return ByteView{b: bytes}, nil
				}
			}
			log.Println("[Gocache] Failed to get from peer", err)
		}
		// 否则从本地源获取数据
		return g.getLocally(key)
	})
	if err == nil {
		return view.(ByteView), nil
	}
	return
}

// 从本地获取源数据
func (g *Group) getLocally(key string) (ByteView, error) {
	// 获取源数据
	bytes, err := g.retriever.retrieve(key)
	// 获取源数据失败
	if err != nil {
		return ByteView{}, err
	}
	// 防止修改 拷贝一份 并返回
	value := ByteView{b: cloneBytes(bytes)}
	// 放入缓存中
	g.populateCache(key, value)
	return value, nil
}

// 将从源数据获取的数据 放入缓存中
func (g *Group) populateCache(key string, value ByteView) {
	g.cache.add(key, value)
}

// 将实现了 Picker 接口的 Server(实现了网络模块的服务端) 注入到 Group 中
func (g *Group) RegisterSvr(p Picker) {
	if g.server != nil {
		panic("group has been registered server")
	}
	g.server = p
}
