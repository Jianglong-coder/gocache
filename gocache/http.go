package gocache

import (
	"example/gocache/consistenthash"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync"
)

const (
	defaultBasePath = "/_gocache/"
	defaultReplicas = 50
)

type HTTPPool struct {
	self     string // self 用来记录自己的地址 包括主机名/IP和端口
	basePath string // basePath 作为节点间通信的前缀 默认是/_gocache/
	//下面为HTTPPool添加节点选择功能
	mu          sync.Mutex
	peers       *consistenthash.Map    // 一致性哈希算法的map结构 用来根据key选择节点
	httpGetters map[string]*httpGetter // 新增成员变量 httpGetters，映射远程节点与对应的 httpGetter。每一个远程节点对应一个 httpGetter，因为 httpGetter 与远程节点的地址 baseURL 有关
}

func NewHTTPPool(self string) *HTTPPool {
	return &HTTPPool{
		self:     self,
		basePath: defaultBasePath,
	}
}

// log info with server name
func (p *HTTPPool) Log(format string, v ...interface{}) {
	log.Printf("[Server %s] %s", p.self, fmt.Sprintf(format, v...))
}

// 服务端的路由处理函数 处理客户端发起的请求(获取缓存)
func (p *HTTPPool) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// 检查路由中是否包含gocache的基本路径
	if !strings.HasPrefix(r.URL.Path, p.basePath) {
		panic("HTTPPool serving unexpected path: " + r.URL.Path)
	}
	p.Log("%s %s", r.Method, r.URL.Path)
	// /<basepath>/<groupname>/<key> required
	// 从去掉基本路径的后面分割出groupname和key
	parts := strings.SplitN(r.URL.Path[len(p.basePath):], "/", 2)
	if len(parts) != 2 {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	groupName := parts[0]
	key := parts[1]
	// 根据groupName获得group
	group := GetGroup(groupName)
	if group == nil {
		http.Error(w, "no such group: "+groupName, http.StatusNotFound)
		return
	}
	// 根据key 然后在group中获取key
	view, err := group.Get(key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	// 设置响应头 Content-Type代表响应体中的数据类型
	// application/octet-stream通常用来表示响应体是一些二进制流数据，而非文本。
	// 当响应的内容不应由浏览器直接显示，或者当内容的类型未知或不具体时，常用这个类型。
	// 例如，它经常用于文件下载的场景，告诉浏览器或其他客户端应该将响应的数据视为文件来处理。
	w.Header().Set("Content-Type", "application/octet-stream")
	//将数据写回
	w.Write(view.ByteSlice())
}

// HTTPPool客户端功能

// 具体的HTTP客户端类httpGetter 实现PeerGetter接口
type httpGetter struct {
	baseURL string // 表示将要访问的远程节点的地址，例如http://example.com/_geecache/
}

func (h *httpGetter) Get(group, key string) ([]byte, error) {
	// 拼接要访问节点的路由  baseURL + group + key
	u := fmt.Sprintf(
		"%v%v/%v",
		h.baseURL,
		url.QueryEscape(group), //QueryEscape 函数用来转义URL查询字符串中的特殊字符
		url.QueryEscape(key),
	)
	// 进行http请求
	res, err := http.Get(u)
	if err != nil {
		return nil, err
	}
	// 延迟关闭http响应
	defer res.Body.Close()
	// http请求失败
	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("server returned: %v", res.Status)
	}

	bytes, err := io.ReadAll(res.Body)
	// 读响应体失败
	if err != nil {
		return nil, fmt.Errorf("reading response body: %v", err)
	}
	return bytes, nil
}

// var _ PeerGetter = (*httpGetter)(nil)这行代码是一个编译时检查，
// 用来确认httpGetter类型的指针是否实现了PeerGetter接口。
// 这是一种确保类型安全和符合预期行为的技术手段
var _ PeerGetter = (*httpGetter)(nil)

// 实例化一致性哈希算法 并且添加了传入的节点 并为所有节点创建了要给HTTP客户端httpGetter
func (p *HTTPPool) Set(peers ...string) {
	// 操作hash环和每个节点对应的httpgetter结构需要上锁
	p.mu.Lock()
	defer p.mu.Unlock()
	p.peers = consistenthash.New(defaultReplicas, nil)
	p.peers.Add(peers...)
	p.httpGetters = make(map[string]*httpGetter, len(peers))
	for _, peer := range peers {
		p.httpGetters[peer] = &httpGetter{baseURL: peer + p.basePath}
	}
}

// 为HTTPool实现PeerPicker接口
// 包装了一致性哈希算法的Get() 根据key寻找对应的节点 返回节点对应的HTTP客户端
func (p *HTTPPool) PickPeer(key string) (PeerGetter, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if peer := p.peers.Get(key); peer != "" && peer != p.self {
		p.Log("Pick peer %v", peer)
		return p.httpGetters[peer], true
	}
	return nil, false
}

// 将HTTPool断言为PeerPicker类型
var _ PeerPicker = (*HTTPPool)(nil)
