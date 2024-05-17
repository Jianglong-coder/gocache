package gocache

import pb "example/gocache/gocachepb"

/*
	该文件实现流程(2) 从远程节点获取缓存值
	(2)流程:
	使用一致性哈希选择节点       是                                     是
		|-----> 是否是远程节点 -----> HTTP 客户端访问远程节点 --> 成功？-----> 服务端返回返回值
						|  否                                    ↓  否
						|----------------------------> 回退到本地节点处理。
*/

// PeerPicker 的 PickPeer() 方法用于根据传入的 key 选择相应节点 PeerGetter
type PeerPicker interface {
	PickPeer(key string) (peer PeerGetter, ok bool)
}

// 接口 PeerGetter 的 Get() 方法用于从对应 group 查找缓存值。PeerGetter 就对应于上述流程中的 HTTP 客户端
type PeerGetter interface {
	Get(in *pb.Request, out *pb.Response) error
}
