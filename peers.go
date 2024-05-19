package gocache

/*
	该文件实现流程(2) 从远程节点获取缓存值
	(2)流程:
	使用一致性哈希选择节点       是                                     是
		|-----> 是否是远程节点 -----> HTTP 客户端访问远程节点 --> 成功？-----> 服务端返回返回值
						|  否                                    ↓  否
						|----------------------------> 回退到本地节点处理。
*/

// Picker 的 Pick() 方法用于根据传入的 key 选择相应的分布式节点
type Picker interface {
	Pick(key string) (peer Fetcher, ok bool)
}

// 接口 Fetcher 的 Fetch() 方法用于从其他节点查找缓存值。
type Fetcher interface {
	Fetch(group string, key string) ([]byte, error)
}
