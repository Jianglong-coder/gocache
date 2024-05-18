package consistenthash

import (
	"hash/crc32"
	"sort"
	"strconv"
)

// 定义了函数类型 Hash，采取依赖注入的方式，允许用于替换成自定义的 Hash 函数，默认为 crc32.ChecksumIEEE 算法。
type HashFunc func(data []byte) uint32

// Map 是一致性哈希算法的主数据结构
type Consistentency struct {
	hash     HashFunc       // Hash函数
	replicas int            // 虚拟节点个数
	ring     []int          // 哈希环
	hashMap  map[int]string // 虚拟节点与真实节点的映射表 键是虚拟节点的哈希值 值是真实节点的名称
}

func New(replicas int, fn HashFunc) *Consistentency {
	c := &Consistentency{
		hash:     fn,
		replicas: replicas,
		hashMap:  make(map[int]string),
	}
	if c.hash == nil {
		c.hash = crc32.ChecksumIEEE //  ChecksumIEEE需要更详细的资料
	}
	return c
}

// 添加节点方法Add()
// Add 函数允许传入 0 或 多个真实节点的名称
func (c *Consistentency) Register(peersName ...string) {
	// 对每一个真实节点 key，对应创建 m.replicas 个虚拟节点，
	// 虚拟节点的名称是：strconv.Itoa(i) + key，即通过添加编号的方式区分不同虚拟节点.
	for _, peerName := range peersName {
		for i := 0; i < c.replicas; i++ {
			// 使用 m.hash() 计算虚拟节点的哈希值，
			hashValue := int(c.hash([]byte(strconv.Itoa(i) + peerName)))
			// 使用 append(m.keys, hash) 添加到环上
			c.ring = append(c.ring, hashValue)
			// 在 hashMap 中增加虚拟节点和真实节点的映射关系
			c.hashMap[hashValue] = peerName
		}
	}
	// 环上的哈希值排序
	sort.Ints(c.ring)
}

func (c *Consistentency) Get(key string) string {
	// hash环上节点的数量为0
	if len(c.ring) == 0 {
		return ""
	}
	// 算出待查找key的hash值
	hashValue := int(c.hash([]byte(key)))
	// 在hash环上顺时针查找虚拟节点在keys中的下标
	idx := sort.Search(len(c.ring), func(i int) bool {
		return c.ring[i] >= hashValue
	})
	// 返回虚拟节点对应的真实节点 如果idx==len(m.keys) 说明应该选择m.keys[0] 因为keys是个环状结构
	return c.hashMap[c.ring[idx%len(c.ring)]]
}
