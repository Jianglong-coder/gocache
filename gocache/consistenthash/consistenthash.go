package consistenthash

import "hash/crc32"

// 定义了函数类型 Hash，采取依赖注入的方式，允许用于替换成自定义的 Hash 函数，默认为 crc32.ChecksumIEEE 算法。
type Hash func(data []byte) uint32

// Map 是一致性哈希算法的主数据结构
type Map struct {
	hash     Hash           // Hash函数
	replicas int            // 虚拟节点倍数
	keys     []int          // 哈希环
	hashMap  map[int]string // 虚拟节点与真实节点的映射表 键是虚拟节点的哈希值 值是真实节点的名称
}

func New(replicas int, fn Hash) *Map {
	m := &Map{
		hash : fn,
		replicas: replicas,
		hashMap : map[int]string
	}
	if m.hash == nil {
		m.hash = crc32.ChecksumIEEE //  ChecksumIEEE需要更详细的资料
	}
	return m
}

// 添加节点方法Add()
// Add 函数允许传入 0 或 多个真实节点的名称
func (m *Map) Add(keys ...string) {
	// 对每一个真实节点 key，对应创建 m.replicas 个虚拟节点，
	// 虚拟节点的名称是：strconv.Itoa(i) + key，即通过添加编号的方式区分不同虚拟节点.
	for _, key := range keys {
		for i := 0; i < m.replicas; i++ {
			// 使用 m.hash() 计算虚拟节点的哈希值，
			hash := int(m.hash([]byte(strconv.Itoa(i) + key)))
			// 使用 append(m.keys, hash) 添加到环上
			m.keys = append(m.keys, hash)
			// 在 hashMap 中增加虚拟节点和真实节点的映射关系
			m.hashMap[hash] = key
		}
	}
	// 环上的哈希值排序
	sort.Ints(m.keys)
}

func (m *Map) Get(key string) string {
	// hash环上节点的数量为0
	if len(m.keys) == 0 {
		return ""
	}
	// 算出待查找key的hash值
	hash := int(m.hash([]byte(key)))
	// 在hash环上顺时针查找虚拟节点在keys中的下标
	idx := sort.Search(len(m.keys), func(i int) bool {
		return m.keys >= hash
	})
	// 返回虚拟节点对应的真实节点 如果idx==len(m.keys) 说明应该选择m.keys[0] 因为keys是个环状结构
	return m.hashMap[m.keys[idx%len(m.keys)]]
}