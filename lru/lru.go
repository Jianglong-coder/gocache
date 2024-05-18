package lru

import "container/list"

// 接口类型 实现了获取长度的函数
type Lengthable interface {
	Len() int
}

// 双向链表节点的数据类型
type Value struct {
	key   string
	value Lengthable //接口类型 实现了获取长度的函数
}
type OnEliminated func(key string, Value Lengthable)

// lru
type Cache struct {
	capacity         int64                    // 最大内存
	length           int64                    // 当前已使用内存
	doublyLinkedList *list.List               // 双链表
	hashmap          map[string]*list.Element // map key是string 值是链表中值对应的指针
	callback         OnEliminated             // 当一个entry被清除时执行的函数
}

// 实例化cache
func New(maxBytes int64, callback OnEliminated) *Cache {
	return &Cache{
		capacity:         maxBytes,
		doublyLinkedList: list.New(),
		hashmap:          make(map[string]*list.Element),
		callback:         callback,
	}
}

// 获取节点value 并移动到链表头部
func (c *Cache) Get(key string) (value Lengthable, ok bool) {
	// 如果缓存中存在key
	if elem, ok := c.hashmap[key]; ok {
		// 将元素移到链表头
		c.doublyLinkedList.MoveToFront(elem)
		// 将元素断言成Value类型
		entry := elem.Value.(*Value)
		// 返回元素中实际包含的值
		return entry.value, true
	}
	return
}

//	向缓存中添加值
func (c *Cache) Add(key string, value Lengthable) {
	// 算出新添加的kv的大小
	kvSize := int64(len(key)) + int64(value.Len())
	// 当lru容量不够时 持续从链表尾pop元素 直到能够放下新元素
	for c.capacity != 0 && kvSize+c.length > c.capacity {
		c.Remove()
	}
	// 如果该元素已经存在
	if elem, ok := c.hashmap[key]; ok {
		// 重新放到链表头
		c.doublyLinkedList.MoveToFront(elem)
		// 拿到旧元素  断言成指针 方便后续修改
		oldEntry := elem.Value.(*Value)
		// 更改缓存当前大小
		c.length += int64(oldEntry.value.Len()) - int64(value.Len())
		// 更改key对应的元素值
		oldEntry.value = value
	} else {
		// 用key和value生成新的元素插入链表头
		elem := c.doublyLinkedList.PushFront(&Value{key: key, value: value})
		// 更新缓存的哈希表
		c.hashmap[key] = elem
		// 更新缓存大小
		c.length += kvSize
	}
}

// RemoveOldest removes the oldest item
func (c *Cache) Remove() {
	tailElem := c.doublyLinkedList.Back()
	if tailElem != nil {
		entry := tailElem.Value.(*Value)
		k, v := entry.key, entry.value
		delete(c.hashmap, k)
		c.doublyLinkedList.Remove(tailElem)
		c.length -= int64(len(k)) + int64(v.Len())
		if c.callback != nil {
			c.callback(k, v)
		}
	}
}
