package singleflight

import "sync"

// call代表正在进行中 或已经结束的请求 使用sync.WaitGroup锁避免重入
type packet struct {
	wg  sync.WaitGroup
	val interface{}
	err error
}

// Group是singleflight的主数据结构 管理不同key的请求(call)
type Flight struct {
	mu     sync.Mutex         // 保护Group成员变量m并发读写而加上的锁
	flight map[string]*packet // 一个key对应一个请求 发起请求时就插入到map中 后续相同的请求到来时 发现map中有就等待map中请求返回 避免重复发送
}

// 对Group 实现do方法 不论Do被调用多少次 传入的fn都只会被调用一次 等待fn调用结束了 返回返回值或者错误
func (f *Flight) Fly(key string, fn func() (interface{}, error)) (interface{}, error) {
	f.mu.Lock()
	//	Group里的map结构延迟初始化
	if f.flight == nil {
		f.flight = make(map[string]*packet)
	}
	// 如果对应key的请求已经存在还未返回
	if p, ok := f.flight[key]; ok {
		f.mu.Unlock()
		// 阻塞知道锁被释放  可以等待已经存在请求的结果 不必重复请求
		p.wg.Wait()
		return p.val, p.err
	}
	//没有key对应的请求

	//新建一个请求
	p := new(packet)
	//锁加1
	p.wg.Add(1)
	//将对应key的请求插入到map中
	f.flight[key] = p
	f.mu.Unlock()
	//发起请求
	p.val, p.err = fn()
	//请求结束 唤醒其他所有等待这个请求的协程
	p.wg.Done()

	//请求已经结束 将map中key对应的请求删除
	f.mu.Lock()
	delete(f.flight, key)
	f.mu.Unlock()
	//返回结果
	return p.val, p.err
}
