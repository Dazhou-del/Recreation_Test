package net

import (
	"fmt"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"sync"
	"sync/atomic"
)

// WsConnectionPool WebSocket连接对象池
type WsConnectionPool struct {
	pool      sync.Pool
	count     int32 // 当前池中对象数量
	maxSize   int32 // 最大池大小
	created   int64 // 总共创建的对象数
	reused    int64 // 复用的对象数
	discarded int64 // 丢弃的对象数
}

var (
	// 全局连接池实例
	globalWsConnectionPool *WsConnectionPool
	poolOnce               sync.Once
)

func GetWsConnectionPool() *WsConnectionPool {
	poolOnce.Do(func() {
		globalWsConnectionPool = NewWsConnectionPool(10000) // 默认最大池大小为10000
	})

	return globalWsConnectionPool
}

func NewWsConnectionPool(maxSize int32) *WsConnectionPool {
	p := &WsConnectionPool{
		maxSize: maxSize,
	}

	p.pool = sync.Pool{
		// 创建新的连接的方法
		New: func() interface{} {
			atomic.AddInt64(&p.created, 1)
			atomic.AddInt32(&p.count, 1)
			return &WsConnection{}
		},
	}

	return p
}

// Get 从连接池中获取一个连接
func (w *WsConnectionPool) Get(conn *websocket.Conn, manager *Manager) *WsConnection {
	wsConn := w.pool.Get().(*WsConnection)
	atomic.AddInt64(&w.reused, 1)

	// 初始化连接对象
	// 连接客户端id,可根据需要自定义
	cid := fmt.Sprintf("%s-%s-%d", uuid.New().String(), manager.ServerId, atomic.AddUint64(&cidBase, 1))

	wsConn.Conn = conn
	wsConn.manager = manager
	wsConn.Cid = cid
	wsConn.WriteChan = make(chan []byte, 1024)

	// 将wsConn中的ReadChan汇总到manager.ClientReadChan中
	wsConn.ReadChan = manager.ClientReadChan
	wsConn.Session = NewSession(cid, manager)
	wsConn.closeChan = make(chan struct{})

	// 重置同步对象
	wsConn.closeOnce = sync.Once{}
	wsConn.readChanOnce = sync.Once{}
	wsConn.writeChanOnce = sync.Once{}

	return wsConn
}

// Put 将对象设置到连接池中
func (w *WsConnectionPool) Put(wsConn *WsConnection) {
	if wsConn == nil {
		return
	}

	if atomic.LoadInt32(&w.count) > w.maxSize {
		atomic.AddInt64(&w.discarded, 1)
		atomic.AddInt32(&w.count, -1)
		return
	}

	// 重置连接状态
	wsConn.reset()
	w.pool.Put(wsConn)
}

// reset 重置WsConnection对象的状态
func (w *WsConnection) reset() {
	w.Cid = ""
	w.Conn = nil
	w.manager = nil
	w.ReadChan = nil
	w.WriteChan = nil
	w.Session = nil
	w.pingTicker = nil
	w.closeChan = nil
}

// Stats 获取池统计信息
func (w *WsConnectionPool) Stats() map[string]any {
	return map[string]any{
		"current":   atomic.LoadInt32(&w.count),
		"max":       w.maxSize,
		"created":   atomic.LoadInt64(&w.created),
		"reused":    atomic.LoadInt64(&w.reused),
		"discarded": atomic.LoadInt64(&w.discarded),
	}
}
