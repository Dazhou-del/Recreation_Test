package net

import (
	"common/logs"
	"encoding/json"
	"errors"
	"framework/protocol"
	"framework/stream"
	"github.com/gorilla/websocket"
	"go.uber.org/zap"
	"hash/fnv"
	"net/http"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
	"utils/limit"
)

var (
	// 优化websocket连接配置
	websocketUpgrade = websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool {
			return true
		},
		ReadBufferSize:    4096, // 增加缓冲区大小
		WriteBufferSize:   4096, // 增加缓冲区大小
		EnableCompression: true, // 启用压缩
	}

	// 连接限流配置
	connectionRateLimiter = limit.NewRateLimiter(100, 1) // 每秒最多100个新连接
)

type Manager struct {
	dataLock           sync.RWMutex // 仅用于保护data字段
	websocketUpgrade   *websocket.Upgrader
	ServerId           string
	CheckOriginHandler CheckOriginHandler

	// 分片存储客户端连接，使用分片锁
	clientBuckets []*ClientBucket
	bucketMask    uint32

	// 工作池相关
	ClientReadChan chan *MsgPack
	clientWorkers  []chan *MsgPack // 工作协程池
	workerCount    int             // 工作协程数量

	//handlers          map[protocol.PackageType]EventHandler
	handlers          map[string]EventHandler
	ConnectorHandlers LogicHandler

	// 共享数据
	data map[string]any

	// 连接限制
	maxConnections int           // 最大连接数
	connSemaphore  chan struct{} // 连接信号量

	// 性能统计
	stats struct {
		messageProcessed   int64
		messageErrors      int64
		avgProcessingTime  int64
		currentConnections int32
	}

	// 负载均衡状态,只有客户端与ws服务端这层则无需配置
	lbState loadBalanceState
}

// ClientBucket 客户端连接分片
type ClientBucket struct {
	sync.RWMutex
	clients map[string]Connection
}

// NewClientBucket 创建新的客户端分片
func NewClientBucket() *ClientBucket {
	return &ClientBucket{
		clients: map[string]Connection{},
	}
}

// NewManager 创建一个新的连接管理器
func NewManager(maxConn int) *Manager {
	// 确定分片数量，使用2的幂次方以便位运算
	bucketCount := 32
	bucketMask := uint32(bucketCount - 1)

	// 确定工作协程数量，默认为CPU核心数的2倍
	workerCount := runtime.NumCPU() * 2

	m := &Manager{
		ClientReadChan: make(chan *MsgPack, 2048), // 增大缓冲区
		handlers:       make(map[string]EventHandler),
		data:           make(map[string]any),
		maxConnections: maxConn,
		connSemaphore:  make(chan struct{}, maxConn),
		bucketMask:     bucketMask,
		workerCount:    workerCount,
		// 初始化负载均衡状态
		lbState: loadBalanceState{
			strategy:      Random, // 默认使用随机策略
			roundRobinIdx: make(map[string]int),
			hashRing:      make(map[string]*consistentHash),
			serverLoads:   make(map[string]int),
		},
	}

	// 初始化客户端分片
	m.clientBuckets = make([]*ClientBucket, bucketCount)
	for i := 0; i < bucketCount; i++ {
		m.clientBuckets[i] = NewClientBucket()
	}

	// 初始化工作协程池
	m.clientWorkers = make([]chan *MsgPack, workerCount)
	for i := 0; i < workerCount; i++ {
		m.clientWorkers[i] = make(chan *MsgPack, 256)
	}

	// 设置默认的CheckOriginHandler
	m.CheckOriginHandler = func(r *http.Request) bool {
		return true
	}

	logs.Log.Info("WebSocket manager initialized with", zap.Any("workerCount", workerCount), zap.Any("worker goroutines and %d connection buckets", bucketCount))

	return m
}

// Close 关闭wsManager
func (m *Manager) Close() {
	// 使用多个goroutine并行关闭连接
	var wg sync.WaitGroup

	for i, bucket := range m.clientBuckets {
		wg.Add(1)
		go func(b *ClientBucket, bucketID int) {
			defer wg.Done()

			b.Lock()
			clients := make([]Connection, 0, len(b.clients))
			for _, client := range b.clients {
				clients = append(clients, client)
			}

			// 清空map
			for cid := range b.clients {
				delete(b.clients, cid)
			}
			b.Unlock()

			// 关闭连接
			for _, client := range clients {
				client.Close()
				// 不需要从connSemaphore中取出，因为整个Manager都要关闭了
			}

			logs.Log.Info("Closed", zap.Any("len(clients)", len(clients)), zap.Any("connections in bucket", bucketID))
		}(bucket, i)
	}

	wg.Wait()
	logs.Log.Info("All connections closed")
}

type CheckOriginHandler func(r *http.Request) bool
type HandlerFunc func(session *Session, body []byte) (any, error)
type LogicHandler map[string]HandlerFunc
type EventHandler func(packet *protocol.Packet, c Connection) error

func (m *Manager) Run(addr string) {
	// 启动工作协程池
	for i := 0; i < m.workerCount; i++ {
		go m.clientWorkerRoutine(i)
	}

	go m.clientReadChanHandler()

	// 启动性能监控
	go m.monitorPerformance()

	http.HandleFunc("/", m.serveWS)

	m.setupEventHandlers()

	logs.Log.Info("WebSocket manager started with",
		zap.Any("workerCount", m.workerCount),
		zap.Any("worker goroutines and connection buckets", len(m.clientBuckets)))

	logs.Log.Fatal("connector listen serve ", zap.Error(http.ListenAndServe(addr, nil)))
}

func (m *Manager) clientWorkerRoutine(workerID int) {
	for msg := range m.clientWorkers[workerID] {
		startTime := time.Now()
		m.decodeClientPack(msg)
		processingTime := time.Since(startTime).Microseconds()

		// 更新统计信息
		atomic.AddInt64(&m.stats.messageProcessed, 1)
		// 使用指数移动平均更新处理时间
		oldAvg := atomic.LoadInt64(&m.stats.avgProcessingTime)
		newAvg := (oldAvg*9 + processingTime) / 10 // 90%旧值，10%新值
		atomic.StoreInt64(&m.stats.avgProcessingTime, newAvg)
	}
}

// decodeClientPack 解析协议
func (m *Manager) decodeClientPack(body *MsgPack) {
	// 可根据不同的协议类型进行解析，按需更改
	packet, err := protocol.Decode(body.Body)
	if err != nil {
		atomic.AddInt64(&m.stats.messageErrors, 1)
		logs.Log.Error("decode stream", zap.Error(err))
		return
	}

	if err := m.routeEvent(packet, body.Cid); err != nil {
		atomic.AddInt64(&m.stats.messageErrors, 1)
		logs.Log.Error("routeEvent ", zap.Error(err))
	}
}

// routeEvent 根据路由处理事件
func (m *Manager) routeEvent(packet *protocol.Packet, cid string) error {
	// 根据packet.type来做不同的处理
	bucket := m.getBucket(cid)

	bucket.RLock()
	conn, ok := bucket.clients[cid]
	bucket.RUnlock()

	if !ok {
		return errors.New("no client found")
	}

	handler, ok := m.handlers[packet.Type]
	if !ok {
		return errors.New("no packetType found")
	}

	return handler(packet, conn)
}

func (m *Manager) clientReadChanHandler() {
	for body := range m.ClientReadChan {
		// 根据连接ID分配到特定工作协程
		hash := fnv32(body.Cid)
		workerID := hash % uint32(m.workerCount)
		select {
		case m.clientWorkers[workerID] <- body:
			// 消息已分发到工作协程
		default:
			// 工作协程队列已满，记录错误并尝试直接处理
			atomic.AddInt64(&m.stats.messageErrors, 1)
			logs.Log.Warn("Worker queue  full, processing message in main goroutine", zap.Any("workerID", workerID))
			go m.decodeClientPack(body) // 使用新的goroutine避免阻塞
		}
	}
}

func (m *Manager) setSessionData(msg stream.Msg) {
	if msg.SessionData == nil {
		return
	}

	// 处理单个连接的数据
	if msg.Cid != "" && msg.SessionData.SingleData != nil {
		bucket := m.getBucket(msg.Cid)
		bucket.RLock()
		connection, ok := bucket.clients[msg.Cid]
		bucket.RUnlock()

		if ok {
			connection.GetSession().SetData(msg.Uid, msg.SessionData.SingleData)
		}
	}

	// 处理全局数据
	if len(msg.SessionData.AllData) > 0 {
		// 先更新Manager的数据
		m.dataLock.Lock()
		for k, v := range msg.SessionData.AllData {
			m.data[k] = v
		}
		m.dataLock.Unlock()

		// 使用工作池更新所有连接的数据
		allData := msg.SessionData.AllData

		// 并行处理每个分片
		var wg sync.WaitGroup
		for _, bucket := range m.clientBuckets {
			wg.Add(1)
			go func(b *ClientBucket) {
				defer wg.Done()

				// 获取分片中的所有连接
				b.RLock()
				connections := make([]Connection, 0, len(b.clients))
				for _, conn := range b.clients {
					connections = append(connections, conn)
				}
				b.RUnlock()

				// 更新每个连接的会话数据
				for _, conn := range connections {
					conn.GetSession().SetAll(allData)
				}
			}(bucket)
		}

		// 等待所有分片处理完成
		wg.Wait()
	}
}

// removeClient 从管理器中删除一个客户端
func (m *Manager) removeClient(wc *WsConnection) {
	bucket := m.getBucket(wc.Cid)

	bucket.Lock()
	if _, exists := bucket.clients[wc.Cid]; exists {
		// 先从map中删除，避免其他地方再次访问
		delete(bucket.clients, wc.Cid)
		bucket.Unlock()

		// 关闭连接
		wc.Close()

		// 释放连接槽位
		<-m.connSemaphore

		// 更新统计信息
		atomic.AddInt32(&m.stats.currentConnections, -1)
	} else {
		bucket.Unlock()
	}
}

// getBucket 获取连接所在的分片
func (m *Manager) getBucket(cid string) *ClientBucket {
	hash := fnv32(cid)
	index := hash & m.bucketMask
	return m.clientBuckets[index]
}

// addClient 添加连接
func (m *Manager) addClient(client *WsConnection) {
	// 使用分片锁
	bucket := m.getBucket(client.Cid)

	select {
	case m.connSemaphore <- struct{}{}:
		// 允许新连接
		bucket.Lock()
		bucket.clients[client.Cid] = client
		bucket.Unlock()

		// 设置会话数据
		m.dataLock.RLock()
		client.GetSession().SetAll(m.data)
		m.dataLock.RUnlock()
		// 更新统计信息
		atomic.AddInt32(&m.stats.currentConnections, 1)

		return
	default:
		// 连接数已达上限
		logs.Log.Warn("Connection limit reached, rejecting new connection")
		client.Close()

		return
	}
}

// 用于计算哈希值的辅助函数
func fnv32(key string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(key))
	return h.Sum32()
}

// monitorPerformance 性能监控
func (m *Manager) monitorPerformance() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		logs.Log.Info("Performance stats", zap.Any("connections", atomic.LoadInt32(&m.stats.currentConnections)),
			zap.Any("messages_processed", atomic.LoadInt64(&m.stats.messageProcessed)),
			zap.Any("avg_processing_time", atomic.LoadInt64(&m.stats.avgProcessingTime)),
			zap.Any("errors", atomic.LoadInt64(&m.stats.messageErrors)))
	}
}

// serveWS 启动WebSocket服务
func (m *Manager) serveWS(w http.ResponseWriter, r *http.Request) {
	// 连接限流
	if !connectionRateLimiter.Allow() {
		http.Error(w, "Too many connections", http.StatusTooManyRequests)
		logs.Log.Warn("Connection rate limit exceeded from", zap.Any("remoteAddr", r.RemoteAddr))

		return
	}

	// 检查当前连接是否已达上限
	if atomic.LoadInt32(&m.stats.currentConnections) >= int32(m.maxConnections) {
		http.Error(w, "Server is at capacity", http.StatusServiceUnavailable)
		logs.Log.Warn("Connection limit reached, rejecting connection from", zap.Any("remoteAddr", r.RemoteAddr))

		return
	}

	// 设置连接超时
	var upgrader *websocket.Upgrader
	if m.websocketUpgrade == nil {
		// 创建一个带有超时设置的upgrader
		upgrader = &websocketUpgrade
	} else {
		upgrader = m.websocketUpgrade
	}

	// 设置响应头
	header := w.Header()
	header.Add("Server", "name-WebSocket-Server")

	// 记录连接信息
	logs.Log.Debug("WebSocket connection attempt from ", zap.Any("remoteAddr", r.RemoteAddr), zap.Any("UserAgent", r.UserAgent()))

	// 升级HTTP连接为WebSocket
	wsConn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		logs.Log.Error("websocketUpgrade.Upgrade fail ", zap.Error(err), zap.Any("from remoteAddr", r.RemoteAddr))

		return
	}

	// 设置读写超时
	if err := wsConn.SetReadDeadline(time.Now().Add(120 * time.Second)); err != nil {
		logs.Log.Error("websocketUpgrade.SetReadDeadline fail ", zap.Error(err), zap.Any("from remoteAddr", r.RemoteAddr))

		return
	}

	if err := wsConn.SetWriteDeadline(time.Now().Add(10 * time.Second)); err != nil {
		logs.Log.Error("websocketUpgrade.SetWriteDeadline fail ", zap.Error(err), zap.Any("from remoteAddr", r.RemoteAddr))

		return
	}

	// 创建客户端连接
	client := NewWsConnection(wsConn, m)

	// 记录连接成功
	logs.Log.Debug("WebSocket connection established", zap.Any("client.Cid", client.Cid), zap.Any("from remoteAddr", r.RemoteAddr))

	// 添加客户端并启动
	m.addClient(client)
	client.Run()
}

// setupEventHandlers 设置事件处理器
func (m *Manager) setupEventHandlers() {
	//m.handlers[protocol.Handshake] = m.HandshakeHandler
	//m.handlers[protocol.HandshakeAck] = m.HandshakeAckHandler
	//m.handlers[protocol.Heartbeat] = m.HeartbeatHandler
	//m.handlers[protocol.Data] = m.MessageHandler
	//m.handlers[protocol.Kick] = m.KickHandler
	// 可定义消息处理器
	m.handlers["1"] = m.MessageHandler
}

// MessageHandler 消息处理器
func (m *Manager) MessageHandler(packet *protocol.Packet, c Connection) error {
	res, err := json.Marshal(packet.Body)
	if err != nil {
		logs.Log.Error("MessageHandler fail", zap.Error(err), zap.Any("packet.Body", packet.Body))
	}

	m.BroadcastToAll(packet)
	return c.SendMessage(res)
}

// UpdateServerLoad 更新服务器负载
func (m *Manager) UpdateServerLoad(serverID string, load int) {
	m.lbState.mu.Lock()
	defer m.lbState.mu.Unlock()

	m.lbState.serverLoads[serverID] = load
}

// GetAllClients 获取所有客户端连接
func (m *Manager) GetAllClients() map[string]Connection {
	result := make(map[string]Connection)

	// 从所有分片中收集客户端
	for _, bucket := range m.clientBuckets {
		bucket.RLock()
		for cid, conn := range bucket.clients {
			result[cid] = conn
		}
		bucket.RUnlock()
	}

	return result
}

// GetConnectionCount 获取当前连接数量
func (m *Manager) GetConnectionCount() int {
	return int(atomic.LoadInt32(&m.stats.currentConnections))
}

// GetStats 获取性能统计信息
func (m *Manager) GetStats() map[string]interface{} {
	return map[string]interface{}{
		"connections":            atomic.LoadInt32(&m.stats.currentConnections),
		"messages_processed":     atomic.LoadInt64(&m.stats.messageProcessed),
		"message_errors":         atomic.LoadInt64(&m.stats.messageErrors),
		"avg_processing_time_us": atomic.LoadInt64(&m.stats.avgProcessingTime),
		"worker_count":           m.workerCount,
		"bucket_count":           len(m.clientBuckets),
	}
}

// FindClientByUID 根据用户ID查找客户端连接
func (m *Manager) FindClientByUID(uid string) Connection {
	if uid == "" {
		return nil
	}

	// 并行搜索所有分片
	type result struct {
		conn  Connection
		found bool
	}

	results := make(chan result, len(m.clientBuckets))

	for _, bucket := range m.clientBuckets {
		go func(b *ClientBucket) {
			b.RLock()
			defer b.RUnlock()

			for _, conn := range b.clients {
				if conn.GetSession().Uid == uid {
					results <- result{conn: conn, found: true}
					return
				}
			}

			results <- result{found: false}
		}(bucket)
	}

	// 收集结果
	for i := 0; i < len(m.clientBuckets); i++ {
		if r := <-results; r.found {
			return r.conn
		}
	}

	return nil
}

// SetConnectionRateLimit 设置连接速率限制
func (m *Manager) SetConnectionRateLimit(connectionsPerSecond int) {
	connectionRateLimiter = limit.NewRateLimiter(connectionsPerSecond, 1)
	logs.Log.Info("Connection rate limit set to %d per second", zap.Any("connectionsPerSecond", connectionsPerSecond))
}

// BroadcastToAll 向所有连接的客户端广播消息
func (m *Manager) BroadcastToAll(packet *protocol.Packet) {
	res, err := json.Marshal(packet.Body)
	if err != nil {
		logs.Log.Error("MessageHandler fail", zap.Error(err), zap.Any("packet.Body", packet.Body))
	}

	// 并行处理每个分片
	var wg sync.WaitGroup
	for _, bucket := range m.clientBuckets {
		wg.Add(1)
		go func(b *ClientBucket) {
			defer wg.Done()

			// 获取分片中的所有连接
			b.RLock()
			connections := make([]Connection, 0, len(b.clients))
			for _, conn := range b.clients {
				connections = append(connections, conn)
			}
			b.RUnlock()

			// 向每个连接发送消息
			for _, conn := range connections {
				if err := conn.SendMessage(res); err != nil {
					logs.Log.Error("SendMessage fail", zap.Error(err), zap.Any("data", res))
				}
			}
		}(bucket)
	}

	// 等待所有分片处理完成
	wg.Wait()
	logs.Log.Info("Broadcast message sent to all clients")
}
