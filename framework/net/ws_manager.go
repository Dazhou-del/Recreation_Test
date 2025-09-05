package net

import (
	"common/logs"
	"encoding/json"
	"errors"
	"fmt"
	"framework/game"
	"framework/protocol"
	"framework/remote"
	"framework/stream"
	"github.com/gorilla/websocket"
	"github.com/samber/lo"
	"go.uber.org/zap"
	"hash/fnv"
	"net/http"
	"runtime"
	"strings"
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
	// 移除全局锁，使用分片锁
	dataLock           sync.RWMutex // 仅用于保护data字段
	websocketUpgrade   *websocket.Upgrader
	ServerId           string
	CheckOriginHandler CheckOriginHandler

	// 分片存储客户端连接
	clientBuckets []*ClientBucket
	bucketMask    uint32

	// 工作池相关
	ClientReadChan chan *MsgPack
	clientWorkers  []chan *MsgPack // 工作协程池
	workerCount    int             // 工作协程数量

	handlers          map[protocol.PackageType]EventHandler
	ConnectorHandlers LogicHandler

	// 远程消息处理
	RemoteReadChan chan []byte
	RemoteCli      remote.Client
	RemotePushChan chan *stream.Msg

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

	// 负载均衡状态
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
	go m.remoteReadChanHandler()
	go m.remotePushChanHandler()

	// 启动性能监控
	go m.monitorPerformance()

	http.HandleFunc("/", m.serveWS)

	//设置不同的消息处理器
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

// 解析协议 decodeClientPack
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

// SetConnectionRateLimit 设置连接速率限制
func (m *Manager) SetConnectionRateLimit(connectionsPerSecond int) {
	connectionRateLimiter = limit.NewRateLimiter(connectionsPerSecond, 1)
	logs.Log.Info("Connection rate limit set to %d per second", zap.Any("connectionsPerSecond", connectionsPerSecond))
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

// BroadcastToAll 向所有连接的客户端广播消息
func (m *Manager) BroadcastToAll(messageType protocol.PackageType, data []byte) {
	// 编码消息
	res, err := protocol.Encode(messageType, data)
	if err != nil {
		logs.Log.Error("BroadcastToAll encode ", zap.Error(err))
		return
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
				conn.SendMessage(res)
			}
		}(bucket)
	}

	// 等待所有分片处理完成
	wg.Wait()
	logs.Log.Info("Broadcast message sent to all clients")
}

func (m *Manager) remoteReadChanHandler() {
	const batchSize = 32
	batch := make([][]byte, 0, batchSize)

	processBatch := func() {
		if len(batch) == 0 {
			return
		}

		// 并行处理批次中的消息
		var wg sync.WaitGroup
		for _, body := range batch {
			wg.Add(1)
			go func(msgBody []byte) {
				defer wg.Done()

				var msg stream.Msg
				if err := json.Unmarshal(msgBody, &msg); err != nil {
					logs.Log.Error("nats remote stream format fail", zap.Error(err))
					return
				}

				if msg.SessionType == stream.Session {
					//需要特出处理，session类型是存储在connection中的session 并不 推送客户端
					m.setSessionData(msg)
					return
				}

				if msg.Body != nil {
					if msg.Body.Type == protocol.Request || msg.Body.Type == protocol.Response {
						//给客户端回信息 都是 response
						msg.Body.Type = protocol.Response
						m.Response(&msg)
					}
					if msg.Body.Type == protocol.Push {
						select {
						case m.RemotePushChan <- &msg:
							// 成功发送到推送通道
						default:
							// 通道已满，直接处理
							if msg.Body.Type == protocol.Push {
								m.Response(&msg)
							}
						}
					}
				}
			}(body)
		}

		// 等待所有消息处理完成
		wg.Wait()
		batch = batch[:0] // 清空批次
	}

	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case body, ok := <-m.RemoteReadChan:
			if !ok {
				// 通道已关闭
				processBatch() // 处理剩余消息
				return
			}

			batch = append(batch, body)
			if len(batch) >= batchSize {
				processBatch()
			}

		case <-ticker.C:
			processBatch()
		}
	}
}

func (m *Manager) remotePushChanHandler() {
	const batchSize = 32
	batch := make([]*stream.Msg, 0, batchSize)

	processBatch := func() {
		if len(batch) == 0 {
			return
		}

		// 并行处理批次中的消息
		var wg sync.WaitGroup
		for _, msg := range batch {
			if msg.Body.Type == protocol.Push {
				wg.Add(1)
				go func(pushMsg *stream.Msg) {
					defer wg.Done()
					m.Response(pushMsg)
				}(msg)
			}
		}

		// 等待所有消息处理完成
		wg.Wait()
		batch = batch[:0] // 清空批次
	}

	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case msg, ok := <-m.RemotePushChan:
			if !ok {
				// 通道已关闭
				processBatch() // 处理剩余消息
				return
			}

			batch = append(batch, msg)
			if len(batch) >= batchSize {
				processBatch()
			}

		case <-ticker.C:
			processBatch()
		}
	}
}

func (m *Manager) Response(msg *stream.Msg) {
	// 编码消息（只编码一次）
	buf, err := protocol.MessageEncode(msg.Body)
	if err != nil {
		logs.Log.Error("Response MessageEncode fail", zap.Error(err))

		return
	}

	res, err := protocol.Encode(protocol.Data, buf)
	if err != nil {
		logs.Log.Error("Response Encode fail", zap.Error(err))
		return
	}

	if msg.Body.Type == protocol.Push {
		// 推送消息给多个用户
		if len(msg.PushUser) > 0 {
			// 创建用户ID到连接的映射
			userConnectionList := make(map[string][]Connection)

			// 并行收集每个分片中的目标连接
			var wg sync.WaitGroup
			var mu sync.Mutex // 保护userConnections
			for _, bucket := range m.clientBuckets {
				wg.Add(1)
				go func(b *ClientBucket) {
					defer wg.Done()

					b.RLock()

					for _, conn := range b.clients {
						uid := conn.GetSession().Uid
						if lo.Contains(msg.PushUser, uid) {
							mu.Lock()
							userConnectionList[uid] = append(userConnectionList[uid], conn)
							mu.Unlock()
						}
					}
					b.Unlock()
				}(bucket)
			}

			wg.Wait()

			// 并行发送消息
			var sendWg sync.WaitGroup
			for _, connections := range userConnectionList {
				for _, conn := range connections {
					sendWg.Add(1)
					go func(c Connection) {
						defer sendWg.Done()
						c.SendMessage(res)
					}(conn)
				}
			}

			// 可选：等待所有消息发送完成
			// sendWg.Wait()
		}
	} else if msg.Cid != "" {
		// 发送消息给单个客户端
		bucket := m.getBucket(msg.Cid)
		bucket.RLock()
		connection, ok := bucket.clients[msg.Cid]
		bucket.RUnlock()

		if ok {
			connection.SendMessage(res)
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

// 获取连接所在的分片
func (m *Manager) getBucket(cid string) *ClientBucket {
	hash := fnv32(cid)
	index := hash & m.bucketMask
	return m.clientBuckets[index]
}

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

// 性能监控
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
	wsConn.SetReadDeadline(time.Now().Add(120 * time.Second))
	wsConn.SetWriteDeadline(time.Now().Add(10 * time.Second))

	// 创建客户端连接
	client := NewWsConnection(wsConn, m)

	// 记录连接成功
	logs.Log.Debug("WebSocket connection established", zap.Any("client.Cid", client.Cid), zap.Any("from remoteAddr", r.RemoteAddr))

	// 添加客户端并启动
	m.addClient(client)
	client.Run()
}

func (m *Manager) setupEventHandlers() {
	m.handlers[protocol.Handshake] = m.HandshakeHandler
	m.handlers[protocol.HandshakeAck] = m.HandshakeAckHandler
	m.handlers[protocol.Heartbeat] = m.HeartbeatHandler
	m.handlers[protocol.Data] = m.MessageHandler
	m.handlers[protocol.Kick] = m.KickHandler
}

func (m *Manager) HandshakeHandler(packet *protocol.Packet, c Connection) error {
	res := protocol.HandshakeResponse{
		Code: 200,
		Sys: protocol.Sys{
			Heartbeat: 3,
		},
	}

	data, _ := json.Marshal(res)
	buf, err := protocol.Encode(packet.Type, data)
	if err != nil {
		logs.Log.Error("encode packet ", zap.Error(err))

		return err
	}
	return c.SendMessage(buf)
}

func (m *Manager) HandshakeAckHandler(packet *protocol.Packet, c Connection) error {
	return nil
}

func (m *Manager) HeartbeatHandler(packet *protocol.Packet, c Connection) error {
	var res []byte
	data, _ := json.Marshal(res)
	buf, err := protocol.Encode(packet.Type, data)
	if err != nil {
		logs.Log.Error("encode packet err", zap.Error(err))
		return err
	}

	return c.SendMessage(buf)
}

func (m *Manager) MessageHandler(packet *protocol.Packet, c Connection) error {
	message := packet.MessageBody()
	//connector.entryHandler.entry
	routeStr := message.Route
	routers := strings.Split(routeStr, ".")
	if len(routers) != 3 {
		return errors.New("router unsupported")
	}

	serverType := routers[0]
	handlerMethod := fmt.Sprintf("%s.%s", routers[1], routers[2])
	connectorConfig := game.Conf.GetConnectorByServerType(serverType)
	if connectorConfig != nil {
		//本地connector服务器处理
		handler, ok := m.ConnectorHandlers[handlerMethod]
		if ok {
			data, err := handler(c.GetSession(), message.Data)
			if err != nil {
				return err
			}
			marshal, _ := json.Marshal(data)
			message.Type = protocol.Response
			message.Data = marshal
			encode, err := protocol.MessageEncode(message)
			if err != nil {
				return err
			}
			res, err := protocol.Encode(packet.Type, encode)
			if err != nil {
				return err
			}
			return c.SendMessage(res)
		}
	} else {
		//nats 远端调用处理 hall.userHandler.updateUserAddress
		dst, err := m.selectDst(serverType)
		if err != nil {
			logs.Log.Error("remote send stream selectDst", zap.Error(err))

			return err
		}

		msg := &stream.Msg{
			Cid:         c.GetSession().Cid,
			Uid:         c.GetSession().Uid,
			Src:         m.ServerId,
			ConnectorId: m.ServerId,
			Dst:         dst,
			Router:      handlerMethod,
			Body:        message,
			SessionData: &stream.SessionData{
				SingleData: c.GetSession().data,
				AllData:    c.GetSession().all,
			},
		}
		data, _ := json.Marshal(msg)
		logs.Log.Warn("remote send stream", zap.Any("data", string(msg.Body.Data)))
		err = m.RemoteCli.SendMsg(dst, data)

		if err != nil {
			logs.Log.Error("remote send stream ", zap.Error(err))

			return err
		}
	}
	return nil
}

func (m *Manager) KickHandler(packet *protocol.Packet, c Connection) error {
	return nil
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
		handlers:       make(map[protocol.PackageType]EventHandler),
		RemoteReadChan: make(chan []byte, 2048),      // 增大缓冲区
		RemotePushChan: make(chan *stream.Msg, 2048), // 增大缓冲区
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

// SetMaxConnections 设置最大连接数
func (m *Manager) SetMaxConnections(maxConn int) {
	// 只能在启动前调用
	if m.connSemaphore == nil {
		m.maxConnections = maxConn
		m.connSemaphore = make(chan struct{}, maxConn)
		logs.Log.Info("Max connections set to ", zap.Any("maxConn", maxConn))
	} else {
		logs.Log.Warn("Cannot change max connections after manager has started")
	}
}

// SetWorkerCount 设置工作协程数量
func (m *Manager) SetWorkerCount(count int) {
	// 只能在启动前调用
	if m.clientWorkers == nil {
		m.workerCount = count
		logs.Log.Info("Worker count set to", zap.Any("count", count))
	} else {
		logs.Log.Warn("Cannot change worker count after manager has started")
	}
}

// SetBucketCount 设置分片数量
func (m *Manager) SetBucketCount(count int) {
	// 只能在启动前调用
	if m.clientBuckets == nil {
		// 确保是2的幂次方
		bucketCount := 1
		for bucketCount < count {
			bucketCount *= 2
		}
		m.bucketMask = uint32(bucketCount - 1)
		logs.Log.Info("Bucket count set to %d (rounded to power of 2)", zap.Any("bucketCount", bucketCount))
	} else {
		logs.Log.Warn("Cannot change bucket count after manager has started")
	}
}
