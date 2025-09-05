package net

import (
	"common/logs"
	"context"
	"errors"
	"fmt"
	"framework/game"
	"go.uber.org/zap"
	"hash/fnv"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"time"
)

// LoadBalanceStrategy 定义负载均衡策略类型
type LoadBalanceStrategy int

const (
	// Random 随机选择策略
	Random LoadBalanceStrategy = iota
	// RoundRobin 轮询策略
	RoundRobin
	// WeightedRoundRobin 加权轮询策略
	WeightedRoundRobin
	// LeastConnection 最少连接策略
	LeastConnection
	// ConsistentHash 一致性哈希策略
	ConsistentHash
	// IPHash IP哈希策略
	IPHash
)

// 负载均衡状态
type loadBalanceState struct {
	strategy      LoadBalanceStrategy
	roundRobinIdx map[string]int             // 每种服务器类型的轮询索引
	hashRing      map[string]*consistentHash // 每种服务器类型的一致性哈希环
	serverLoads   map[string]int             // 服务器负载计数
	mu            sync.RWMutex               // 保护状态的互斥锁
}

// consistentHash 简单的一致性哈希实现
type consistentHash struct {
	hashRing []uint32          // 排序的哈希环
	mapping  map[uint32]string // 哈希值到服务器ID的映射
}

func (m *Manager) selectDst(serverType string) (string, error) {
	serversConfigs, ok := game.Conf.ServersConf.TypeServer[serverType]
	if !ok {
		return "", errors.New("no server found")
	}

	if len(serversConfigs) == 0 {
		return "", errors.New("no available servers")
	}

	// 如果只有一个服务器，直接返回
	if len(serversConfigs) == 1 {
		return serversConfigs[0].ID, nil
	}

	// 根据选择的负载均衡策略选择服务器
	m.lbState.mu.RLock()
	strategy := m.lbState.strategy
	m.lbState.mu.RUnlock()

	var serverID string
	//var err error

	switch strategy {
	case Random:
		serverID = m.selectRandomServer(serversConfigs)
	case RoundRobin:
		serverID = m.selectRoundRobinServer(serverType, serversConfigs)
	case WeightedRoundRobin:
		serverID = m.selectWeightedRoundRobinServer(serversConfigs)
	case LeastConnection:
		serverID = m.selectLeastConnectionServer(serversConfigs)
	case ConsistentHash:
		// 对于一致性哈希，我们需要一个键（通常是用户ID）
		// 这里我们尝试从当前上下文获取用户ID，如果没有则回退到随机选择
		uid := m.getCurrentUserID()
		if uid != "" {
			serverID = m.selectConsistentHashServer(serverType, uid, serversConfigs)
		} else {
			serverID = m.selectRandomServer(serversConfigs)
			logs.Log.Debug("No user ID available for consistent hash, falling back to random selection")
		}
	case IPHash:
		// 对于IP哈希，我们需要客户端IP
		// 这里我们尝试从当前上下文获取客户端IP，如果没有则回退到随机选择
		clientIP := m.getCurrentClientIP()
		if clientIP != "" {
			serverID = m.selectIPHashServer(serverType, clientIP, serversConfigs)
		} else {
			serverID = m.selectRandomServer(serversConfigs)
			logs.Log.Debug("No client IP available for IP hash, falling back to random selection")
		}
	default:
		// 默认使用随机选择
		serverID = m.selectRandomServer(serversConfigs)
	}

	// 记录服务器选择信息，便于后续分析
	logs.Log.Debug("Selected server", zap.Any("serverId", serverID), zap.Any("for type %s ", serverType), zap.Any("using strategy", strategy))

	return serverID, nil
}

// selectRandomServer 随机选择服务器
func (m *Manager) selectRandomServer(servers []*game.ServersConfig) string {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	index := r.Intn(len(servers))
	return servers[index].ID
}

// selectRoundRobinServer 轮询选择服务器
func (m *Manager) selectRoundRobinServer(serverType string, servers []*game.ServersConfig) string {
	m.lbState.mu.Lock()
	defer m.lbState.mu.Unlock()

	// 初始化该服务器类型的轮询索引（如果不存在）
	if _, exists := m.lbState.roundRobinIdx[serverType]; !exists {
		m.lbState.roundRobinIdx[serverType] = 0
	}

	// 获取当前索引并更新
	index := m.lbState.roundRobinIdx[serverType]
	m.lbState.roundRobinIdx[serverType] = (index + 1) % len(servers)

	return servers[index].ID
}

// selectWeightedRoundRobinServer 加权轮询选择服务器
func (m *Manager) selectWeightedRoundRobinServer(servers []*game.ServersConfig) string {
	// 这里我们假设ServerConfig中有一个Weight字段
	// 如果没有，我们可以根据服务器的其他属性计算权重

	// 简单实现：使用服务器配置中的某个属性作为权重
	// 这里我们假设所有服务器权重相等，实际应用中可以根据需要修改
	totalWeight := len(servers)
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	randomWeight := r.Intn(totalWeight)

	// 选择权重对应的服务器
	currentWeight := 0
	for _, server := range servers {
		currentWeight++
		if currentWeight > randomWeight {
			return server.ID
		}
	}

	// 默认返回第一个服务器
	return servers[0].ID
}

// selectLeastConnectionServer 选择连接数最少的服务器
func (m *Manager) selectLeastConnectionServer(servers []*game.ServersConfig) string {
	m.lbState.mu.RLock()
	defer m.lbState.mu.RUnlock()

	minLoad := -1
	var selectedServer string

	for _, server := range servers {
		load, exists := m.lbState.serverLoads[server.ID]
		if !exists || (minLoad == -1 || load < minLoad) {
			minLoad = load
			selectedServer = server.ID
		}
	}

	return selectedServer
}

// selectConsistentHashServer 使用一致性哈希选择服务器
func (m *Manager) selectConsistentHashServer(serverType string, key string, servers []*game.ServersConfig) string {
	m.lbState.mu.Lock()
	defer m.lbState.mu.Unlock()

	// 初始化该服务器类型的哈希环（如果不存在）
	if _, exists := m.lbState.hashRing[serverType]; !exists {
		m.initConsistentHash(serverType, servers)
	}

	// 计算键的哈希值
	h := fnv.New32a()
	h.Write([]byte(key))
	keyHash := h.Sum32()

	// 在哈希环上查找服务器
	hashRing := m.lbState.hashRing[serverType]
	if hashRing == nil || len(hashRing.hashRing) == 0 {
		// 如果哈希环为空，回退到随机选择
		return m.selectRandomServer(servers)
	}

	// 二分查找大于等于keyHash的第一个点
	idx := sort.Search(len(hashRing.hashRing), func(i int) bool {
		return hashRing.hashRing[i] >= keyHash
	})

	// 如果没有找到，则使用第一个点（环状结构）
	if idx == len(hashRing.hashRing) {
		idx = 0
	}

	// 返回对应的服务器ID
	return hashRing.mapping[hashRing.hashRing[idx]]
}

// selectIPHashServer 使用IP哈希选择服务器
func (m *Manager) selectIPHashServer(serverType string, ip string, servers []*game.ServersConfig) string {
	// IP哈希实际上是一致性哈希的特例，使用IP作为键
	return m.selectConsistentHashServer(serverType, ip, servers)
}

// initConsistentHash 初始化一致性哈希环
func (m *Manager) initConsistentHash(serverType string, servers []*game.ServersConfig) {
	// 为每个服务器创建多个虚拟节点以提高均衡性
	const virtualNodes = 100
	hashRing := &consistentHash{
		hashRing: make([]uint32, 0, len(servers)*virtualNodes),
		mapping:  make(map[uint32]string),
	}

	for _, server := range servers {
		for i := 0; i < virtualNodes; i++ {
			// 为每个服务器创建多个虚拟节点
			key := fmt.Sprintf("%s-%d", server.ID, i)
			h := fnv.New32a()
			h.Write([]byte(key))
			hash := h.Sum32()

			hashRing.hashRing = append(hashRing.hashRing, hash)
			hashRing.mapping[hash] = server.ID
		}
	}

	// 排序哈希环
	sort.Slice(hashRing.hashRing, func(i, j int) bool {
		return hashRing.hashRing[i] < hashRing.hashRing[j]
	})

	// 保存哈希环
	m.lbState.hashRing[serverType] = hashRing
}

// 上下文键，用于在请求上下文中存储用户ID和客户端IP
var (
	userIDContextKey   = struct{}{}
	clientIPContextKey = struct{}{}
	currentContext     context.Context // 当前请求上下文
)

// getCurrentUserID 获取当前上下文中的用户ID
func (m *Manager) getCurrentUserID() string {
	// 尝试从当前上下文中获取用户ID
	if currentContext != nil {
		if uid, ok := currentContext.Value(userIDContextKey).(string); ok && uid != "" {
			return uid
		}
	}

	// 如果无法从上下文中获取，尝试从当前处理的消息中获取
	// 这需要在消息处理过程中设置一个线程本地变量或全局变量
	// 在实际应用中，可能需要根据具体的消息处理流程来实现

	// 这里我们可以尝试从当前正在处理的连接中获取用户ID
	// 注意：这种方法只在处理特定连接的消息时有效
	// 在其他情况下（如定时任务、系统事件等），可能无法获取用户ID

	return ""
}

// getCurrentClientIP 获取当前上下文中的客户端IP
func (m *Manager) getCurrentClientIP() string {
	// 尝试从当前上下文中获取客户端IP
	if currentContext != nil {
		if ip, ok := currentContext.Value(clientIPContextKey).(string); ok && ip != "" {
			return ip
		}
	}

	// 如果无法从上下文中获取，尝试从当前处理的连接中获取
	// 在实际应用中，WebSocket连接通常会记录客户端的IP地址

	return ""
}

// SetRequestContext 设置当前请求上下文
// 这个方法应该在处理每个请求之前调用，以便在负载均衡过程中使用上下文信息
func (m *Manager) SetRequestContext(ctx context.Context) {
	currentContext = ctx
}

// WithUserID 创建一个包含用户ID的上下文
func WithUserID(ctx context.Context, userID string) context.Context {
	return context.WithValue(ctx, userIDContextKey, userID)
}

// WithClientIP 创建一个包含客户端IP的上下文
func WithClientIP(ctx context.Context, clientIP string) context.Context {
	return context.WithValue(ctx, clientIPContextKey, clientIP)
}

// SetLoadBalanceStrategy 设置负载均衡策略
func (m *Manager) SetLoadBalanceStrategy(strategy LoadBalanceStrategy) {
	m.lbState.mu.Lock()
	defer m.lbState.mu.Unlock()

	m.lbState.strategy = strategy
	logs.Log.Info("Load balance strategy set to ", zap.Any("strategy", strategy))
}

// SetLoadBalanceStrategyByName 通过策略名称设置负载均衡策略
func (m *Manager) SetLoadBalanceStrategyByName(strategyName string) error {
	strategy, err := ParseLoadBalanceStrategy(strategyName)
	if err != nil {
		return err
	}

	m.SetLoadBalanceStrategy(strategy)
	return nil
}

// GetCurrentLoadBalanceStrategy 获取当前使用的负载均衡策略
func (m *Manager) GetCurrentLoadBalanceStrategy() LoadBalanceStrategy {
	m.lbState.mu.RLock()
	defer m.lbState.mu.RUnlock()

	return m.lbState.strategy
}

// GetCurrentLoadBalanceStrategyName 获取当前使用的负载均衡策略名称
func (m *Manager) GetCurrentLoadBalanceStrategyName() string {
	strategy := m.GetCurrentLoadBalanceStrategy()
	return strategy.String()
}

// GetAvailableLoadBalanceStrategies 获取所有可用的负载均衡策略
func (m *Manager) GetAvailableLoadBalanceStrategies() []string {
	return []string{
		"random",
		"round_robin",
		"weighted_round_robin",
		"least_connection",
		"consistent_hash",
		"ip_hash",
	}
}

// String 返回负载均衡策略的字符串表示
func (s LoadBalanceStrategy) String() string {
	switch s {
	case Random:
		return "random"
	case RoundRobin:
		return "round_robin"
	case WeightedRoundRobin:
		return "weighted_round_robin"
	case LeastConnection:
		return "least_connection"
	case ConsistentHash:
		return "consistent_hash"
	case IPHash:
		return "ip_hash"
	default:
		return "unknown"
	}
}

// ParseLoadBalanceStrategy 将字符串解析为负载均衡策略
func ParseLoadBalanceStrategy(s string) (LoadBalanceStrategy, error) {
	switch strings.ToLower(s) {
	case "random":
		return Random, nil
	case "round_robin", "roundrobin":
		return RoundRobin, nil
	case "weighted_round_robin", "weightedroundrobin":
		return WeightedRoundRobin, nil
	case "least_connection", "leastconnection":
		return LeastConnection, nil
	case "consistent_hash", "consistenthash":
		return ConsistentHash, nil
	case "ip_hash", "iphash":
		return IPHash, nil
	default:
		return Random, fmt.Errorf("unknown load balance strategy: %s", s)
	}
}
