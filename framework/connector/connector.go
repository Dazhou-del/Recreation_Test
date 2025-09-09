package connector

import (
	"common/logs"
	"fmt"
	"framework/game"
	"framework/net"
	"go.uber.org/zap"
	"net/http"
)

type Connector struct {
	isRunning bool
	wsManager *net.Manager
	handlers  net.LogicHandler
}

func Default() *Connector {
	return &Connector{
		handlers: make(net.LogicHandler),
	}
}

func (c *Connector) Run(serverId string, maxConn int) {
	if !c.isRunning {
		//启动websocket和nats
		c.wsManager = net.NewManager(maxConn)
		c.wsManager.CheckTokenHandler = c.checkTokenHandler
		c.wsManager.ConnectorHandlers = c.handlers
		c.wsManager.ConnCloseHandler = c.connCloseHandler
		c.Serve(serverId)
	}
}
func (c *Connector) Close() {
	if c.isRunning {
		//关闭websocket和nats
		c.wsManager.Close()
	}
}

func (c *Connector) Serve(serverId string) {
	logs.Log.Info("run connector", zap.Any("serverId", serverId))
	//地址 需要读取配置文件 在游戏中可能加载很多的信息（配置） 如果写到yml可能会比较复杂 不容易维护
	//游戏中的配置 读取 一般采用json的方式 需要读取json的配置文件
	c.wsManager.ServerId = serverId
	connectorConfig := game.Conf.GetConnector(serverId)
	if connectorConfig == nil {
		logs.Log.Fatal("no connector config found")
	}
	addr := fmt.Sprintf("%s:%d", connectorConfig.Host, connectorConfig.ClientPort)
	c.isRunning = true
	c.wsManager.Run(addr)
}

func (c *Connector) RegisterHandler(handlers net.LogicHandler) {
	c.handlers = handlers
}

func (c *Connector) checkTokenHandler(r *http.Request) bool {
	logs.Log.Info("checkTokenHandler")
	return true
}

func (c *Connector) connCloseHandler(*net.WsConnection, int, string) error {
	logs.Log.Info("conn close")

	return nil
}
