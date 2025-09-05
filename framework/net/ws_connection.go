package net

import (
	"common/logs"
	"github.com/gorilla/websocket"
	"go.uber.org/zap"
	"sync"
	"time"
)

var cidBase uint64 = 10000

var (
	pongWait             = 10 * time.Second
	writeWait            = 10 * time.Second
	pingInterval         = (pongWait * 9) / 10
	maxMessageSize int64 = 1024
)

type WsConnection struct {
	Cid           string // 客户端唯一标识
	Conn          *websocket.Conn
	manager       *Manager
	ReadChan      chan *MsgPack // 消息类型可根据不同需求自定义
	WriteChan     chan []byte
	Session       *Session
	pingTicker    *time.Ticker
	closeChan     chan struct{}
	closeOnce     sync.Once
	readChanOnce  sync.Once
	writeChanOnce sync.Once
}

func NewWsConnection(conn *websocket.Conn, manager *Manager) *WsConnection {
	// 从连接池获取对象
	return GetWsConnectionPool().Get(conn, manager)
}

func (w *WsConnection) GetSession() *Session {
	return w.Session
}

func (w *WsConnection) SendMessage(buf []byte) error {
	w.WriteChan <- buf
	return nil
}

func (w *WsConnection) Close() {
	w.closeOnce.Do(func() {
		// 只执行一次不用检查是否关闭
		close(w.closeChan)

		if w.Conn != nil {
			_ = w.Conn.Close()
		}

		// 停止计时器
		if w.pingTicker != nil {
			w.pingTicker.Stop()
		}

		// 清理session资源
		if w.Session != nil {
			w.Session.Close()
		}

		logs.Log.Info("client connection closed", zap.Any("cid:", w.Cid))

		// 将连接对象放入池中复用
		go func(conn *WsConnection) {
			// 使用延迟执行确保所有资源都已清理
			time.Sleep(100 * time.Millisecond)
			GetWsConnectionPool().Put(conn)
		}(w)
	})
}

func (w *WsConnection) Run() {
	go w.readMessage()
	go w.writeMessage()

	// 做一些心跳检测 websocket中 ping pong机制
	w.Conn.SetPongHandler(w.PongHandler)
}

// writeMessage 写数据
func (w *WsConnection) writeMessage() {
	w.pingTicker = time.NewTicker(pingInterval)

	defer func() {
		if w.WriteChan != nil {
			w.writeChanOnce.Do(func() {
				close(w.WriteChan)
			})
		}
	}()

	for {
		select {
		case message, ok := <-w.WriteChan:
			if !ok {
				if err := w.Conn.WriteMessage(websocket.CloseMessage, nil); err != nil {
					logs.Log.Error("connection closed", zap.Error(err))
				}

				w.Close()
				return
			}

			logs.Log.Warn("message data ", zap.Any("message", message))

			if err := w.Conn.WriteMessage(websocket.TextMessage, message); err != nil {
				logs.Log.Error("writeMessage  BinaryMessage fail ", zap.Any("client:", w.Cid), zap.Error(err))
			}
		case <-w.pingTicker.C:
			// 设置ping超时时间
			if err := w.Conn.SetWriteDeadline(time.Now().Add(writeWait)); err != nil {
				logs.Log.Error("ping SetWriteDeadline fail", zap.Any("client:", w.Cid), zap.Error(err))
			}

			if err := w.Conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				logs.Log.Error("ping  fail", zap.Any("client:", w.Cid), zap.Error(err))
				w.Close()
			}
		case <-w.closeChan:
			// 收到关闭信号，退出协程
			logs.Log.Info("writeMessage stopped", zap.Any("client:", w.Cid))
			return
		}
	}
}

// readMessage 读数据
func (w *WsConnection) readMessage() {
	defer func() {
		logs.Log.Info("readMessage stopped", zap.Any("client:", w.Cid))
		w.manager.removeClient(w)
	}()

	w.Conn.SetReadLimit(maxMessageSize)

	if err := w.Conn.SetReadDeadline(time.Now().Add(pongWait)); err != nil {
		logs.Log.Error("SetReadDeadline", zap.Error(err))
		return
	}

	for {
		select {
		case <-w.closeChan:
			// 收到关闭信号，退出协程
			logs.Log.Info("received close signal", zap.Any("client:", w.Cid))
			return
		default:
			messageType, message, err := w.Conn.ReadMessage()
			if err != nil {
				// 检测到错误或连接关闭，退出循环
				if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
					logs.Log.Error("unexpected close", zap.Any("client:", w.Cid), zap.Error(err))
				}
				return
			}

			logs.Log.Warn("receive====", zap.Any("message", message))

			// 只支持二进制和json消息,如有需求可以添加
			if messageType == websocket.BinaryMessage || messageType == websocket.TextMessage {
				select {
				case w.ReadChan <- &MsgPack{Cid: w.Cid, Body: message}:
				case <-w.closeChan:
					logs.Log.Info("readMessage stopped while sending to channel", zap.Any("client:", w.Cid))
					return
				}
			} else {
				logs.Log.Error("unsupported stream type", zap.Any("messageType", messageType))
			}
		}
	}
}

// PongHandler ping处理器
func (w *WsConnection) PongHandler(data string) error {
	if err := w.Conn.SetReadDeadline(time.Now().Add(pongWait)); err != nil {
		return err
	}
	return nil
}
