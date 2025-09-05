package net

import (
	"sync"
)

type Session struct {
	sync.RWMutex
	Cid     string
	Uid     string
	data    map[string]any
	all     map[string]any
	manager *Manager
}

func NewSession(cid string, manager *Manager) *Session {
	return &Session{
		Cid:     cid,
		data:    make(map[string]any),
		all:     make(map[string]any),
		manager: manager,
	}
}

func (s *Session) Put(key string, value any) {
	s.Lock()
	defer s.Unlock()
	s.data[key] = value
}

func (s *Session) Get(key string) (any, bool) {
	s.RLock()
	defer s.Unlock()
	v, ok := s.data[key]
	return v, ok
}

func (s *Session) SetData(uid string, data map[string]any) {
	s.Lock()
	defer s.Unlock()
	if s.Uid == uid {
		for k, v := range data {
			s.data[k] = v
		}
	}
}

func (s *Session) SetAll(data map[string]any) {
	s.Lock()
	defer s.Unlock()
	for k, v := range data {
		s.all[k] = v
	}
}

//func (s *Session) PushData(dst string, router string, message *protocol.Message) {
//	msg := &stream.Msg{
//		Cid:         s.Cid,
//		Uid:         s.Uid,
//		Src:         s.manager.ServerId,
//		ConnectorId: s.manager.ServerId,
//		Dst:         dst,
//		Router:      router,
//		Body:        message,
//		SessionData: &stream.SessionData{
//			SingleData: s.data,
//			AllData:    s.all,
//		},
//	}
//
//	data, _ := json.Marshal(msg)
//	if err := s.manager.RemoteCli.SendMsg(dst, data); err != nil {
//		logs.Log.Error("push session data err", zap.Error(err))
//	}
//}

func (s *Session) Close() {
	s.Lock()
	defer s.Unlock()
	//清理资源
	s.data = make(map[string]any)
	s.all = make(map[string]any)
}
