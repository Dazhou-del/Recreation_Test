package repo

import (
	"common/database"
	"context"
)

type Manager struct {
	Mongo *database.MongoManager
	Redis *database.RedisManager
}

func (m *Manager) Close() {
	if m.Mongo != nil {
		m.Mongo.Close()
	}

	if m.Redis != nil {
		m.Redis.Close()
	}
}

func New(context context.Context) *Manager {
	return &Manager{
		Mongo: database.NewMongo(context),
		Redis: database.NewRedis(context),
	}
}
