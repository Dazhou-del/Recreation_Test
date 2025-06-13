package database

import (
	"common/config"
	"common/logs"
	"context"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.uber.org/zap"
)

type MongoManager struct {
	Cli *mongo.Client
	Db  *mongo.Database
}

func NewMongo(ctx context.Context) *MongoManager {
	clientOptions := options.Client().ApplyURI(config.Conf.Database.MongoConf.Url)
	clientOptions.SetAuth(options.Credential{
		Username: config.Conf.Database.MongoConf.UserName,
		Password: config.Conf.Database.MongoConf.Password,
	})

	clientOptions.SetMinPoolSize(uint64(config.Conf.Database.MongoConf.MinPoolSize))
	clientOptions.SetMaxPoolSize(uint64(config.Conf.Database.MongoConf.MaxPoolSize))

	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		logs.Log.Fatal("mongo connect ", zap.Error(err))
		panic(err)
		return nil
	}

	if err := client.Ping(ctx, readpref.Primary()); err != nil {
		logs.Log.Fatal("mongo ping  ", zap.Error(err))
		panic(err)

		return nil
	}

	m := &MongoManager{
		Cli: client,
	}

	m.Db = m.Cli.Database(config.Conf.Database.MongoConf.Db)
	return m
}

func (m *MongoManager) Close() {
	err := m.Cli.Disconnect(context.TODO())
	if err != nil {
		logs.Log.Error("mongo close ", zap.Error(err))
	}
}
