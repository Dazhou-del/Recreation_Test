package database

import (
	"common/config"
	"common/logs"
	"context"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
	"time"
)

type RedisManager struct {
	Cli        *redis.Client        // 单机
	ClusterCli *redis.ClusterClient // 集群
}

func NewRedis(ctx context.Context) *RedisManager {
	var clusterCli *redis.ClusterClient
	var cli *redis.Client
	addrList := config.Conf.Database.RedisConf.ClusterAddrList

	if len(addrList) == 0 {
		//非集群 单节点
		cli = redis.NewClient(&redis.Options{
			Addr:         config.Conf.Database.RedisConf.Addr,
			PoolSize:     config.Conf.Database.RedisConf.PoolSize,
			MinIdleConns: config.Conf.Database.RedisConf.MinIdleConnList,
			Password:     config.Conf.Database.RedisConf.Password,
		})
	} else {
		clusterCli = redis.NewClusterClient(&redis.ClusterOptions{
			Addrs:        config.Conf.Database.RedisConf.ClusterAddrList,
			PoolSize:     config.Conf.Database.RedisConf.PoolSize,
			MinIdleConns: config.Conf.Database.RedisConf.MinIdleConnList,
			Password:     config.Conf.Database.RedisConf.Password,
		})
	}

	if clusterCli != nil {
		if err := clusterCli.Ping(ctx).Err(); err != nil {
			panic(err)
			return nil
		}
	}

	if cli != nil {
		if err := cli.Ping(ctx).Err(); err != nil {
			panic(err)
			return nil
		}
	}

	return &RedisManager{
		Cli:        cli,
		ClusterCli: clusterCli,
	}
}

func (r *RedisManager) Close() {
	if r.ClusterCli != nil {
		if err := r.ClusterCli.Close(); err != nil {
			logs.Log.Error("redis cluster close", zap.Error(err))
		}
	}

	if r.Cli != nil {
		if err := r.Cli.Close(); err != nil {
			logs.Log.Error("redis close ", zap.Error(err))
		}
	}
}

func (r *RedisManager) Set(ctx context.Context, key, value string, expire time.Duration) error {
	if r.ClusterCli != nil {
		return r.ClusterCli.Set(ctx, key, value, expire).Err()
	}
	if r.Cli != nil {
		return r.Cli.Set(ctx, key, value, expire).Err()
	}
	return nil
}
