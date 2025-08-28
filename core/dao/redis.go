package dao

import (
	"common/logs"
	"context"
	"core/repo"
	"errors"
	"fmt"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

const Prefix = "recreation_test"
const AccountIdRedisKey = "AccountId"
const AccountIdBegin = 10000

type RedisDao struct {
	repo *repo.Manager
}

func (d *RedisDao) NextAccountId() (string, error) {
	// 自增 给一个前缀
	return d.incr(Prefix + ":" + AccountIdRedisKey)
}

func (d *RedisDao) incr(key string) (string, error) {
	// 判断此key是否存在 不存在 set 存在就自增
	todo := context.TODO()
	var exist int64
	var err error

	// 0 代表不存在
	if d.repo.Redis.Cli != nil {
		exist, err = d.repo.Redis.Cli.Exists(todo, key).Result()
		if err != nil {
			logs.Log.Error("Redis.Cli Exists err:", zap.Error(err), zap.String("key", key))

			return "", err
		}
	} else {
		exist, err = d.repo.Redis.ClusterCli.Exists(todo, key).Result()
		if err != nil {
			logs.Log.Error("Redis.ClusterCli Exists err:", zap.Error(err), zap.String("key", key))

			return "", err
		}
	}

	if exist == 0 {
		// 不存在
		if d.repo.Redis.Cli != nil {
			err = d.repo.Redis.Cli.Set(todo, key, AccountIdBegin, 0).Err()
		} else {
			err = d.repo.Redis.ClusterCli.Set(todo, key, AccountIdBegin, 0).Err()
		}

		if err != nil {
			logs.Log.Error("Redis.Cli.Set err:", zap.Error(err), zap.String("key", key))

			return "", err
		}
	}

	var id int64
	if d.repo.Redis.Cli != nil {
		id, err = d.repo.Redis.Cli.Incr(todo, key).Result()
	} else {
		id, err = d.repo.Redis.ClusterCli.Incr(todo, key).Result()
	}

	if err != nil {
		logs.Log.Error("Redis Incr err:", zap.Error(err), zap.String("key", key))

		return "", err
	}

	return fmt.Sprintf("%d", id), nil
}

func NewRedisDao(m *repo.Manager) *RedisDao {
	return &RedisDao{
		repo: m,
	}
}

func (d *RedisDao) Store(ctx context.Context, key string, value string) error {
	var err error
	if d.repo.Redis.Cli != nil {
		_, err = d.repo.Redis.Cli.Set(ctx, key, value, 0).Result()
	} else {
		_, err = d.repo.Redis.ClusterCli.Set(ctx, key, value, 0).Result()
	}
	return err
}
func (d *RedisDao) Get(ctx context.Context, key string) (string, error) {
	var err error
	var value string
	if d.repo.Redis.Cli != nil {
		value, err = d.repo.Redis.Cli.Get(ctx, key).Result()
	} else {
		value, err = d.repo.Redis.ClusterCli.Get(ctx, key).Result()

	}
	if errors.Is(err, redis.Nil) {
		return "", nil
	}
	return value, err
}

func (d *RedisDao) Delete(ctx context.Context, key string) error {
	return d.repo.Redis.Cli.Del(ctx, key).Err()
}
