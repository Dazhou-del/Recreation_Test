package service

import (
	"common/biz"
	"common/logs"
	"context"
	"core/dao"
	"core/models/entity"
	"core/models/requests"
	"core/repo"
	"framework/msError"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap"
	"time"
	"user/pb"
)

//创建账号

type AccountService struct {
	accountDao *dao.AccountDao
	redisDao   *dao.RedisDao
	pb.UnimplementedUserServiceServer
}

func NewAccountService(manager *repo.Manager) *AccountService {
	return &AccountService{
		accountDao: dao.NewAccountDao(manager),
		redisDao:   dao.NewRedisDao(manager),
	}
}

var Tracer = otel.Tracer("api2")

func (a *AccountService) Register(ctx context.Context, req *pb.RegisterParams) (*pb.RegisterResponse, error) {
	// 写注册的业务逻辑
	if req.LoginPlatform == requests.WeiXin {
		ac, err := a.wxRegister(ctx, req)
		if err != nil {
			return &pb.RegisterResponse{}, msError.GrpcError(err)
		}

		return &pb.RegisterResponse{
			Uid:      ac.Uid,
			RoleList: ac.RoleList,
		}, nil
	}

	return &pb.RegisterResponse{}, nil
}

func (a *AccountService) wxRegister(ctx context.Context, req *pb.RegisterParams) (*entity.Account, *msError.Error) {
	// 封装一个account结构 将其存入数据库  mongo 分布式id objectID
	ac := &entity.Account{
		WxAccount:  req.Account,
		CreateTime: time.Now(),
	}

	// 需要生成几个数字做为用户的唯一id  redis自增
	uid, err := a.redisDao.NextAccountId()
	if err != nil {
		logs.Log.WithContext(ctx).Error("NextAccountId failed err:", zap.Error(err))

		return ac, biz.SqlError
	}

	ac.Uid = uid
	err = a.accountDao.SaveAccount(ctx, ac)
	if err != nil {
		logs.Log.WithContext(ctx).Error("SaveAccount failed err:", zap.Error(err))

		return ac, biz.SqlError
	}

	return ac, nil
}
