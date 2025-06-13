package service

import (
	"common/biz"
	"context"
	"core/dao"
	"core/models/entity"
	"core/models/requests"
	"core/repo"
	"framework/msError"
	"time"
	"user/pb"
)

//创建账号

type AccountService struct {
	accountDao *dao.AccountDao
	redisDao   *dao.RedisDao
	pb.UnimplementedUserServiceServer
	ctx context.Context
}

func NewAccountService(manager *repo.Manager, ctx context.Context) *AccountService {
	return &AccountService{
		accountDao: dao.NewAccountDao(manager),
		redisDao:   dao.NewRedisDao(manager),
		ctx:        ctx,
	}
}

func (a *AccountService) Register(ctx context.Context, req *pb.RegisterParams) (*pb.RegisterResponse, error) {
	// 写注册的业务逻辑
	if req.LoginPlatform == requests.WeiXin {
		ac, err := a.wxRegister(req)
		if err != nil {
			return &pb.RegisterResponse{}, msError.GrpcError(err)
		}

		return &pb.RegisterResponse{
			Uid: ac.Uid,
		}, nil
	}

	return &pb.RegisterResponse{}, nil
}

func (a *AccountService) wxRegister(req *pb.RegisterParams) (*entity.Account, *msError.Error) {
	// 封装一个account结构 将其存入数据库  mongo 分布式id objectID
	ac := &entity.Account{
		WxAccount:  req.Account,
		CreateTime: time.Now(),
	}

	// 需要生成几个数字做为用户的唯一id  redis自增
	uid, err := a.redisDao.NextAccountId()
	if err != nil {
		return ac, biz.SqlError
	}

	ac.Uid = uid
	err = a.accountDao.SaveAccount(a.ctx, ac)

	if err != nil {
		return ac, biz.SqlError
	}

	return ac, nil
}
