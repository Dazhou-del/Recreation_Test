package main

import (
	"common/config"
	metrics "common/metircs"
	"context"
	"flag"
	"fmt"
	"github.com/kardianos/service"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
	"user/app"
)

var configFile = flag.String("config", "application.yml", "config file")

type program struct {
	//server    *gin.Engine
	svc       service.Service
	once      sync.Once
	clearFunc func()
}

// Start 方法在服务启动时调用
func (p *program) Start(s service.Service) error {
	// 在一个新的 goroutine 中启动服务
	go p.run()
	return nil
}

// Stop 方法在服务停止时调用
func (p *program) Stop(s service.Service) error {
	// 这里可以添加清理资源的代码
	p.once.Do(func() {
		if p.clearFunc != nil {
			p.clearFunc()
		}
	})
	return nil
}

// 运行 Gin 服务器
func (p *program) run() {
	// 加载配置
	flag.Parse()
	config.InitConfig(*configFile)

	// 启动监控
	go func() {
		err := metrics.Serve(fmt.Sprintf("0.0.0.0:%d", config.Conf.MetricPort))
		if err != nil {
			panic(err)
		}
	}()

	go func() {
		newApp()
	}()
}

func main() {
	// 定义服务配置
	svcConfig := &service.Config{
		Name:        "recreation_test",
		DisplayName: "casual game testing",
		Description: "casual game testing",
	}

	prg := &program{}
	s, err := service.New(prg, svcConfig)
	if err != nil {
		log.Fatal(err)
	}
	prg.svc = s

	// 设置日志
	errs := make(chan error)
	logger, err := s.Logger(errs)
	if err != nil {
		log.Fatal(err)
	}

	// 启动服务
	if err := s.Run(); err != nil {
		err := logger.Error(err)
		if err != nil {
			log.Println("run Error:", err)

			return
		}
	}

	// 处理错误
	go func() {
		for err := range errs {
			log.Println("Error:", err)
		}
	}()

	// 等待中断信号以优雅关闭
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	<-sigs
	log.Println("Shutting down... time:", time.Now())
}

func newApp() {
	// 启动grpc服务端
	err := app.Run(context.Background())
	if err != nil {
		log.Println(err)
		os.Exit(-1)
	}
}
