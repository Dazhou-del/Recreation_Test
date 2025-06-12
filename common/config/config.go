package config

import (
	"fmt"
	"github.com/fsnotify/fsnotify"
	"github.com/spf13/viper"
	"log"
)

var Conf *Config

type Config struct {
	Log        LogConf                 `mapstructure:"log" json:"log"`
	Port       int                     `mapstructure:"port" json:"port"`
	WsPort     int                     `mapstructure:"wsPort" json:"wsPort"`
	MetricPort int                     `mapstructure:"metricPort" json:"metricPort"`
	HttpPort   int                     `mapstructure:"httpPort" json:"httpPort"`
	AppName    string                  `mapstructure:"appName" json:"appName"`
	Database   Database                `mapstructure:"db" json:"db"`
	Jwt        JwtConf                 `mapstructure:"jwt" json:"jwt"`
	Grpc       GrpcConf                `mapstructure:"grpc" json:"grpc"`
	Etcd       EtcdConf                `mapstructure:"etcd" json:"etcd"`
	Domain     map[string]Domain       `mapstructure:"domain" json:"domain"`
	Services   map[string]ServicesConf `mapstructure:"services" json:"services"`
}

type ServicesConf struct {
	Id         string `mapstructure:"id" json:"id"`
	ClientHost string `mapstructure:"clientHost" json:"clientHost"`
	ClientPort int    `mapstructure:"clientPort" json:"clientPort"`
}

type Domain struct {
	Name        string `mapstructure:"name" json:"name"`
	LoadBalance bool   `mapstructure:"loadBalance" json:"loadBalance"`
}

type JwtConf struct {
	Secret string `mapstructure:"secret" json:"secret"`
	Exp    int64  `mapstructure:"exp" json:"exp"`
}

type LogConf struct {
	Level string `mapstructure:"level" json:"level"`
}

// Database 数据库配置
type Database struct {
	MongoConf MongoConf `mapstructure:"mongo" json:"mongo"`
	RedisConf RedisConf `mapstructure:"redis" json:"redis"`
}

type MongoConf struct {
	Url         string `mapstructure:"url" json:"url"`
	Db          string `mapstructure:"db" json:"db"`
	UserName    string `mapstructure:"userName" json:"userName"`
	Password    string `mapstructure:"password" json:"password"`
	MinPoolSize int    `mapstructure:"minPoolSize" json:"minPoolSize"`
	MaxPoolSize int    `mapstructure:"maxPoolSize" json:"maxPoolSize"`
}

type RedisConf struct {
	Addr            string   `mapstructure:"addr" json:"addr"`
	ClusterAddrList []string `mapstructure:"clusterAddrList" json:"clusterAddrList"`
	Password        string   `mapstructure:"password" json:"password"`
	PoolSize        int      `mapstructure:"poolSize" json:"poolSize"`
	MinIdleConnList int      `mapstructure:"minIdleConnList" json:"minIdleConnList"`
	Host            string   `mapstructure:"host" json:"host"`
	Port            int      `mapstructure:"port" json:"port"`
}

type EtcdConf struct {
	AddrList    []string       `mapstructure:"addrList" json:"addrList"`
	RWTimeout   int            `mapstructure:"rwTimeout" json:"rwTimeout"`
	DialTimeout int            `mapstructure:"dialTimeout" json:"dialTimeout"`
	Register    RegisterServer `mapstructure:"register" json:"register"`
}

type RegisterServer struct {
	Addr    string `mapstructure:"addr" json:"addr"`
	Name    string `mapstructure:"name" json:"name"`
	Version string `mapstructure:"version" json:"version"`
	Weight  int    `mapstructure:"weight" json:"weight"`
	Ttl     int64  `mapstructure:"ttl" json:"ttl"` // 租约时长
}

type GrpcConf struct {
	Addr string `mapstructure:"addr" json:"addr"`
}

// InitConfig 加载配置
func InitConfig(confFile string) {
	Conf = new(Config)
	v := viper.New()
	v.SetConfigFile(confFile)
	v.WatchConfig()
	v.OnConfigChange(func(in fsnotify.Event) {
		log.Println("配置文件被修改了")
		err := v.Unmarshal(&Conf)
		if err != nil {
			panic(fmt.Errorf("Unmarshal change config data,err:%v \n", err))
		}
	})

	err := v.ReadInConfig()
	if err != nil {
		panic(fmt.Errorf("读取配置文件出错,err:%v \n", err))
	}

	//解析
	err = v.Unmarshal(&Conf)
	if err != nil {
		panic(fmt.Errorf("Unmarshal config data,err:%v \n", err))
	}
}
