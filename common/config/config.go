package config

import (
	"fmt"
	"github.com/fsnotify/fsnotify"
	"github.com/spf13/viper"
	"log"
)

var Conf *Config

type Config struct {
	Log        Zap                     `mapstructure:"log" json:"log"`
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
	Cors       CORS                    `mapstructure:"cors" json:"cors"` // 跨越配置
	Server     ServerConf              `mapstructure:"server" json:"server"`
}
type ServerConf struct {
	MaxConn int `mapstructure:"maxConn" json:"maxConn"`
}

type CORS struct {
	Mode      string          `mapstructure:"mode" json:"mode" yaml:"mode"`
	Whitelist []CORSWhitelist `mapstructure:"whitelist" json:"whitelist" yaml:"whitelist"`
}

type CORSWhitelist struct {
	AllowOrigin      string `mapstructure:"allow-origin" json:"allow-origin" yaml:"allow-origin"`
	AllowMethods     string `mapstructure:"allow-methods" json:"allow-methods" yaml:"allow-methods"`
	AllowHeaders     string `mapstructure:"allow-headers" json:"allow-headers" yaml:"allow-headers"`
	ExposeHeaders    string `mapstructure:"expose-headers" json:"expose-headers" yaml:"expose-headers"`
	AllowCredentials bool   `mapstructure:"allow-credentials" json:"allow-credentials" yaml:"allow-credentials"`
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

type Zap struct {
	Level         string `mapstructure:"level" json:"level" yaml:"level"`                            // 级别
	Prefix        string `mapstructure:"prefix" json:"prefix" yaml:"prefix"`                         // 日志前缀
	Format        string `mapstructure:"format" json:"format" yaml:"format"`                         // 输出
	Director      string `mapstructure:"director" json:"director"  yaml:"director"`                  // 日志文件夹
	EncodeLevel   string `mapstructure:"encode-level" json:"encode-level" yaml:"encode-level"`       // 编码级
	StacktraceKey string `mapstructure:"stacktrace-key" json:"stacktrace-key" yaml:"stacktrace-key"` // 栈名
	ShowLine      bool   `mapstructure:"show-line" json:"show-line" yaml:"show-line"`                // 显示行
	LogInConsole  bool   `mapstructure:"log-in-console" json:"log-in-console" yaml:"log-in-console"` // 输出控制台
	RetentionDay  int    `mapstructure:"retention-day" json:"retention-day" yaml:"retention-day"`    // 日志保留天数
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
	Addr    string `mapstructure:"addr" json:"addr"`       // 地址
	Name    string `mapstructure:"name" json:"name"`       // 名称
	Version string `mapstructure:"version" json:"version"` // 版本
	Weight  int    `mapstructure:"weight" json:"weight"`   // 权重
	Ttl     int64  `mapstructure:"ttl" json:"ttl"`         // 租约时长
}

type GrpcConf struct {
	Addr         string `mapstructure:"addr" json:"addr"`                 // 地址
	ClientId     string `mapstructure:"clientId" json:"clientId"`         // 客服端ID(用于服务端校验)
	ClientSecret string `mapstructure:"clientSecret" json:"clientSecret"` // 客户端密钥(用于服务端校验)
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
			panic(fmt.Errorf("failed to unmarshal changed config data: %v", err))
		}
	})

	err := v.ReadInConfig()
	if err != nil {
		panic(fmt.Errorf("读取配置文件出错,err:%v", err))
	}

	//解析
	err = v.Unmarshal(&Conf)
	if err != nil {
		panic(fmt.Errorf("unmarshal config data,err:%v", err))
	}
}
