package mynats

import (
	"fmt"
	"time"

	"github.com/alecthomas/log4go"
	"github.com/nats-io/nats.go"
)

// Config 配置
type Config struct {
	Cluster       string `xml:"cluster"`        // 集群名字,为了同一个nats-server各个集群下互相不影响
	Server        string `xml:"server"`         // nats://127.0.0.1:4222,nats://127.0.0.1:4223
	User          string `xml:"user"`           // 用户名
	Pwd           string `xml:"pwd"`            //密码
	ReconnectWait int64  `xml:"reconnect_wait"` // 重连间隔
	MaxReconnects int32  `xml:"max_reconnects"` // 重连次数
}

// SetupConnOptions 设置启动选项
func SetupConnOptions(name string, cfg *Config) []nats.Option {
	opts := make([]nats.Option, 0)
	opts = append(opts, nats.Name(name))
	if len(cfg.User) > 0 {
		opts = append(opts, nats.UserInfo(cfg.User, cfg.Pwd))
	}
	opts = append(opts, nats.ReconnectWait(time.Second*time.Duration(cfg.ReconnectWait)))
	opts = append(opts, nats.MaxReconnects(int(cfg.MaxReconnects)))
	opts = append(opts, nats.ReconnectHandler(func(nc *nats.Conn) {
		log4go.Warn("[main] nats.Reconnected [%s]", nc.ConnectedUrl())
	}))
	opts = append(opts, nats.DiscoveredServersHandler(func(nc *nats.Conn) {
		log4go.Info("[main] nats.DiscoveredServersHandler", nc.DiscoveredServers())
	}))
	opts = append(opts, nats.DisconnectHandler(func(nc *nats.Conn) {
		log4go.Warn("[main] nats.Disconnect")
	}))
	return opts
}

// joinSubject 把subject用.分割组合
func joinSubject(cluster string, typeName string, subjectPostfix ...interface{}) string {
	sub := fmt.Sprintf("%s.%s", cluster, typeName)
	if nil != subjectPostfix {
		for _, v := range subjectPostfix {
			sub += fmt.Sprintf(".%v", v)
		}
	}
	return sub
}
