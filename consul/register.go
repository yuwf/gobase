package consul

// https://github.com/yuwf/gobase

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"reflect"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/hashicorp/consul/api"
	"github.com/rs/zerolog/log"
)

// RegistryInfo 服务注册信息
type RegistryInfo struct {
	RegistryName string            `json:"registryname,omitempty"` // 注册的名字 组名
	RegistryID   string            `json:"registryid,omitempty"`   // 注册的ID
	RegistryAddr string            `json:"registryaddr,omitempty"` // 服务器对外暴露的地址
	RegistryPort int               `json:"registryport,omitempty"` // 服务器对外暴露的端口
	RegistryTag  []string          `json:"registrytag,omitempty"`  // 注册的Tag
	RegistryMeta map[string]string `json:"registrymeta,omitempty"` // 注册的Meta，单个Value长度限制是512个字符
}

// RegistryConfig 服务注册配置
type RegistryConfig struct {
	RegistryName string            `json:"registryname,omitempty"` // 注册的名字 组名
	RegistryID   string            `json:"registryid,omitempty"`   // 注册的ID
	RegistryAddr string            `json:"registryaddr,omitempty"` // 服务器对外暴露的地址
	RegistryPort int               `json:"registryport,omitempty"` // 服务器对外暴露的端口
	RegistryTag  []string          `json:"registrytag,omitempty"`  // 注册的Tag
	RegistryMeta map[string]string `json:"registrymeta,omitempty"` // 注册的Meta，单个Value长度限制是512个字符

	HealthPort     int    `json:"healthport,omitempty"`     // 健康检查端口
	HealthPath     string `json:"healthpath,omitempty"`     // 健康检查路径 以/开头
	HealthInterval int    `json:"healthinterval,omitempty"` // 健康检查间隔 单位秒
	HealthTimeout  int    `json:"healthtimeout,omitempty"`  // 健康检查超时时间 单位秒
	DeregisterTime int    `json:"deregistertime,omitempty"` // 健康检查失败后 多长时间自动取消注册 貌似最小值是60s 单位秒
}

// Register consul注册对象
type Register struct {
	c                  *Client
	cfg                RegistryConfig
	consulRegistration *api.AgentServiceRegistration
	state              int32 // 注册状态 原子操作 0：未注册 1：注册中 2：已注册
	// 退出检查使用
	quit chan int
}

// CreateRegister 创建注册对象
func (c *Client) CreateRegister(cfg *RegistryConfig) (*Register, error) {
	// 一些默认参数
	interval := 4
	if cfg.HealthInterval > 0 {
		interval = cfg.HealthInterval
	}
	timeout := 4
	if cfg.HealthTimeout > 0 {
		timeout = cfg.HealthTimeout
	}
	deregister := 4
	if cfg.DeregisterTime > 0 {
		deregister = cfg.DeregisterTime
	}

	r := &api.AgentServiceRegistration{
		ID:      cfg.RegistryID,
		Name:    cfg.RegistryName,
		Address: cfg.RegistryAddr,
		Port:    cfg.RegistryPort,
		Tags:    cfg.RegistryTag,
		Meta:    cfg.RegistryMeta,
		Check: &api.AgentServiceCheck{
			HTTP:                           fmt.Sprintf("http://%s:%d%s", cfg.RegistryAddr, cfg.HealthPort, cfg.HealthPath),
			Interval:                       (time.Duration(interval) * time.Second).String(),
			Timeout:                        (time.Duration(timeout) * time.Second).String(),
			TLSSkipVerify:                  true,
			DeregisterCriticalServiceAfter: (time.Duration(deregister) * time.Second).String(),
		},
	}
	register := &Register{
		c:                  c,
		cfg:                *cfg, // 配置拷贝一份 防止被外部修改
		consulRegistration: r,
		state:              0,
		quit:               make(chan int),
	}
	log.Info().Msg("Consul CreateRegister success")
	return register, nil
}

// Reg 向配置中心注册
// 默认注册会自动开启健康检查的端口监听
// 若RegistryMeta中有 "healthListenNo":"yes" 配置将不会开启健康检查的端口监听
// 若RegistryMeta中有 "healthListenReuse":"yes" 开启健康检查的端口监听采用复用的方式
func (r *Register) Reg() error {
	if !atomic.CompareAndSwapInt32(&r.state, 0, 1) {
		log.Error().Str("RegistryName", r.cfg.RegistryName).Str("RegistryID", r.cfg.RegistryID).Msg("Consul already register")
		return nil
	}
	log.Debug().Str("RegistryName", r.cfg.RegistryName).Str("RegistryID", r.cfg.RegistryID).Msg("Consul registering")

	// 开启健康检查监听
	healthAddr := fmt.Sprintf("%s:%d", r.cfg.RegistryAddr, r.cfg.HealthPort)
	healthListenNo, ok := r.cfg.RegistryMeta["healthListenNo"]
	var healthListener net.Listener // 健康检查监听对象
	var err error
	if ok && healthListenNo == "yes" {
		// 要求不开启健康检查的端口监听
	} else {
		healthListenReuse, ok := r.cfg.RegistryMeta["healthListenReuse"]
		var control func(network, address string, c syscall.RawConn) error
		if ok && healthListenReuse == "yes" {
			control = reusePortControl
		}
		l := &net.ListenConfig{Control: control}
		healthListener, err = l.Listen(context.Background(), "tcp", healthAddr)
		if err != nil || healthListener == nil {
			log.Error().Err(err).Str("HealthAddr", healthAddr).Msg("Consul CreateRegister error")
			return err
		}
		go func() {
			mux := http.NewServeMux()
			mux.HandleFunc(r.cfg.HealthPath, func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
			})
			http.Serve(healthListener, mux)
			log.Info().Msg("Consul health check service start")
		}()
	}

	// 注册
	err = r.c.consulCli.Agent().ServiceRegister(r.consulRegistration)
	if err != nil {
		log.Error().Err(err).Str("RegistryName", r.cfg.RegistryName).Str("RegistryID", r.cfg.RegistryID).Msg("Consul Cannot register")
		if healthListener != nil {
			healthListener.Close()
		}
		atomic.StoreInt32(&r.state, 0)
		return err
	}

	// 开启协程自查
	go r.loop(healthListener)
	atomic.StoreInt32(&r.state, 2)

	log.Info().Str("RegistryName", r.cfg.RegistryName).Str("RegistryID", r.cfg.RegistryID).Msg("Consul register success")
	return nil
}

func reusePortControl(network, address string, c syscall.RawConn) error {
	var opErr error
	err := c.Control(func(fd uintptr) {
		funType := reflect.TypeOf(syscall.SetsockoptInt)
		funValue := reflect.ValueOf(syscall.SetsockoptInt)
		v := reflect.New(funType.In(0))
		if v.Elem().Type().Kind() == reflect.Uintptr {
			v.Elem().SetUint(uint64(fd))
		} else if v.Elem().Type().Kind() == reflect.Int {
			v.Elem().SetInt(int64(fd))
		} else {
			log.Error().Str("kind", v.Elem().Type().Kind().String()).Msg("Consul control error")
			return
		}
		r := funValue.Call([]reflect.Value{v.Elem(), reflect.ValueOf(syscall.SOL_SOCKET), reflect.ValueOf(syscall.SO_REUSEADDR), reflect.ValueOf(1)})
		if r[0].Interface() != nil {
			opErr = r[0].Interface().(error)
			log.Error().Err(opErr).Msg("Consul control error")
		}
	})
	if err != nil {
		return err
	}
	return opErr
}

// DeReg 注销
func (r *Register) DeReg() error {
	if !atomic.CompareAndSwapInt32(&r.state, 2, 0) {
		log.Error().Str("RegistryName", r.cfg.RegistryName).Str("RegistryID", r.cfg.RegistryID).Msg("Consul not register")
		return nil
	}

	r.quit <- 1
	<-r.quit

	log.Info().Str("RegistryName", r.cfg.RegistryName).Str("RegistryID", r.cfg.RegistryID).Msgf("Consul deregistered")
	return nil
}

func (r *Register) loop(healthListener net.Listener) {
	// 定时检查是否还存在，不存在重新注册下
	for {
		select {
		case <-time.NewTicker(time.Duration(r.cfg.HealthInterval) * time.Second).C:
			services, err := r.c.consulCli.Agent().Services()
			if err != nil {
				log.Error().Err(err).Msgf("Consul Cannot get service list")
				continue
			}
			_, ok := services[r.cfg.RegistryID]
			if !ok {
				err := r.c.consulCli.Agent().ServiceRegister(r.consulRegistration)
				if err != nil {
					log.Error().Err(err).Str("RegistryName", r.cfg.RegistryName).Str("RegistryID", r.cfg.RegistryID).Msg("Consul Cannot register")
				}
			}
		case <-r.quit:
			err := r.c.consulCli.Agent().ServiceDeregister(r.cfg.RegistryID)
			if err != nil {
				log.Error().Str("RegistryName", r.cfg.RegistryName).Str("RegistryID", r.cfg.RegistryID).Msgf("Consul didn't deregister")
			}
			if healthListener != nil {
				healthListener.Close()
			}
			r.quit <- 1
			return
		}
	}
}
