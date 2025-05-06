package nacos

// https://github.com/yuwf/gobase

import (
	"errors"
	"sync/atomic"

	"github.com/nacos-group/nacos-sdk-go/v2/vo"
	"github.com/rs/zerolog/log"
)

type RegistryInfo struct {
	InstanceId  string            `json:"instanceId,omitempty"` //实例ID 组成方式 "ip#port#clusterName#groupName@@serviceName"
	Ip          string            `json:"ip,omitempty"`
	Port        int               `json:"port,omitempty"`
	Metadata    map[string]string `json:"metadata,omitempty"`
	ServiceName string            `json:"serviceName,omitempty"` // nacos返回的ServiceName是 groupName@@serviceName， 不是注册时的serviceName，
	GroupName   string            `json:"groupName,omitempty"`
	ClusterName string            `json:"clusterName,omitempty"`
}

// RegistryConfig 服务注册配置
type RegistryConfig struct {
	Ip          string            `json:"ip,omitempty"`          //required
	Port        int               `json:"port,omitempty"`        //required
	Metadata    map[string]string `json:"metadata,omitempty"`    //optional
	ServiceName string            `json:"serviceName,omitempty"` //required 同一个程序应该叫一个名字，存在多个实例
	GroupName   string            `json:"groupName,omitempty"`   //optional 不同的项目直接用组隔离
	ClusterName string            `json:"clusterName,omitempty"` //optional // 集群隔离，对应不同的人都连接相同Nacos时，可以起不同的名字
}

type Register struct {
	c     *Client
	conf  RegistryConfig
	state int32 // 注册状态 原子操作 0：未注册 1：注册中 2：已注册 3:取消注册中
}

func (c *Client) CreateRegister(conf *RegistryConfig) (*Register, error) {
	register := &Register{
		c:     c,
		conf:  *conf, // 配置拷贝一份 防止被外部修改
		state: 0,
	}
	register.conf.ServiceName = c.SanitizeString(conf.ServiceName)
	register.conf.ClusterName = c.SanitizeString(conf.ClusterName)

	log.Info().Str("GroupName", conf.GroupName).Str("ServiceName", conf.ServiceName).Msg("Nacos CreateRegister success")
	return register, nil
}

func (r *Register) Reg() error {
	if !atomic.CompareAndSwapInt32(&r.state, 0, 1) {
		log.Error().Str("GroupName", r.conf.GroupName).Str("ServiceName", r.conf.ServiceName).Msg("Nacos already register")
		return nil
	}
	log.Debug().Str("GroupName", r.conf.GroupName).Str("ServiceName", r.conf.ServiceName).Msg("Nacos registering")

	regConf := vo.RegisterInstanceParam{
		Ip:          r.conf.Ip,
		Port:        uint64(r.conf.Port),
		Weight:      1,
		Enable:      true,
		Healthy:     true,
		Metadata:    r.conf.Metadata,
		ClusterName: r.conf.ClusterName,
		ServiceName: r.conf.ServiceName,
		GroupName:   r.conf.GroupName,
		Ephemeral:   true,
		/*该字段表示注册的实例是否是临时实例还是持久化实例
		如果是临时实例，则不会在 Nacos 服务端持久化存储，需要通过上报心跳的方式进行包活
		如果一段时间内没有上报心跳，则会被 Nacos 服务端摘除
		在被摘除后如果又开始上报心跳，则会重新将这个实例注册
		持久化实例则会持久化被 Nacos 服务端，此时即使注册实例的客户端进程不在
		这个实例也不会从服务端删除，只会将健康状态设为不健康*/
	}
	success, err := r.c.nacosNamingCli.RegisterInstance(regConf)
	if err != nil {
		log.Error().Err(err).Msg("Nacos RegisterInstance error")
		return err
	}
	if !success {
		log.Error().Msg("Nacos RegisterInstance error")
		return errors.New("fail")
	}
	atomic.StoreInt32(&r.state, 2)

	log.Info().Str("GroupName", r.conf.GroupName).Str("ServiceName", r.conf.ServiceName).Msg("Nacos register success")
	return nil
}

func (r *Register) DeReg() error {
	if !atomic.CompareAndSwapInt32(&r.state, 2, 3) {
		log.Error().Str("GroupName", r.conf.GroupName).Str("ServiceName", r.conf.ServiceName).Msg("Nacos not register")
		return nil
	}

	regConf := vo.DeregisterInstanceParam{
		Ip:          r.conf.Ip,
		Port:        uint64(r.conf.Port),
		Cluster:     r.conf.ClusterName,
		ServiceName: r.conf.ServiceName,
		GroupName:   r.conf.GroupName,
		Ephemeral:   true,
	}

	success, err := r.c.nacosNamingCli.DeregisterInstance(regConf)
	if err != nil {
		atomic.StoreInt32(&r.state, 2)
		log.Error().Err(err).Msg("Nacos DeregisterInstance error")
		return err
	}
	if !success {
		atomic.StoreInt32(&r.state, 2)
		log.Error().Msg("Nacos DeregisterInstance error")
		return errors.New("fail")
	}

	atomic.StoreInt32(&r.state, 0)
	log.Info().Str("GroupName", r.conf.GroupName).Str("ServiceName", r.conf.ServiceName).Msgf("Nacos deregistered")
	return nil
}
