package nacos

// https://github.com/yuwf/gobase

import (
	"errors"
	"fmt"
	"sync"

	"github.com/nacos-group/nacos-sdk-go/v2/clients"
	"github.com/nacos-group/nacos-sdk-go/v2/clients/naming_client"
	"github.com/nacos-group/nacos-sdk-go/v2/common/constant"
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

// 每个Register对象会创建一个naming_client.INamingClient对象，见Client中nacosNamingCli的说明
// Close后naming_client.INamingClient对象有goruntine泄露，如果需要频繁的创建Register和Close会有问题
type Register struct {
	mu             sync.Mutex
	c              *Client
	conf           RegistryConfig
	ins            string
	nacosNamingCli naming_client.INamingClient
}

func (c *Client) CreateRegister(conf *RegistryConfig) (*Register, error) {
	ins := fmt.Sprintf("%s#%d#%s", conf.Ip, conf.Port, conf.ClusterName)
	cNaming, err := clients.CreateNamingClient(map[string]interface{}{
		constant.KEY_SERVER_CONFIGS: c.serverConfigs,
		constant.KEY_CLIENT_CONFIG:  c.clientConfig,
	})
	if err != nil {
		log.Error().Err(err).Str("GroupName", conf.GroupName).Str("ServiceName", conf.ServiceName).Str("Instance", ins).Msg("Nacos Reg CreateNamingClient error")
		return nil, err
	}

	register := &Register{
		c:              c,
		conf:           *conf, // 配置拷贝一份 防止被外部修改
		ins:            ins,
		nacosNamingCli: cNaming,
	}
	register.conf.ServiceName = c.SanitizeString(conf.ServiceName)
	register.conf.ClusterName = c.SanitizeString(conf.ClusterName)

	log.Info().Str("GroupName", conf.GroupName).Str("ServiceName", conf.ServiceName).Msg("Nacos CreateRegister success")
	return register, nil
}

func (r *Register) Reg() error {
	if r.nacosNamingCli == nil {
		err := errors.New("closed")
		log.Error().Err(err).Str("GroupName", r.conf.GroupName).Str("ServiceName", r.conf.ServiceName).Str("Instance", r.ins).Msg("Nacos Reg error")
		return nil // 不返回错误
	}

	log.Debug().Str("GroupName", r.conf.GroupName).Str("ServiceName", r.conf.ServiceName).Str("Instance", r.ins).Msg("Nacos Reging")

	// 加锁
	r.mu.Lock()
	defer r.mu.Unlock()

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
	success, err := r.nacosNamingCli.RegisterInstance(regConf)
	if err != nil {
		log.Error().Err(err).Str("GroupName", r.conf.GroupName).Str("ServiceName", r.conf.ServiceName).Str("Instance", r.ins).Msg("Nacos Reg RegisterInstance error")
		return err
	}
	if !success {
		log.Error().Str("GroupName", r.conf.GroupName).Str("ServiceName", r.conf.ServiceName).Str("Instance", r.ins).Msg("Nacos Reg RegisterInstance error")
		return errors.New("fail")
	}

	log.Info().Str("GroupName", r.conf.GroupName).Str("ServiceName", r.conf.ServiceName).Str("Instance", r.ins).Msg("Nacos Reg success")
	return nil
}

func (r *Register) DeReg() error {
	if r.nacosNamingCli == nil {
		err := errors.New("closed")
		log.Error().Err(err).Str("GroupName", r.conf.GroupName).Str("ServiceName", r.conf.ServiceName).Str("Instance", r.ins).Msg("Nacos DeReg error")
		return nil // 不返回错误
	}

	log.Debug().Str("GroupName", r.conf.GroupName).Str("ServiceName", r.conf.ServiceName).Str("Instance", r.ins).Msg("Nacos DeReging")
	// 加锁
	r.mu.Lock()
	defer r.mu.Unlock()

	regConf := vo.DeregisterInstanceParam{
		Ip:          r.conf.Ip,
		Port:        uint64(r.conf.Port),
		Cluster:     r.conf.ClusterName,
		ServiceName: r.conf.ServiceName,
		GroupName:   r.conf.GroupName,
		Ephemeral:   true,
	}

	success, err := r.nacosNamingCli.DeregisterInstance(regConf)
	if err != nil {
		log.Error().Str("GroupName", r.conf.GroupName).Str("ServiceName", r.conf.ServiceName).Str("Instance", r.ins).Err(err).Msg("Nacos DeReg DeregisterInstance error")
		return err
	}
	if !success {
		log.Error().Str("GroupName", r.conf.GroupName).Str("ServiceName", r.conf.ServiceName).Str("Instance", r.ins).Msg("Nacos DeReg DeregisterInstance error")
		return errors.New("fail")
	}

	log.Info().Str("GroupName", r.conf.GroupName).Str("ServiceName", r.conf.ServiceName).Str("Instance", r.ins).Msg("Nacos DeReg success")
	return nil
}

// 注意Close后，不能在调用上面的函数
func (r *Register) Close() {
	// 加锁
	r.mu.Lock()
	defer r.mu.Unlock()

	log.Info().Str("GroupName", r.conf.GroupName).Str("ServiceName", r.conf.ServiceName).Str("Instance", r.ins).Msg("Nacos Close")

	t := r.nacosNamingCli
	r.nacosNamingCli = nil
	t.CloseClient() // 经测试里面有goruntine泄露
}
