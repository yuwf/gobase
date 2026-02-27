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

// RegistryConfig 服务注册配置
type RegistrysConfig struct {
	Ip          string            `json:"ip,omitempty"`          //required
	Port        int               `json:"port,omitempty"`        //required
	Metadata    map[string]string `json:"metadata,omitempty"`    //optional
	ClusterName string            `json:"clusterName,omitempty"` //optional // 集群隔离，对应不同的人都连接相同Nacos时，可以起不同的名字
}

// 每个Registers对象会创建一个naming_client.INamingClient对象，见Client中nacosNamingCli的说明
// Close后naming_client.INamingClient对象有goruntine泄露，如果需要频繁的创建Register和Close会有问题
type Registers struct {
	mu             sync.Mutex
	c              *Client
	serviceName    string
	groupName      string
	nacosNamingCli naming_client.INamingClient
	confs          map[string]*RegistrysConfig
}

func (c *Client) CreateRegisters(serviceName, groupName string) (*Registers, error) {
	cNaming, err := clients.CreateNamingClient(map[string]interface{}{
		constant.KEY_SERVER_CONFIGS: c.serverConfigs,
		constant.KEY_CLIENT_CONFIG:  c.clientConfig,
	})
	if err != nil {
		log.Error().Err(err).Str("GroupName", groupName).Str("ServiceName", serviceName).Msg("Nacos CreateRegisters CreateNamingClient error")
		return nil, err
	}

	register := &Registers{
		c:              c,
		serviceName:    c.SanitizeString(serviceName),
		groupName:      groupName,
		nacosNamingCli: cNaming,
		confs:          map[string]*RegistrysConfig{},
	}

	log.Info().Str("GroupName", groupName).Str("ServiceName", serviceName).Msg("Nacos CreateRegisters success")
	return register, nil
}

// conf采用拷贝的方式，防止外层修改
func (r *Registers) Reg(conf RegistrysConfig) error {
	ins := fmt.Sprintf("%s#%d#%s", conf.Ip, conf.Port, conf.ClusterName)
	if r.nacosNamingCli == nil {
		err := errors.New("closed")
		log.Error().Err(err).Str("GroupName", r.groupName).Str("ServiceName", r.serviceName).Str("Instance", ins).Msg("Nacos Reg error")
		return err
	}

	log.Debug().Str("GroupName", r.groupName).Str("ServiceName", r.serviceName).Str("Instance", ins).Msg("Nacos Reging")

	// 加锁
	r.mu.Lock()
	defer r.mu.Unlock()

	params := vo.BatchRegisterInstanceParam{
		ServiceName: r.serviceName,
		GroupName:   r.groupName,
	}
	params.Instances = append(params.Instances, vo.RegisterInstanceParam{
		Ip:          conf.Ip,
		Port:        uint64(conf.Port),
		Weight:      1,
		Enable:      true,
		Healthy:     true,
		Metadata:    conf.Metadata,
		ClusterName: r.c.SanitizeString(conf.ClusterName),
		ServiceName: r.serviceName,
		GroupName:   r.groupName,
		Ephemeral:   true,
	})
	for _, conf := range r.confs {
		params.Instances = append(params.Instances, vo.RegisterInstanceParam{
			Ip:          conf.Ip,
			Port:        uint64(conf.Port),
			Weight:      1,
			Enable:      true,
			Healthy:     true,
			Metadata:    conf.Metadata,
			ClusterName: r.c.SanitizeString(conf.ClusterName),
			ServiceName: r.serviceName,
			GroupName:   r.groupName,
			Ephemeral:   true,
		})
	}
	success, err := r.nacosNamingCli.BatchRegisterInstance(params)
	if err != nil {
		log.Error().Err(err).Str("GroupName", r.groupName).Str("ServiceName", r.serviceName).Str("Instance", ins).Msg("Nacos Reg BatchRegisterInstance error")
		return err
	}
	if !success {
		log.Error().Str("GroupName", r.groupName).Str("ServiceName", r.serviceName).Str("Instance", ins).Msg("Nacos Reg BatchRegisterInstance error")
		return errors.New("fail")
	}
	r.confs[ins] = &conf

	log.Info().Str("GroupName", r.groupName).Str("ServiceName", r.serviceName).Str("Instance", ins).Msg("Nacos Reg success")
	return nil
}

func (r *Registers) DeReg(conf *RegistrysConfig) error {
	ins := fmt.Sprintf("%s#%d#%s", conf.Ip, conf.Port, conf.ClusterName)
	if r.nacosNamingCli == nil {
		err := errors.New("closed")
		log.Error().Err(err).Str("GroupName", r.groupName).Str("ServiceName", r.serviceName).Str("Instance", ins).Msg("Nacos DeReg error")
		return nil // 不返回错误
	}

	// 加锁
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.confs[ins]; !ok {
		err := errors.New("not exist")
		log.Error().Err(err).Str("GroupName", r.groupName).Str("ServiceName", r.serviceName).Str("Instance", ins).Msg("Nacos DeReg error")
		return nil // 不返回错误
	}

	log.Debug().Str("GroupName", r.groupName).Str("ServiceName", r.serviceName).Str("Instance", ins).Msg("Nacos DeReging")

	// 不能使用DeregisterInstance， 他会把所有的注册对象都移除掉
	// BatchRegisterInstance函数中传入的参数又不能为空

	if len(r.confs) == 1 {
		regConf := vo.DeregisterInstanceParam{
			Ip:          conf.Ip,
			Port:        uint64(conf.Port),
			Cluster:     r.c.SanitizeString(conf.ClusterName),
			ServiceName: r.serviceName,
			GroupName:   r.groupName,
			Ephemeral:   true,
		}

		success, err := r.nacosNamingCli.DeregisterInstance(regConf)
		if err != nil {
			log.Error().Err(err).Str("GroupName", r.groupName).Str("ServiceName", r.serviceName).Str("Instance", ins).Msg("Nacos DeReg DeregisterInstance error")
			return err
		}
		if !success {
			log.Error().Str("GroupName", r.groupName).Str("ServiceName", r.serviceName).Str("Instance", ins).Msg("Nacos DeReg DeregisterInstance error")
			return errors.New("fail")
		}
	} else {
		params := vo.BatchRegisterInstanceParam{
			ServiceName: r.serviceName,
			GroupName:   r.groupName,
		}
		for key, conf := range r.confs {
			if key == ins {
				continue
			}
			params.Instances = append(params.Instances, vo.RegisterInstanceParam{
				Ip:          conf.Ip,
				Port:        uint64(conf.Port),
				Weight:      1,
				Enable:      true,
				Healthy:     true,
				Metadata:    conf.Metadata,
				ClusterName: r.c.SanitizeString(conf.ClusterName),
				ServiceName: r.serviceName,
				GroupName:   r.groupName,
				Ephemeral:   true,
			})
		}
		success, err := r.nacosNamingCli.BatchRegisterInstance(params)
		if err != nil {
			log.Error().Err(err).Str("GroupName", r.groupName).Str("ServiceName", r.serviceName).Str("Instance", ins).Msg("Nacos DeReg BatchRegisterInstance error")
			return err
		}
		if !success {
			log.Error().Str("GroupName", r.groupName).Str("ServiceName", r.serviceName).Str("Instance", ins).Msg("Nacos DeReg BatchRegisterInstance error")
			return errors.New("fail")
		}
	}
	// 删除对象
	delete(r.confs, ins)

	log.Info().Str("GroupName", r.groupName).Str("ServiceName", r.serviceName).Str("Instance", ins).Msg("Nacos DeReg success")
	return nil
}

func (r *Registers) DeRegAll() error {
	if r.nacosNamingCli == nil {
		err := errors.New("closed")
		log.Error().Err(err).Str("GroupName", r.groupName).Str("ServiceName", r.serviceName).Msg("Nacos DeRegAll error")
		return nil // 不返回错误
	}

	// 加锁
	r.mu.Lock()
	defer r.mu.Unlock()

	if len(r.confs) == 0 {
		return nil
	}

	log.Debug().Str("GroupName", r.groupName).Str("ServiceName", r.serviceName).Msg("Nacos DeRegAlling")

	for _, conf := range r.confs {
		regConf := vo.DeregisterInstanceParam{
			Ip:          conf.Ip,
			Port:        uint64(conf.Port),
			Cluster:     r.c.SanitizeString(conf.ClusterName),
			ServiceName: r.serviceName,
			GroupName:   r.groupName,
			Ephemeral:   true,
		}

		ins := fmt.Sprintf("%s#%d#%s", conf.Ip, conf.Port, conf.ClusterName)
		success, err := r.nacosNamingCli.DeregisterInstance(regConf)
		// 只报警 不返回错误
		if err != nil {
			log.Error().Err(err).Str("GroupName", r.groupName).Str("ServiceName", r.serviceName).Str("Instance", ins).Msg("Nacos DeRegAll DeregisterInstance error")
		}
		if !success {
			log.Error().Str("GroupName", r.groupName).Str("ServiceName", r.serviceName).Str("Instance", ins).Msg("Nacos DeRegAll DeregisterInstance error")
		}
	}

	r.confs = map[string]*RegistrysConfig{}

	log.Info().Str("GroupName", r.groupName).Str("ServiceName", r.serviceName).Msg("Nacos DeRegAll")
	return nil
}

// 注意Close后，不能在调用上面的函数
func (r *Registers) Close() {
	// 加锁
	r.mu.Lock()
	defer r.mu.Unlock()

	log.Info().Str("GroupName", r.groupName).Str("ServiceName", r.serviceName).Msg("Nacos Close")

	t := r.nacosNamingCli
	r.nacosNamingCli = nil
	t.CloseClient() // 经测试里面有goruntine泄露
}
