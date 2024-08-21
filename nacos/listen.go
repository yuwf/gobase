package nacos

// https://github.com/yuwf/gobase

import (
	"sort"
	"time"

	"github.com/yuwf/gobase/loader"
	"github.com/yuwf/gobase/utils"

	"github.com/nacos-group/nacos-sdk-go/v2/model"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"
	"github.com/rs/zerolog/log"
)

// ListenConfig 监听对应的value，不要对相同dataid和group开启多个listen，只会相应一个
func (c *Client) ListenConfig(dataId, group string, loader loader.Loader, immediately bool) error {
	log.Info().Str("dataId", dataId).Str("group", group).Msg("Nacos ListenConfig")
	if immediately {
		err := c.LoadConfig(dataId, group, loader)
		if err != nil {
			return err
		}
	}

	// 内部会开启goruntine
	err := c.nacosCfgCli.ListenConfig(vo.ConfigParam{
		DataId: dataId,
		Group:  group,
		OnChange: func(namespace, group, dataId, data string) {
			err := loader.Load(utils.StringToBytes(data), c.GetConfigKey(dataId, group, namespace))
			if err != nil {
				time.Sleep(time.Second)
			}
		},
	})
	if err != nil {
		log.Error().Err(err).Str("dataId", dataId).Str("group", group).Msg("Nacos ListenConfig error")
		return err
	}
	return nil
}

func (c *Client) CancelListenConfig(dataId, group string) error {
	log.Info().Str("dataId", dataId).Str("group", group).Msg("Nacos CancelListenConfig")
	err := c.nacosCfgCli.CancelListenConfig(vo.ConfigParam{
		DataId: dataId,
		Group:  group,
	})
	if err != nil {
		log.Error().Err(err).Str("dataId", dataId).Str("group", group).Msg("Nacos CancelListenConfig error")
		return err
	}
	return nil
}

// 回调外部不要修改infos参数
func (c *Client) ListenService(serviceName, groupName string, clusters []string, fun func(infos []*RegistryInfo)) error {
	log.Info().Str("serviceName", serviceName).Str("groupName", groupName).Strs("clusters", clusters).Msg("Nacos ListenService")
	last := []*RegistryInfo{} // 当前所
	err := c.nacosNamingCli.Subscribe(&vo.SubscribeParam{
		ServiceName: serviceName,
		GroupName:   groupName,
		Clusters:    clusters,
		SubscribeCallback: func(services []model.Instance, err error) {
			rst := []*RegistryInfo{}
			for _, service := range services {
				rst = append(rst, &RegistryInfo{
					InstanceId:  service.InstanceId,
					Ip:          service.Ip,
					Port:        int(service.Port),
					Metadata:    service.Metadata,
					ServiceName: service.ServiceName,
					GroupName:   groupName,
					ClusterName: service.ClusterName,
				})
			}
			// 排序
			sort.SliceStable(rst, func(i, j int) bool {
				return rst[i].InstanceId < rst[j].InstanceId
			})
			if !isSame(last, rst) {
				last = rst
				fun(rst)
			}
		},
	})
	if err != nil {
		log.Error().Err(err).Str("serviceName", serviceName).Str("groupName", groupName).Strs("clusters", clusters).Msg("Nacos ListenService error")
		return err
	}
	return nil
}

// 回调外部不要修改infos参数
func (c *Client) ListenService2(serviceName, groupName string, clusters []string, fun func(addInfos, delInfos []*RegistryInfo)) error {
	log.Info().Str("serviceName", serviceName).Str("groupName", groupName).Strs("clusters", clusters).Msg("Nacos ListenService2")
	last := []*RegistryInfo{} // 当前所
	err := c.nacosNamingCli.Subscribe(&vo.SubscribeParam{
		ServiceName: serviceName,
		GroupName:   groupName,
		Clusters:    clusters,
		SubscribeCallback: func(services []model.Instance, err error) {
			rst := []*RegistryInfo{}
			for _, service := range services {
				rst = append(rst, &RegistryInfo{
					InstanceId:  service.InstanceId,
					Ip:          service.Ip,
					Port:        int(service.Port),
					Metadata:    service.Metadata,
					ServiceName: service.ServiceName,
					GroupName:   groupName,
					ClusterName: service.ClusterName,
				})
			}
			// 排序
			sort.SliceStable(rst, func(i, j int) bool {
				return rst[i].InstanceId < rst[j].InstanceId
			})
			addInfos, delInfos := diff(last, rst)
			if len(addInfos) != 0 || len(delInfos) != 0 {
				last = rst
				fun(addInfos, delInfos)
			}
		},
	})
	if err != nil {
		log.Error().Err(err).Str("serviceName", serviceName).Str("groupName", groupName).Strs("clusters", clusters).Msg("Nacos ListenService error")
		return err
	}
	return nil
}

// 回调外部不要修改infos参数
func (c *Client) ListenServices(serviceNames []string, groupName string, clusters []string, fun func(infos []*RegistryInfo)) error {
	log.Info().Strs("serviceNames", serviceNames).Str("groupName", groupName).Strs("clusters", clusters).Msg("Nacos ListenService")
	last := make([][]*RegistryInfo, len(serviceNames)) // 当前所
	for i, serviceName_ := range serviceNames {
		index := i
		serviceName := serviceName_
		last[index] = []*RegistryInfo{}

		err := c.nacosNamingCli.Subscribe(&vo.SubscribeParam{
			ServiceName: serviceName,
			GroupName:   groupName,
			Clusters:    clusters,
			SubscribeCallback: func(services []model.Instance, err error) {
				rst := []*RegistryInfo{}
				for _, service := range services {
					rst = append(rst, &RegistryInfo{
						InstanceId:  service.InstanceId,
						Ip:          service.Ip,
						Port:        int(service.Port),
						Metadata:    service.Metadata,
						ServiceName: service.ServiceName,
						GroupName:   groupName,
						ClusterName: service.ClusterName,
					})
				}
				// 排序
				sort.SliceStable(rst, func(i, j int) bool {
					return rst[i].InstanceId < rst[j].InstanceId
				})
				if !isSame(last[index], rst) {
					last[index] = rst
					all := []*RegistryInfo{}
					for _, s := range last {
						all = append(all, s...)
					}
					fun(all)
				}
			},
		})
		if err != nil {
			// 将注册消掉
			for ii, serviceName := range serviceNames {
				if ii < i {
					c.nacosNamingCli.Unsubscribe(&vo.SubscribeParam{
						ServiceName: serviceName,
						GroupName:   groupName,
						Clusters:    clusters,
					})
				}
			}
			log.Error().Err(err).Str("serviceName", serviceName).Str("groupName", groupName).Strs("clusters", clusters).Msg("Nacos ListenService error")
			return err
		}
	}

	return nil
}

// 回调外部不要修改infos参数
func (c *Client) ListenServices2(serviceNames []string, groupName string, clusters []string, fun func(addInfos, delInfos []*RegistryInfo)) error {
	log.Info().Strs("serviceNames", serviceNames).Str("groupName", groupName).Strs("clusters", clusters).Msg("Nacos ListenService")
	last := make([][]*RegistryInfo, len(serviceNames)) // 当前所
	for i, serviceName_ := range serviceNames {
		index := i
		serviceName := serviceName_
		last[index] = []*RegistryInfo{}

		err := c.nacosNamingCli.Subscribe(&vo.SubscribeParam{
			ServiceName: serviceName,
			GroupName:   groupName,
			Clusters:    clusters,
			SubscribeCallback: func(services []model.Instance, err error) {
				rst := []*RegistryInfo{}
				for _, service := range services {
					rst = append(rst, &RegistryInfo{
						InstanceId:  service.InstanceId,
						Ip:          service.Ip,
						Port:        int(service.Port),
						Metadata:    service.Metadata,
						ServiceName: service.ServiceName,
						GroupName:   groupName,
						ClusterName: service.ClusterName,
					})
				}
				// 排序
				sort.SliceStable(rst, func(i, j int) bool {
					return rst[i].InstanceId < rst[j].InstanceId
				})
				addInfos, delInfos := diff(last[index], rst)
				if len(addInfos) != 0 || len(delInfos) != 0 {
					last[index] = rst
					fun(addInfos, delInfos)
				}
			},
		})
		if err != nil {
			// 将注册消掉
			for ii, serviceName_ := range serviceNames {
				if ii < i {
					c.nacosNamingCli.Unsubscribe(&vo.SubscribeParam{
						ServiceName: serviceName_,
						GroupName:   groupName,
						Clusters:    clusters,
					})
				}
			}
			log.Error().Err(err).Str("serviceName", serviceName).Str("groupName", groupName).Strs("clusters", clusters).Msg("Nacos ListenService error")
			return err
		}
	}

	return nil
}

// 判断两个列表是否一样
func isSame(last, new []*RegistryInfo) bool {
	if len(last) != len(new) {
		return false
	}
	for i := 0; i < len(new); i++ {
		if !equal(last[i], new[i]) {
			return false
		}
	}
	return true
}

// 比较新旧列表，返回：new相比last，增加列表，删除的列表
func diff(last, new []*RegistryInfo) ([]*RegistryInfo, []*RegistryInfo) {
	addInfos, delInfos := make([]*RegistryInfo, 0), make([]*RegistryInfo, 0)
	for i := range new {
		addFlag := true
		for j := range last {
			if equal(new[i], last[j]) {
				addFlag = false
				break
			}
		}
		if addFlag {
			addInfos = append(addInfos, new[i])
		}
	}
	for i := range last {
		delFlag := true
		for j := range new {
			if equal(new[j], last[i]) {
				delFlag = false
				break
			}
		}
		if delFlag {
			delInfos = append(delInfos, last[i])
		}
	}
	return addInfos, delInfos
}

func equal(last, new *RegistryInfo) bool {
	if last.InstanceId != new.InstanceId {
		return false
	}
	if last.ServiceName != new.ServiceName {
		return false
	}
	if last.GroupName != new.GroupName {
		return false
	}
	if last.ClusterName != new.ClusterName {
		return false
	}
	if last.Ip != new.Ip {
		return false
	}
	if last.Port != new.Port {
		return false
	}
	if len(last.Metadata) != len(new.Metadata) {
		return false
	}

	// 对比Meta 是否完全一致
	lastMeta := []string{}
	newMeta := []string{}
	for k := range last.Metadata {
		lastMeta = append(lastMeta, k)
	}
	for k := range new.Metadata {
		newMeta = append(newMeta, k)
	}
	sort.Strings(lastMeta)
	sort.Strings(newMeta)
	for j := 0; j < len(lastMeta); j++ {
		if lastMeta[j] != newMeta[j] {
			return false
		}
		if last.Metadata[lastMeta[j]] != new.Metadata[lastMeta[j]] {
			return false
		}
	}
	return true
}
