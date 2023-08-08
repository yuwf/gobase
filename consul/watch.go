package consul

// https://github.com/yuwf/gobase

import (
	"sort"
	"time"

	"gobase/loader"

	"github.com/hashicorp/consul/api"
	"github.com/rs/zerolog/log"
)

// WatchKV 监控key配置 immediately是否先同步获取一次配置
// watch后的key不允许删除，就是说这个key要求一只存在，否则一只会尝试加载
func (c *Client) WatchKV(key string, loader loader.Loader, immediately bool) error {
	log.Info().Str("key", key).Msg("Consul WatchKV")
	var waitIndex uint64
	if immediately {
		index, err := c.LoadKV(key, waitIndex, loader)
		if err != nil {
			return err
		}
		waitIndex = index
	}
	go func() {
		for {
			index, err := c.LoadKV(key, waitIndex, loader)
			if err != nil {
				time.Sleep(time.Second)
				continue
			}
			waitIndex = index
		}
	}()
	return nil
}

// WatchListKV 监控path路径下的多个配置 immediately是否先同步获取一次配置
func (c *Client) WatchListKV(path string, loader loader.Loaders, immediately bool) error {
	log.Info().Str("path", path).Msg("Consul WatchKV")
	var waitIndex uint64
	if immediately {
		index, err := c.LoadListKV(path, waitIndex, loader)
		if err != nil {
			return err
		}
		waitIndex = index
	}
	go func() {
		for {
			index, err := c.LoadListKV(path, waitIndex, loader)
			if err != nil {
				time.Sleep(time.Second)
				continue
			}
			waitIndex = index
		}
	}()
	return nil
}

// WatchService 监控服务器变化 RegistryConfig值填充Registry前缀的变量 回调外部不要修改infos参数
func (c *Client) WatchServices(tag string, fun func(infos []*RegistryInfo)) {
	log.Info().Str("tag", tag).Msg("Consul WatchService")
	go func() {
		last := []*RegistryInfo{} // 当前所
		var waitIndex uint64
		for {
			// 根据监控状态触发检查
			healthChecks, index, err := c.GetHealthState(api.HealthAny, waitIndex)
			if err != nil {
				time.Sleep(time.Second)
				continue
			}
			waitIndex = index
			// 过滤信息 serviceName : 健康通过的函有tag的serviceId列表
			passing := map[string][]string{}
			for _, check := range healthChecks {
				// 如果服务器是健康的
				if check.Status != api.HealthPassing {
					continue
				}
				// 必须包含tag
				findTag := false
				for _, t := range check.ServiceTags {
					if t == tag {
						findTag = true
						break
					}
				}
				if !findTag {
					continue
				}
				passing[check.ServiceName] = append(passing[check.ServiceName], check.ServiceID)
			}
			rst := []*RegistryInfo{}
			for serviceName, serviceIds := range passing {
				catlog, _, err := c.GetService(serviceName, tag, 0)
				if err != nil {
					continue
				}
				for _, s := range catlog {
					// GetService 会把不健康的也拉取过来，判断是否健康
					ok := false
					for _, serviceId := range serviceIds {
						if s.ServiceID == serviceId {
							ok = true
							break
						}
					}
					if !ok {
						continue
					}
					rst = append(rst, &RegistryInfo{
						RegistryName: s.ServiceName,
						RegistryID:   s.ServiceID,
						RegistryPort: s.ServicePort,
						RegistryAddr: s.ServiceAddress,
						RegistryTag:  s.ServiceTags,
						RegistryMeta: s.ServiceMeta,
					})
				}
			}
			// 排序
			sort.SliceStable(rst, func(i, j int) bool {
				if rst[i].RegistryID < rst[j].RegistryID {
					return true
				}
				return rst[i].RegistryName < rst[j].RegistryName
			})
			if !isSame(last, rst) {
				last = rst
				fun(rst)
			}
		}
	}()
}

// 判断两个列表是否一样
func isSame(last, new []*RegistryInfo) bool {
	if len(last) != len(new) {
		return false
	}
	for i := 0; i < len(new); i++ {
		if last[i].RegistryName != new[i].RegistryName {
			return false
		}
		if last[i].RegistryID != new[i].RegistryID {
			return false
		}
		if last[i].RegistryAddr != new[i].RegistryAddr {
			return false
		}
		if last[i].RegistryPort != new[i].RegistryPort {
			return false
		}
		if len(last[i].RegistryMeta) != len(new[i].RegistryMeta) {
			return false
		}
		// 对比Meta 是否完全一致
		lastMeta := []string{}
		newMeta := []string{}
		for k, _ := range last[i].RegistryMeta {
			lastMeta = append(lastMeta, k)
		}
		for k, _ := range new[i].RegistryMeta {
			newMeta = append(newMeta, k)
		}
		sort.Strings(lastMeta)
		sort.Strings(newMeta)
		for j := 0; j < len(lastMeta); j++ {
			if lastMeta[j] != newMeta[j] {
				return false
			}
		}
		for j := 0; j < len(lastMeta); j++ {
			if last[i].RegistryMeta[lastMeta[i]] != new[i].RegistryMeta[lastMeta[i]] {
				return false
			}
		}
	}
	return true
}
