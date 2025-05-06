package nacos

// https://github.com/yuwf/gobase

import (
	"errors"
	"net"
	"strconv"
	"unicode"

	"github.com/yuwf/gobase/loader"
	"github.com/yuwf/gobase/utils"

	"github.com/nacos-group/nacos-sdk-go/v2/clients"
	"github.com/nacos-group/nacos-sdk-go/v2/clients/config_client"
	"github.com/nacos-group/nacos-sdk-go/v2/clients/naming_client"
	"github.com/nacos-group/nacos-sdk-go/v2/common/constant"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"
	"github.com/rs/zerolog/log"
)

type Config struct {
	NamespaceId string   // 命名空间的ID，不是名字，默认的public命名空间没有id
	Addrs       []string // ip:port
	Username    string   // the username for nacos auth
	Password    string   // the password for nacos auth
}

var defaultClient *Client

// Client nacos对象
type Client struct {
	nacosCfgCli    config_client.IConfigClient
	nacosNamingCli naming_client.INamingClient

	clientConfig  constant.ClientConfig
	serverConfigs []constant.ServerConfig
}

func DefaultClient() *Client {
	return defaultClient
}

func InitDefaultClient(cfg *Config) (*Client, error) {
	var err error
	defaultClient, err = CreateClient(cfg)
	return defaultClient, err
}

// CreateClient
func CreateClient(cfg *Config) (*Client, error) {
	clientConfig := constant.ClientConfig{
		NamespaceId: cfg.NamespaceId,
		Username:    cfg.Username,
		Password:    cfg.Password,
		//TimeoutMs:           5000,
		//NotLoadCacheAtStart: true,
		LogDir:   ".nacos",
		CacheDir: ".nacos",
		//LogLevel: "debug",
		LogLevel:             "-",
		NotLoadCacheAtStart:  true,
		UpdateCacheWhenEmpty: true, // 必须设置 否则注册服务器为空时，不会回到服务器发现
	}

	// At least one ServerConfig
	if len(cfg.Addrs) == 0 {
		log.Error().Msg("Nacos CreateNamingClient Addr error")
		return nil, errors.New("Addr empty")
	}
	serverConfigs := []constant.ServerConfig{}
	for _, addr := range cfg.Addrs {
		host, port, err := net.SplitHostPort(addr)
		if err != nil {
			log.Error().Err(err).Str("Addr", addr).Msg("Nacos Addr error")
			return nil, err
		}
		iport, err := strconv.Atoi(port)
		if err != nil {
			log.Error().Err(err).Str("Addr", addr).Msg("Nacos Addr error")
			return nil, err
		}
		serverConfigs = append(serverConfigs, constant.ServerConfig{
			IpAddr: host,
			Port:   uint64(iport),
		})
	}

	// Create naming client for service discovery
	cNaming, err := clients.CreateNamingClient(map[string]interface{}{
		"serverConfigs": serverConfigs,
		"clientConfig":  clientConfig,
	})
	if err != nil {
		log.Error().Err(err).Strs("Addr", cfg.Addrs).Msg("Nacos CreateNamingClient error")
		return nil, err
	}

	// Create config client for dynamic configuration
	cCfc, err := clients.CreateConfigClient(map[string]interface{}{
		"serverConfigs": serverConfigs,
		"clientConfig":  clientConfig,
	})
	if err != nil {
		log.Error().Err(err).Strs("Addr", cfg.Addrs).Msg("Nacos CreateConfigClient error")
		return nil, err
	}

	Client := &Client{
		nacosCfgCli:    cCfc,
		nacosNamingCli: cNaming,
		clientConfig:   clientConfig,
		serverConfigs:  serverConfigs,
	}
	log.Info().Strs("Addr", cfg.Addrs).Msg("Nacos CreateClient success")
	return Client, nil
}

// LoadKV 调用GetKV并加载配置
func (c *Client) LoadConfig(dataId, group string, loader loader.Loader) error {
	content, err := c.GetConfig(dataId, group)
	if err != nil {
		return err
	}
	err = loader.Load(utils.StringToBytes(content), c.GetConfigKey(dataId, group, c.clientConfig.NamespaceId))
	if err != nil {
		return err
	}
	return nil
}

// LoadListKV 调用ListKV并加载配置
func (c *Client) LoadSearchConfig(dataId, group string, loader loader.Loaders) error {
	contents := map[string][]byte{}
	items, err := c.SearchConfig(dataId, group)
	if err != nil {
		return err
	}
	for _, i := range items {
		contents[c.GetConfigKey(i.DataId, i.Group, c.clientConfig.NamespaceId)] = utils.StringToBytes(i.Content)
	}
	err = loader.Load(contents, c.GetConfigKey(dataId, group, c.clientConfig.NamespaceId))
	if err != nil {
		return err
	}
	return nil
}

func (c *Client) GetConfigKey(dataId string, group string, tenant string) string {
	return dataId + "@" + group + "@" + tenant
}

// GetConfig 获取对应的value
func (c *Client) GetConfig(dataId, group string) (string, error) {
	content, err := c.nacosCfgCli.GetConfig(vo.ConfigParam{
		DataId: dataId,
		Group:  group,
	})
	if err != nil {
		log.Error().Err(err).Str("dataId", dataId).Str("group", group).Msg("Nacos GetConfig error")
		return "", err
	}
	return content, nil
}

// PublishConfig 写对应的value值
func (c *Client) PublishConfig(dataId, group, content string) (bool, error) {
	success, err := c.nacosCfgCli.PublishConfig(vo.ConfigParam{
		DataId:  dataId,
		Group:   group,
		Content: content,
	})
	if err != nil {
		log.Error().Err(err).Str("dataId", dataId).Str("group", group).Msg("Nacos PublishConfig error")
		return false, err
	}
	return success, nil
}

// DelConfig 删除，不存在也会返回成功
func (c *Client) DelConfig(dataId, group string) (bool, error) {
	success, err := c.nacosCfgCli.DeleteConfig(vo.ConfigParam{
		DataId: dataId,
		Group:  group,
	})
	if err != nil {
		log.Error().Err(err).Str("dataId", dataId).Str("group", group).Msg("Nacos DeleteConfig error")
		return false, err
	}
	return success, nil
}

type SearchItem struct {
	DataId  string `json:"dataId"`
	Group   string `json:"group"`
	Content string `json:"content"`
}

// SearchConfig 搜索, dataId,group支持通配符搜索
func (c *Client) SearchConfig(dataId, group string) ([]*SearchItem, error) {
	pageNo := 0
	resp := []*SearchItem{}
	for {
		contents, err := c.nacosCfgCli.SearchConfig(vo.SearchConfigParam{
			Search:   "blur",
			DataId:   dataId,
			Group:    group,
			PageNo:   pageNo,
			PageSize: 10,
		})
		if err != nil {
			log.Error().Err(err).Str("dataId", dataId).Str("group", group).Msg("Nacos SearchConfig error")
			return nil, err
		}

		if len(contents.PageItems) == 0 {
			break
		}
		for _, content := range contents.PageItems {
			resp = append(resp, &SearchItem{
				DataId:  content.DataId,
				Group:   content.Group,
				Content: content.Content,
			})
		}
		if len(resp) == contents.TotalCount {
			break
		}
		pageNo += 1
	}

	return resp, nil
}

// 获取group下所有的服务器name
func (c *Client) GetAllServicesInfo(groupName string) ([]string, error) {
	pageNo := uint32(1)
	serviceNames := []string{}
	for {
		services, err := c.nacosNamingCli.GetAllServicesInfo(vo.GetAllServiceInfoParam{
			NameSpace: c.clientConfig.NamespaceId,
			GroupName: groupName,
			PageNo:    pageNo,
			PageSize:  uint32(100),
		})
		if err != nil {
			log.Error().Err(err).Str("group", groupName).Msg("Nacos GetAllServicesInfo error")
			return nil, err
		}
		if len(services.Doms) == 0 {
			break
		}
		serviceNames = append(serviceNames, services.Doms...)
		if len(serviceNames) == int(services.Count) {
			break
		}
		pageNo += 1
	}
	return serviceNames, nil
}

func (c *Client) SelectInstances(serviceName, groupName string, clusters []string) ([]*RegistryInfo, error) {
	serviceName = c.SanitizeString(serviceName)
	clusters = c.SanitizeStrings(clusters)
	instances, err := c.nacosNamingCli.SelectInstances(vo.SelectInstancesParam{
		ServiceName: serviceName,
		GroupName:   groupName,
		Clusters:    clusters,
		HealthyOnly: true,
	})
	if err != nil {
		log.Error().Err(err).Str("serviceName", serviceName).Str("group", groupName).Strs("clusters", clusters).Msg("Nacos SelectInstances error")
		return nil, err
	}
	resp := []*RegistryInfo{}
	for _, instance := range instances {
		resp = append(resp, &RegistryInfo{
			InstanceId:  instance.InstanceId,
			Ip:          instance.Ip,
			Port:        int(instance.Port),
			Metadata:    instance.Metadata,
			ServiceName: instance.ServiceName,
			GroupName:   groupName,
			ClusterName: instance.ClusterName,
		})
	}

	return resp, nil
}

func (c *Client) SanitizeString(input string) string {
	var result []rune
	for _, char := range input {
		if unicode.IsLetter(char) || unicode.IsDigit(char) || char == '-' {
			result = append(result, char)
		} else {
			result = append(result, '-')
		}
	}
	return string(result)
}

func (c *Client) SanitizeStrings(input []string) []string {
	var result []string
	for _, s := range input {
		result = append(result, c.SanitizeString(s))
	}
	return result
}
