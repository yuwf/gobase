package consul

// https://github.com/yuwf/gobase

import (
	"errors"

	"gobase/loader"

	"github.com/hashicorp/consul/api"
	"github.com/rs/zerolog/log"
)

var defaultClient *Client

// Client consul对象
// https://developer.hashicorp.com/consul/api-docs
type Client struct {
	consulCli *api.Client // consul连接
}

func DefaultClient() *Client {
	return defaultClient
}

func InitDefaultClient(addr, scheme string) (*Client, error) {
	var err error
	defaultClient, err = CreateClient(addr, scheme)
	return defaultClient, err
}

// CreateClient 创建 addr:Consul地址 scheme：连接consul协议
func CreateClient(addr, scheme string) (*Client, error) {
	c, err := api.NewClient(&api.Config{Address: addr, Scheme: scheme})
	if err != nil {
		log.Error().Err(err).Msg("Consul NewClient error")
		return nil, err
	}
	Client := &Client{
		consulCli: c,
	}
	log.Info().Msg("Consul CreateClient success")
	return Client, nil
}

// LoadKV 调用GetKV并加载配置
func (c *Client) LoadKV(path string, waitIndex uint64, loader loader.Loader) (uint64, error) {
	value, index, err := c.GetKV(path, waitIndex)
	if err != nil {
		return 0, err
	}
	err = loader.Load(value, path)
	if err != nil {
		return 0, err
	}
	return index, nil
}

// LoadListKV 调用ListKV并加载配置
func (c *Client) LoadListKV(path string, waitIndex uint64, loader loader.Loaders) (uint64, error) {
	value, index, err := c.ListKV(path, waitIndex)
	if err != nil {
		return 0, err
	}
	err = loader.Load(value, path)
	if err != nil {
		return 0, err
	}
	return index, nil
}

// ListKV 列出path下的所有KV
func (c *Client) ListKV(path string, waitIndex uint64) (map[string][]byte, uint64, error) {
	rst := map[string][]byte{}
	q := &api.QueryOptions{RequireConsistent: true, WaitIndex: waitIndex}
	resp, meta, err := c.consulCli.KV().List(path, q)
	if err != nil {
		log.Error().Err(err).Str("path", path).Msg("Consul KV List error")
		return rst, 0, err
	}
	for _, kv := range resp {
		rst[kv.Key] = kv.Value
	}
	return rst, meta.LastIndex, nil
}

// GetKV 获取key对应的value
func (c *Client) GetKV(key string, waitIndex uint64) ([]byte, uint64, error) {
	q := &api.QueryOptions{RequireConsistent: true, WaitIndex: waitIndex}
	resp, meta, err := c.consulCli.KV().Get(key, q)
	if err != nil {
		log.Error().Err(err).Str("key", key).Msg("Consul KV Get error")
		return nil, 0, err
	}
	if resp == nil {
		err = errors.New("value is nil")
		log.Error().Err(err).Str("key", key).Msg("Consul KV Get error")
		return nil, meta.LastIndex, err
	}
	return resp.Value, meta.LastIndex, nil
}

// PutKV 写key对应的value值
func (c *Client) PutKV(key, value string, index uint64) (bool, error) {
	p := &api.KVPair{Key: key, Value: []byte(value), ModifyIndex: index}
	resp, _, err := c.consulCli.KV().CAS(p, nil)
	if err != nil {
		log.Error().Err(err).Str("key", key).Msg("Consul KV Put error")
		return false, err
	}
	return resp, nil
}

// DelKV 删除key
func (c *Client) DelKV(key string, index uint64) (bool, error) {
	p := &api.KVPair{Key: key, ModifyIndex: index}
	resp, _, err := c.consulCli.KV().DeleteCAS(p, nil)
	if err != nil {
		log.Error().Err(err).Str("key", key).Msg("Consul KV Del error")
		return false, err
	}
	return resp, nil
}

// GetServices 获取服务器组和服务器组下的tag
func (c *Client) GetServices(waitIndex uint64) (map[string][]string, uint64, error) {
	q := &api.QueryOptions{RequireConsistent: true, WaitIndex: waitIndex}
	resp, meta, err := c.consulCli.Catalog().Services(q)
	if err != nil {
		log.Error().Err(err).Msg("Consul Catalog Services error")
		return nil, 0, err
	}
	return resp, meta.LastIndex, nil
}

// GetService 获取服务器组下的服务节点
func (c *Client) GetService(service, tag string, waitIndex uint64) ([]*api.CatalogService, uint64, error) {
	q := &api.QueryOptions{RequireConsistent: true, WaitIndex: waitIndex}
	resp, meta, err := c.consulCli.Catalog().Service(service, tag, q)
	if err != nil {
		log.Error().Err(err).Msg("Consul Catalog Service error")
		return nil, 0, err
	}
	return resp, meta.LastIndex, nil
}

// GetHealthState 获取状态为state的所有的服务节点
// state取值 any(api.HealthAny) passing warning critical maintenance
func (c *Client) GetHealthState(state string, waitIndex uint64) (api.HealthChecks, uint64, error) {
	q := &api.QueryOptions{RequireConsistent: true, WaitIndex: waitIndex}
	resp, meta, err := c.consulCli.Health().State(state, q)
	if err != nil {
		log.Error().Err(err).Msg("Consul Health State error")
		return nil, 0, err
	}
	return resp, meta.LastIndex, nil
}

// GetHealthService 获取某个服务的所有的服务节点
func (c *Client) GetHealthService(service, tag string, passingOnly bool, waitIndex uint64) ([]*api.ServiceEntry, uint64, error) {
	q := &api.QueryOptions{RequireConsistent: true, WaitIndex: waitIndex}
	// 获取所有passing状态的service
	entrys, meta, err := c.consulCli.Health().Service(service, tag, passingOnly, q)
	if err != nil {
		log.Error().Err(err).Msg("Consul Health Service error")
		return nil, 0, err
	}
	return entrys, meta.LastIndex, nil
}
