package apollo

// https://github.com/yuwf/gobase

import (
	"errors"
	"net/url"
	"strings"

	"github.com/yuwf/gobase/loader"
	"github.com/yuwf/gobase/utils"

	"github.com/apolloconfig/agollo/v4"
	"github.com/apolloconfig/agollo/v4/env/config"
	"github.com/rs/zerolog/log"
)

type Config struct {
	Addr      string   `json:"addr"` // 要求 http:// 开头
	AppID     string   `json:"appId"`
	Cluster   string   `json:"cluster"`
	NameSpace []string `json:"namespace,omitempty"`
	Secret    string   `json:"secret"`
}

var defaultClient *Client

// Client apollo对象
type Client struct {
	conf      Config
	apolloCli agollo.Client // apollo连接
	l         ChangeListener
}

func DefaultClient() *Client {
	return defaultClient
}

func InitDefaultClient(conf *Config) (*Client, error) {
	var err error
	defaultClient, err = CreateClient(conf)
	return defaultClient, err
}

// CreateClient
func CreateClient(conf *Config) (*Client, error) {
	addr := conf.Addr
	if !strings.HasPrefix(addr, "http://") {
		addr = "http://" + addr
	}
	url, err := url.Parse(addr)
	if err != nil {
		log.Error().Err(err).Msg("Apollo CreateClient Addr error")
		return nil, err
	}

	appconf := &config.AppConfig{
		AppID:         conf.AppID,
		Cluster:       conf.Cluster,
		NamespaceName: "",
		IP:            url.Scheme + "://" + url.Host + "/",
		Secret:        conf.Secret,
		MustStart:     false,
	}
	if appconf.Cluster == "" {
		appconf.Cluster = "dev"
	}
	appconf.NamespaceName = strings.Join(conf.NameSpace, ",")

	c, err := agollo.StartWithConfig(func() (*config.AppConfig, error) {
		return appconf, nil
	})
	if err != nil {
		log.Error().Err(err).Msg("Apollo CreateClient error")
		return nil, err
	}
	Client := &Client{
		conf:      *conf, // 配置拷贝一份 防止被外部修改
		apolloCli: c,
	}
	c.AddChangeListener(&Client.l)
	log.Info().Str("Addr", conf.Addr).Str("AppID", conf.AppID).Str("Cluster", conf.Cluster).Strs("NameSpace", conf.NameSpace).
		Msg("Apollo CreateClient Success")
	return Client, nil
}

// Watch 监控key配置 immediately是否先同步获取一次配置
// watch后的key允许删除
func (c *Client) Watch(namespace, key string, loader loader.Loader, immediately bool) error {
	log.Info().Str("addr", c.conf.Addr).Str("appID", c.conf.AppID).Str("cluster", c.conf.Cluster).
		Str("namespace", namespace).Str("key", key).
		Msg("Apollo Watch")

	if immediately {
		err := c.Load(namespace, key, loader)
		if err != nil {
			return err
		}
	} else {
		// 尝试加载一次
		conf := c.apolloCli.GetConfig(namespace)
		if conf != nil {
			value, err := conf.GetCache().Get(key)
			if err == nil {
				loader.Load(utils.StringToBytes(value.(string)), namespace+"/"+key)
			}
		}
	}

	c.l.addWatch(namespace, key, func(value interface{}) {
		if value == nil {
			loader.Load(nil, namespace+"/"+key)
		} else {
			loader.Load(utils.StringToBytes(value.(string)), namespace+"/"+key)
		}
	})
	return nil
}

// Load 调用Get并加载配置
func (c *Client) Load(namespace, key string, loader loader.Loader) error {
	value, err := c.Get(namespace, key)
	if err != nil {
		return err
	}
	err = loader.Load(utils.StringToBytes(value), namespace+"/"+key)
	if err != nil {
		return err
	}
	return nil
}

func (c *Client) Get(namespace, key string) (string, error) {
	conf := c.apolloCli.GetConfig(namespace)
	if conf == nil {
		err := errors.New("namespace not exist")
		log.Error().Err(err).
			Str("addr", c.conf.Addr).Str("appID", c.conf.AppID).Str("cluster", c.conf.Cluster).
			Str("namespace", namespace).Str("key", key).
			Msg("Apollo Get namespace error")
		return "", err
	}
	value, err := conf.GetCache().Get(key)
	if err != nil {
		log.Error().Err(err).
			Str("addr", c.conf.Addr).Str("appID", c.conf.AppID).Str("cluster", c.conf.Cluster).
			Str("namespace", namespace).Str("key", key).
			Msg("Apollo Get key error")
		return "", err
	}
	return value.(string), nil
}
