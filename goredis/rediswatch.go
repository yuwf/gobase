package goredis

// https://github.com/yuwf/gobase

import (
	"context"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"time"

	"github.com/yuwf/gobase/utils"

	"github.com/rs/zerolog/log"
)

// 使用Redis做服务器发现使用

// WatchService 监控服务器变化 RegistryConfig值填充Registry前缀的变量 回调外部不要修改infos参数
// key 表示服务器发现的key
// serverName 表示监听哪些服务器 为空表示监听全部的服务器
func (r *Redis) WatchServices(key string, serverNames []string, fun func(ctx context.Context, infos []*RegistryInfo)) (io.Closer, error) {
	log.Info().Str("key", key).Msg("Redis WatchService")

	ctx := utils.CtxSetNolog(context.TODO())
	ctx = utils.CtxAddLog(ctx, "WatchServices", key)
	// 先创建订阅对象
	subscriber := r.CreateSubscribe(ctx)
	if err := subscriber.Subscribe(ctx, key); err != nil {
		log.Error().Err(err).Msg("Redis WatchService Subscribe failed")
		return nil, err
	}
	go func() {
		var last []*RegistryInfo
		// 先读一次
		rst, err := r.ReadServices(ctx, key, serverNames)
		if err == nil {
			if !isSame(last, rst) {
				last = rst
				fun(ctx, rst)
			}
		}
		uuid := utils.LocalIPString() + ":" + strconv.Itoa(os.Getpid()) + ":" + utils.RandString(8)
		checkInterval := RegExprieTime / 2 // 没有正常注销的检查时间间隔
		for {
			ch := subscriber.Channel()
			timer := time.NewTimer(time.Duration(checkInterval) * time.Second)
			select {
			case message := <-ch:
				if !timer.Stop() {
					select {
					case <-timer.C: // try to drain the channel
					default:
					}
				}
				if message == nil {
					log.Debug().Msg("Redis WatchService Exit")
					return
				}
				// 其他情况读取
				rst, err := r.ReadServices(ctx, key, serverNames)
				if err == nil {
					if !isSame(last, rst) {
						last = rst
						ctx2 := utils.CtxSetTrace(ctx, 0, fmt.Sprintf("redis_watch:%s", message.Payload))
						fun(ctx2, rst)
					}
				}
			case <-timer.C:
				// 超时检查一次
				rst, _ := r.Script(ctx, checkServicesScirpt, []string{key}, RegExprieTime, uuid).Int()
				if rst > 0 {
					checkInterval = RegExprieTime / 2
				} else {
					checkInterval = RegExprieTime
				}
			}
		}
	}()
	return subscriber, nil
}

// WatchService 监控服务器变化，通知变化，增加或者删除
// key 表示服务器发现的key
// serverName 表示监听哪些服务器 为空表示监听全部的服务器
func (r *Redis) WatchServices2(key string, serverNames []string, fun func(ctx context.Context, addInfos, delInfos []*RegistryInfo)) (io.Closer, error) {
	log.Info().Str("key", key).Msg("Redis WatchService")
	ctx := utils.CtxSetNolog(context.TODO())
	ctx = utils.CtxAddLog(ctx, "WatchServices2", key)
	// 先创建订阅对象
	subscriber := r.CreateSubscribe(ctx)
	if err := subscriber.Subscribe(ctx, key); err != nil {
		log.Error().Err(err).Msg("Redis WatchService Subscribe failed")
		return nil, err
	}
	go func() {
		var last []*RegistryInfo
		// 先读一次
		rst, err := r.ReadServices(ctx, key, serverNames)
		if err == nil {
			addInfos, delInfos := diff(last, rst)
			if len(addInfos) != 0 || len(delInfos) != 0 {
				fun(ctx, addInfos, delInfos)
				last = rst
			}
		}
		uuid := utils.LocalIPString() + ":" + strconv.Itoa(os.Getpid()) + ":" + utils.RandString(8)
		checkInterval := RegExprieTime / 2 // 没有正常注销的检查时间间隔
		for {
			ch := subscriber.Channel()
			timer := time.NewTimer(time.Duration(checkInterval) * time.Second)
			select {
			case message := <-ch:
				if !timer.Stop() {
					select {
					case <-timer.C: // try to drain the channel
					default:
					}
				}
				if message == nil {
					log.Debug().Msg("Redis WatchService Exit")
					return
				}
				// 其他情况读取
				rst, err := r.ReadServices(ctx, key, serverNames)
				if err == nil {
					addInfos, delInfos := diff(last, rst)
					if len(addInfos) != 0 || len(delInfos) != 0 {
						ctx2 := utils.CtxSetTrace(ctx, 0, fmt.Sprintf("redis_watch:%s", message.Payload))
						fun(ctx2, addInfos, delInfos)
						last = rst
					}
				}
			case <-timer.C:
				// 超时检查一次
				rst, _ := r.Script(ctx, checkServicesScirpt, []string{key}, RegExprieTime, uuid).Int()
				if rst > 0 {
					checkInterval = RegExprieTime / 2
				} else {
					checkInterval = RegExprieTime
				}
			}
		}
	}()
	return subscriber, nil
}

// 读取一次服务器列表
func (r *Redis) ReadServices(ctx context.Context, key string, serverNames []string) ([]*RegistryInfo, error) {
	ctx = utils.CtxAddLog(ctx, "ReadServices", key)
	rst := []*RegistryInfo{}
	err := r.Script(ctx, readRegisterScirpt, []string{key}, RegExprieTime).BindJsonObjSlice(&rst)
	if err != nil {
		// 错误了 在来一次
		err = r.Script(ctx, readRegisterScirpt, []string{key}, RegExprieTime).BindJsonObjSlice(&rst)
		if err != nil {
			return nil, err
		}
	}

	// 排序
	sort.SliceStable(rst, func(i, j int) bool {
		if rst[i].ID != rst[j].ID {
			return rst[i].ID < rst[j].ID
		}
		return rst[i].Name < rst[j].Name
	})
	return rst, nil
}

// 判断两个列表是否一样
func isSame(last, new []*RegistryInfo) bool {
	if len(last) != len(new) {
		return false
	}
	for i := 0; i < len(new); i++ {
		if last[i].Name != new[i].Name {
			return false
		}
		if last[i].ID != new[i].ID {
			return false
		}
		if last[i].Addr != new[i].Addr {
			return false
		}
		if last[i].Port != new[i].Port {
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
	if last.Name != new.Name {
		return false
	}
	if last.ID != new.ID {
		return false
	}
	if last.Addr != new.Addr {
		return false
	}
	if last.Port != new.Port {
		return false
	}
	return true
}
