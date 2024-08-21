package redis

// https://github.com/yuwf/gobase

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/yuwf/gobase/utils"

	"github.com/gomodule/redigo/redis"
	"github.com/rs/zerolog/log"
)

type RedisPipeline struct {
	// 命令列表 执行Do后 会自动清空
	cmds []*RedisCommond
	// 支持特殊情况下指定超时，如 BLPOP 命令
	Timeout time.Duration
	pool    *redis.Pool
}

func (r *Redis) NewPipeline() *RedisPipeline {
	pipeline := &RedisPipeline{
		pool: r.pool,
	}
	return pipeline
}

func (p *RedisPipeline) Do(ctx context.Context) error {
	return p.do(ctx)
}

// 任何一条命令错误，都会返回错误
func (p *RedisPipeline) DoEx(ctx context.Context) ([]*RedisCommond, error) {
	cmds := p.cmds
	err := p.do(ctx)
	if err != nil {
		return cmds, err
	}
	for _, cmd := range cmds {
		if cmd.Err != nil {
			return cmds, cmd.Err
		}
	}
	return cmds, nil
}

// 管道命令发送、接收和解析
func (p *RedisPipeline) do(ctx context.Context) error {
	if p.cmds == nil || len(p.cmds) == 0 {
		return nil
	}
	if ctx.Value(utils.CtxKey_caller) == nil {
		ctx = context.WithValue(ctx, utils.CtxKey_caller, utils.GetCallerDesc(2))
	}
	if p.pool == nil {
		utils.LogCtx(log.Error(), ctx).Msg("RedisPipeline pool is nil")
		return errors.New("RedisPipeline pool is nil")
	}
	entry := time.Now()
	conn := p.pool.Get()
	defer conn.Close()
	defer func() {
		logOut := true
		if ctx != nil {
			nolog := ctx.Value(CtxKey_nolog)
			if nolog != nil {
				logOut = false
			}
		}
		if logOut {
			es := time.Since(entry) / time.Millisecond
			l := utils.LogCtx(log.Debug(), ctx).Int32("elapsed", int32(es))
			for i, cmd := range p.cmds {
				if i == 8 {
					s := fmt.Sprintf("cmd(%d)", len(p.cmds)-i)
					l.Str(s, "...")
					break
				}
				l.Str(fmt.Sprint("cmd", i), cmd.CmdString())
				l.Str(fmt.Sprint("reply", i), cmd.ReplyString())
			}
			l.Msg("RedisPipeline docmd")
		}

		p.cmds = nil // 执行完清空命令
	}()

	for _, cmd := range p.cmds {
		err := conn.Send(cmd.Cmd, cmd.Args...)
		if err != nil {
			utils.LogCtx(log.Error(), ctx).Err(err).Str("cmd", cmd.CmdString()).Msg("RedisPipeline Send Error")
			return err
		}
	}
	err := conn.Flush()
	if err != nil {
		utils.LogCtx(log.Error(), ctx).Err(err).Msg("RedisPipeline Flush Error")
		return err
	}

	for _, cmd := range p.cmds {
		if p.Timeout == 0 {
			cmd.Reply, cmd.Err = conn.Receive()
		} else {
			cmd.Reply, cmd.Err = redis.ReceiveWithTimeout(conn, p.Timeout)
		}
		if cmd.Err != nil { // 命令错误 也会走到这里面
			utils.LogCtx(log.Error(), ctx).Err(cmd.Err).Str("cmd", cmd.CmdString()).Msg("RedisPipeline Receive Error")
			continue
		}
		if cmd.callback != nil {
			cmd.Err = cmd.callback(cmd.Reply)
			if err != nil {
				utils.LogCtx(log.Error(), ctx).Err(cmd.Err).Str("cmd", cmd.CmdString()).Msg("RedisCommond Bind Error")
			}
		}
	}
	return nil
}

// 统一的命令
func (p *RedisPipeline) Cmd(ctx context.Context, commandName string, args ...interface{}) *RedisCommond {
	if ctx.Value(utils.CtxKey_caller) == nil {
		ctx = context.WithValue(ctx, utils.CtxKey_caller, utils.GetCallerDesc(1))
	}
	redisCmd := &RedisCommond{
		ctx:  ctx,
		Cmd:  commandName,
		Args: args,
	}
	p.cmds = append(p.cmds, redisCmd)
	return redisCmd
}

// 参数v 参考Redis.HMGetObj的说明
func (p *RedisPipeline) HMGetObj(ctx context.Context, key string, v interface{}) error {
	if ctx.Value(utils.CtxKey_caller) == nil {
		ctx = context.WithValue(ctx, utils.CtxKey_caller, utils.GetCallerDesc(1))
	}
	redisCmd := &RedisCommond{
		ctx: ctx,
		Cmd: "HMGET",
	}
	// 获取结构数据
	sInfo, err := utils.GetStructInfoByTag(v, RedisTag)
	if err != nil {
		utils.LogCtx(log.Error(), ctx).Err(err).Msg("RedisPipeline HMSetObj Param error")
		return err
	}
	if len(sInfo.Tags) == 0 {
		err := errors.New("structmem invalid")
		utils.LogCtx(log.Error(), ctx).Err(err).Msg("RedisPipeline HMSetObj Param error")
		return err
	}

	redisCmd.BindValues(sInfo.Elemts) // 管道里这个不会返回错误
	redisCmd.Args = append(redisCmd.Args, key)
	redisCmd.Args = append(redisCmd.Args, sInfo.Tags...)
	p.cmds = append(p.cmds, redisCmd)
	return nil
}

// 参数v 参考Redis.HMGetObj的说明
func (p *RedisPipeline) HMSetObj(ctx context.Context, key string, v interface{}) error {
	if ctx.Value(utils.CtxKey_caller) == nil {
		ctx = context.WithValue(ctx, utils.CtxKey_caller, utils.GetCallerDesc(1))
	}
	redisCmd := &RedisCommond{
		ctx: ctx,
		Cmd: "HMSET",
	}
	sInfo, err := utils.GetStructInfoByTag(v, RedisTag)
	if err != nil {
		utils.LogCtx(log.Error(), ctx).Err(err).Msg("RedisPipeline HMSetObj Param error")
		return err
	}
	fargs := sInfo.TagElemtNoNilFmt()
	if len(fargs) == 0 {
		err := errors.New("structmem invalid")
		utils.LogCtx(log.Error(), ctx).Err(err).Msg("RedisPipeline HMSetObj Param error")
		return err
	}

	redisCmd.Args = append(redisCmd.Args, key)
	redisCmd.Args = append(redisCmd.Args, fargs...)
	p.cmds = append(p.cmds, redisCmd)
	return nil
}
