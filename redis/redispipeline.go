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

/*
//绑定声明变量 注意slice和map对象没有初始化，内部会进行初始化
var test123 int
var testsli []int
var testmap map[string]string

var pipeline store.RedisPipeline                   //管道对象
pipeline.Cmd("Set", "test123", "123")              //Redis命令
pipeline.Cmd("Set", "test456", "456")
pipeline.Cmd("Set", "test789", "789")
pipeline.Cmd("HSET", "testmap", "f1", "1")
pipeline.Cmd("HSET", "testmap", "f2", "2")
pipeline.Cmd("Get", "test123").Bind(&test123)      //带绑定对象的Redis命令，命令的返回值解析到绑定的对象上
pipeline.Cmd("MGET", "test123", "test456", "test789").BindSlice(&testsli)
pipeline.Cmd("HGETALL", "testmap").BindMap(&testmap)

type Test struct {
	F1 int `redis:"f1"`
	F2 int `redis:"f2"`
}
var t Test
pipeline.Cmd("HMGET", "testmap").HMGetBindObj(&t)
pipeline.HMGetObj("testmap", &t)
pipeline.HMSetObj("testmap", &t)

pipeline.Do()                           //执行管道命令，并解析返回值到绑定的对象上
*/

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
	caller := utils.GetCallerDesc(2)
	if p.pool == nil {
		log.Error().Str("pos", caller.Pos()).Msg("RedisPipeline pool is nil")
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
			ln := log.Debug().Int32("elapsed", int32(es))
			for i, cmd := range p.cmds {
				if i == 8 {
					s := fmt.Sprintf("cmd(%d)", len(p.cmds)-i)
					ln.Str(s, "...")
					break
				}
				ln.Str(fmt.Sprint("cmd", i), cmd.CmdString())
				ln.Str(fmt.Sprint("reply", i), cmd.ReplyString())
			}
			ln.Str("pos", caller.Pos()).Msg("RedisPipeline docmd")
		}

		p.cmds = nil // 执行完清空命令
	}()

	for _, cmd := range p.cmds {
		err := conn.Send(cmd.Cmd, cmd.Args...)
		if err != nil {
			log.Error().Err(err).Str("cmd", cmd.CmdString()).Str("pos", cmd.Caller.Pos()).Msg("RedisPipeline Send Error")
			return err
		}
	}
	err := conn.Flush()
	if err != nil {
		log.Error().Err(err).Str("pos", caller.Pos()).Msg("RedisPipeline Flush Error")
		return err
	}

	for _, cmd := range p.cmds {
		if p.Timeout == 0 {
			cmd.Reply, cmd.Err = conn.Receive()
		} else {
			cmd.Reply, cmd.Err = redis.ReceiveWithTimeout(conn, p.Timeout)
		}
		if cmd.Err != nil { // 命令错误 也会走到这里面
			log.Error().Err(cmd.Err).Str("cmd", cmd.CmdString()).Str("pos", cmd.Caller.Pos()).Msg("RedisPipeline Receive Error")
			continue
		}
		if cmd.callback != nil {
			cmd.Err = cmd.callback(cmd.Reply)
			if err != nil {
				log.Error().Err(cmd.Err).Str("cmd", cmd.CmdString()).Str("pos", cmd.Caller.Pos()).Msg("RedisResult Bind Error")
			}
		}
	}
	return nil
}

// 统一的命令
func (p *RedisPipeline) Cmd(commandName string, args ...interface{}) RedisResultBind {
	redisCmd := &RedisCommond{
		Cmd:    commandName,
		Args:   args,
		Caller: utils.GetCallerDesc(1),
	}
	p.cmds = append(p.cmds, redisCmd)
	return redisCmd
}

// 参数v 参考RedisResultBind.HMGetBindObj的说明
func (p *RedisPipeline) HMGetObj(key string, v interface{}) error {
	redisCmd := &RedisCommond{
		Cmd:    "HMGET",
		Caller: utils.GetCallerDesc(1),
	}
	// 组织参数
	redisCmd.Args = append(redisCmd.Args, key)
	err := redisCmd.HMGetBindObj(v)
	if err != nil {
		return err
	}
	p.cmds = append(p.cmds, redisCmd)
	return nil
}

// 参数v 参考RedisResultBind.HMGetBindObj的说明
func (p *RedisPipeline) HMSetObj(key string, v interface{}) bool {
	redisCmd := &RedisCommond{
		Cmd:    "HMSET",
		Caller: utils.GetCallerDesc(1),
	}
	redisCmd.Args = append(redisCmd.Args, key)
	redisCmd.Err = redisCmd._HMSetObjArgs(v)
	if redisCmd.Err != nil {
		log.Error().Str("cmd", redisCmd.CmdString()).Str("pos", redisCmd.Caller.Pos()).Msg("RedisPipeline HMSetObj Param error")
		return false
	}

	p.cmds = append(p.cmds, redisCmd)
	return true
}
