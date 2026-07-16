package goredis

// https://github.com/yuwf/gobase

import (
	"context"
	"strings"
	"time"
	"unicode"

	"github.com/yuwf/gobase/utils"

	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

type hook struct {
	redis *Redis
}

func (h *hook) DialHook(next redis.DialHook) redis.DialHook {
	return next
}

func (h *hook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	callback := func(ctx context.Context, cmd redis.Cmder) error {
		entry := time.Now()
		err := next(ctx, cmd)
		err = h.cmdCallback(ctx, cmd, err, entry)
		return err
	}
	return callback
}

func (h *hook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	callback := func(ctx context.Context, cmds []redis.Cmder) error {
		entry := time.Now()
		err := next(ctx, cmds)
		err = h.pipelineCallback(ctx, cmds, err, entry)
		return err
	}
	return callback
}

func getCmdName(query, def string) string {
	for i, r := range query {
		if unicode.IsSpace(r) { // 检查是否为空格或其他空白字符
			return strings.ToUpper(query[:i])
		}
	}
	return def
}

func (h *hook) cmdCallback(ctx context.Context, cmd redis.Cmder, err error, entry time.Time) error {
	elapsed := time.Since(entry)

	// 日志输出
	if err != nil && err != redis.Nil && !redis.HasErrorPrefix(err, "NOSCRIPT") {
		cmdStr := CmdString(ctx, cmd, nil)
		cmdName := getCmdName(cmdStr, "cmd")
		utils.LogCtx(log.Error(), ctx).Err(err).Int32("elapsed", int32(elapsed/time.Millisecond)).
			Str("cmd", cmdStr).
			Msg("Redis " + cmdName + " Fail")
	} else if !utils.CtxHasNolog(ctx) && zerolog.DebugLevel >= log.Logger.GetLevel() {
		cmdStr := CmdString(ctx, cmd, nil)
		cmdName := getCmdName(cmdStr, "cmd")
		replyStr := ReplyString(ctx, cmd, nil)
		utils.LogCtx(log.Debug(), ctx).Int32("elapsed", int32(elapsed/time.Millisecond)).
			Str("cmd", cmdStr).
			Str("reply", replyStr).
			Msg("Redis " + cmdName + " Success")
	}

	// 回调
	func() {
		defer utils.HandlePanic()
		for _, f := range h.redis.hook {
			f(ctx, cmd, nil, elapsed)
		}
	}()
	return err
}

func (h *hook) pipelineCallback(ctx context.Context, cmds []redis.Cmder, err error, entry time.Time) error {
	elapsed := time.Since(entry)

	// 回调绑定
	errModify := false // 记录是否修改了错误
	for _, cmd := range cmds {
		if redisCmd, ok := cmd.(*RedisCommond); ok {
			redisCmd.processed = true

			if redisCmd.nscallback != nil && redis.HasErrorPrefix(cmd.Err(), "NOSCRIPT") {
				r := redisCmd.nscallback()
				redisCmd.Cmd.SetVal(r.Val()) // 修改命令结果
				redisCmd.SetErr(r.Err())
				errModify = true
			}
			if cmd.Err() != nil && cmd.Err() != redis.Nil && !redis.HasErrorPrefix(cmd.Err(), "NOSCRIPT") {
				utils.LogCtx(log.Error(), redisCmd.ctx).Err(cmd.Err()).Str("cmd", CmdString(redisCmd.ctx, cmd, nil)).Msg("RedisCommond Error")
				continue
			}
			if redisCmd.callback != nil {
				err := redisCmd.callback(redisCmd.Val())
				if err != nil {
					redisCmd.SetErr(err)
					errModify = true
					// 有些绑定函数也会返回空
					// 绑定函数返回的redis.Nil 不屏蔽
					if err != redis.Nil {
						utils.LogCtx(log.Error(), redisCmd.ctx).Err(err).Str("cmd", CmdString(redisCmd.ctx, cmd, nil)).Msg("RedisCommond Bind Error")
					}
				}
			}
		}

	}

	// 因为上面的回调绑定可能会重新刷新结果 给重新找一个错误，go-redis也是这样找的
	if errModify {
		err = nil
		for _, cmd := range cmds {
			if r := cmd.Err(); r != nil {
				err = r
				break
			}
		}
	}

	// 日志输出
	if err != nil && err != redis.Nil {
		cmdStr := CmdString(ctx, nil, cmds)
		replyStr := ReplyString(ctx, nil, cmds)
		utils.LogCtx(log.Error(), ctx).Err(err).Int32("elapsed", int32(elapsed/time.Millisecond)).
			Str("cmd", cmdStr).
			Str("reply", replyStr).
			Msg("RedisPipeline Fail")
	} else if !utils.CtxHasNolog(ctx) && zerolog.DebugLevel >= log.Logger.GetLevel() {
		cmdStr := CmdString(ctx, nil, cmds)
		replyStr := ReplyString(ctx, nil, cmds)
		utils.LogCtx(log.Debug(), ctx).Int32("elapsed", int32(elapsed/time.Millisecond)).
			Str("cmd", cmdStr).
			Str("reply", replyStr).
			Msg("RedisPipeline Success")
	}

	// 回调
	func() {
		defer utils.HandlePanic()
		for _, f := range h.redis.hook {
			f(ctx, nil, cmds, elapsed)
		}
	}()

	return err
}
