package redis

// https://github.com/yuwf/gobase

import (
	"context"
	"errors"
	"time"

	"gobase/utils"

	"github.com/gomodule/redigo/redis"
	"github.com/rs/zerolog/log"
)

type RedisScript struct {
	script *redis.Script
}

func NewScript(keyCount int, src string) *RedisScript {
	ret := &RedisScript{
		script: redis.NewScript(keyCount, src),
	}
	return ret
}

func (r *Redis) DoScript(ctx context.Context, script *RedisScript, keysAndArgs ...interface{}) (interface{}, error) {
	redisCmd := r.doScript(ctx, script, keysAndArgs...)
	return redisCmd.Reply, redisCmd.Err
}

func (r *Redis) DoScript2(ctx context.Context, script *RedisScript, keysAndArgs ...interface{}) RedisResultBind {
	return r.doScript(ctx, script, keysAndArgs...)
}

func (r *Redis) doScript(ctx context.Context, script *RedisScript, keysAndArgs ...interface{}) *RedisCommond {
	redisCmd := &RedisCommond{
		Cmd:  "SCRIPT",
		Args: keysAndArgs,
	}
	if ctx != nil {
		caller := ctx.Value("caller")
		if caller != nil {
			redisCmd.Caller, _ = caller.(*utils.CallerDesc)
		}
	}
	if redisCmd.Caller == nil {
		redisCmd.Caller = utils.GetCallerDesc(2)
	}
	r.doScriptCmd(ctx, script, redisCmd)
	return redisCmd
}

func (r *Redis) doScriptCmd(ctx context.Context, script *RedisScript, redisCmd *RedisCommond) {
	if r.pool == nil {
		log.Error().Str("pos", redisCmd.Caller.Pos()).Msg("Redis pool is nil")
		redisCmd.Err = errors.New("Redis pool is nil")
		return
	}

	if len(redisCmd.CmdDesc) == 0 && ctx != nil {
		cmddesc := ctx.Value("cmddesc")
		if cmddesc != nil {
			redisCmd.CmdDesc, _ = cmddesc.(string)
		}
	}

	entry := time.Now()
	con := r.pool.Get()
	defer con.Close()
	redisCmd.Reply, redisCmd.Err = script.script.Do(con, redisCmd.Args...)
	redisCmd.Elapsed = time.Since(entry)

	if redisCmd.Err != nil {
		log.Error().Err(redisCmd.Err).Int32("elapsed", int32(redisCmd.Elapsed/time.Millisecond)).
			Str("cmd", redisCmd.CmdString()).
			Str("pos", redisCmd.Caller.Pos()).
			Msg("Redis doScript fail")
	} else {
		logOut := true
		if ctx != nil {
			nolog := ctx.Value(CtxKey_nolog)
			if nolog != nil {
				logOut = false
			}
		}
		if logOut {
			log.Debug().Int32("elapsed", int32(redisCmd.Elapsed/time.Millisecond)).
				Str("cmd", redisCmd.CmdString()).
				Str("pos", redisCmd.Caller.Pos()).
				Str("reply", redisCmd.ReplyString()).
				Msg("Redis doScript success")
		}
	}

	// 回调
	for _, f := range r.hook {
		f(ctx, redisCmd)
	}
}

func (r *Redis) DoScriptInt(ctx context.Context, script *RedisScript, keysAndArgs ...interface{}) (int, error) {
	var result int
	redisCmd := r.doScript(ctx, script, keysAndArgs...)
	if redisCmd.Err != nil {
		return result, redisCmd.Err
	}
	result, redisCmd.Err = redis.Int(redisCmd.Reply, redisCmd.Err)
	if redisCmd.Err != nil && redisCmd.Err != redis.ErrNil {
		log.Error().Err(redisCmd.Err).
			Str("cmd", redisCmd.CmdString()).
			Str("pos", redisCmd.Caller.Pos()).
			Str("reply", redisCmd.ReplyString()).
			Msg("Redis DoScriptInt fail")
	}
	return result, redisCmd.Err
}

func (r *Redis) DoScriptInt64(ctx context.Context, script *RedisScript, keysAndArgs ...interface{}) (int64, error) {
	var result int64
	redisCmd := r.doScript(ctx, script, keysAndArgs...)
	if redisCmd.Err != nil {
		return result, redisCmd.Err
	}
	result, redisCmd.Err = redis.Int64(redisCmd.Reply, redisCmd.Err)
	if redisCmd.Err != nil && redisCmd.Err != redis.ErrNil {
		log.Error().Err(redisCmd.Err).
			Str("cmd", redisCmd.CmdString()).
			Str("pos", redisCmd.Caller.Pos()).
			Str("reply", redisCmd.ReplyString()).
			Msg("Redis DoScriptInt64 fail")
	}
	return result, redisCmd.Err
}

func (r *Redis) DoScriptUint64(ctx context.Context, script *RedisScript, keysAndArgs ...interface{}) (uint64, error) {
	var result uint64
	redisCmd := r.doScript(ctx, script, keysAndArgs...)
	if redisCmd.Err != nil {
		return result, redisCmd.Err
	}
	result, redisCmd.Err = redis.Uint64(redisCmd.Reply, redisCmd.Err)
	if redisCmd.Err != nil && redisCmd.Err != redis.ErrNil {
		log.Error().Err(redisCmd.Err).
			Str("cmd", redisCmd.CmdString()).
			Str("pos", redisCmd.Caller.Pos()).
			Str("reply", redisCmd.ReplyString()).
			Msg("Redis DoScriptUint64 fail")
	}
	return result, redisCmd.Err
}

func (r *Redis) DoScriptFloat64(ctx context.Context, script *RedisScript, keysAndArgs ...interface{}) (float64, error) {
	var result float64
	redisCmd := r.doScript(ctx, script, keysAndArgs...)
	if redisCmd.Err != nil {
		return result, redisCmd.Err
	}
	result, redisCmd.Err = redis.Float64(redisCmd.Reply, redisCmd.Err)
	if redisCmd.Err != nil && redisCmd.Err != redis.ErrNil {
		log.Error().Err(redisCmd.Err).
			Str("cmd", redisCmd.CmdString()).
			Str("pos", redisCmd.Caller.Pos()).
			Str("reply", redisCmd.ReplyString()).
			Msg("Redis DoScriptFloat64 fail")
	}
	return result, redisCmd.Err
}

func (r *Redis) DoScriptString(ctx context.Context, script *RedisScript, keysAndArgs ...interface{}) (string, error) {
	var result string
	redisCmd := r.doScript(ctx, script, keysAndArgs...)
	if redisCmd.Err != nil {
		return result, redisCmd.Err
	}
	result, redisCmd.Err = redis.String(redisCmd.Reply, redisCmd.Err)
	if redisCmd.Err != nil && redisCmd.Err != redis.ErrNil {
		log.Error().Err(redisCmd.Err).
			Str("cmd", redisCmd.CmdString()).
			Str("pos", redisCmd.Caller.Pos()).
			Str("reply", redisCmd.ReplyString()).
			Msg("Redis DoScriptString fail")
	}
	return result, redisCmd.Err
}

func (r *Redis) DoScriptBytes(ctx context.Context, script *RedisScript, keysAndArgs ...interface{}) ([]byte, error) {
	var result []byte
	redisCmd := r.doScript(ctx, script, keysAndArgs...)
	if redisCmd.Err != nil {
		return result, redisCmd.Err
	}
	result, redisCmd.Err = redis.Bytes(redisCmd.Reply, redisCmd.Err)
	if redisCmd.Err != nil && redisCmd.Err != redis.ErrNil {
		log.Error().Err(redisCmd.Err).
			Str("cmd", redisCmd.CmdString()).
			Str("pos", redisCmd.Caller.Pos()).
			Str("reply", redisCmd.ReplyString()).
			Msg("Redis DoScriptBytes fail")
	}
	return result, redisCmd.Err
}

func (r *Redis) DoScriptBool(ctx context.Context, script *RedisScript, keysAndArgs ...interface{}) (bool, error) {
	var result bool
	redisCmd := r.doScript(ctx, script, keysAndArgs...)
	if redisCmd.Err != nil {
		return result, redisCmd.Err
	}
	result, redisCmd.Err = redis.Bool(redisCmd.Reply, redisCmd.Err)
	if redisCmd.Err != nil && redisCmd.Err != redis.ErrNil {
		log.Error().Err(redisCmd.Err).
			Str("cmd", redisCmd.CmdString()).
			Str("pos", redisCmd.Caller.Pos()).
			Str("reply", redisCmd.ReplyString()).
			Msg("Redis DoScriptBool fail")
	}
	return result, redisCmd.Err
}

func (r *Redis) DoScriptFloat64s(ctx context.Context, script *RedisScript, keysAndArgs ...interface{}) ([]float64, error) {
	var result []float64
	redisCmd := r.doScript(ctx, script, keysAndArgs...)
	if redisCmd.Err != nil {
		return result, redisCmd.Err
	}
	result, redisCmd.Err = redis.Float64s(redisCmd.Reply, redisCmd.Err)
	if redisCmd.Err != nil && redisCmd.Err != redis.ErrNil {
		log.Error().Err(redisCmd.Err).
			Str("cmd", redisCmd.CmdString()).
			Str("pos", redisCmd.Caller.Pos()).
			Str("reply", redisCmd.ReplyString()).
			Msg("Redis DoScriptFloat64s fail")
	}
	return result, redisCmd.Err
}

func (r *Redis) DoScriptStrings(ctx context.Context, script *RedisScript, keysAndArgs ...interface{}) ([]string, error) {
	var result []string
	redisCmd := r.doScript(ctx, script, keysAndArgs...)
	if redisCmd.Err != nil {
		return result, redisCmd.Err
	}
	result, redisCmd.Err = redis.Strings(redisCmd.Reply, redisCmd.Err)
	if redisCmd.Err != nil && redisCmd.Err != redis.ErrNil {
		log.Error().Err(redisCmd.Err).
			Str("cmd", redisCmd.CmdString()).
			Str("pos", redisCmd.Caller.Pos()).
			Str("reply", redisCmd.ReplyString()).
			Msg("Redis DoScriptStrings fail")
	}
	return result, redisCmd.Err
}

func (r *Redis) DoScriptByteSlices(ctx context.Context, script *RedisScript, keysAndArgs ...interface{}) ([][]byte, error) {
	var result [][]byte
	redisCmd := r.doScript(ctx, script, keysAndArgs...)
	if redisCmd.Err != nil {
		return result, redisCmd.Err
	}
	result, redisCmd.Err = redis.ByteSlices(redisCmd.Reply, redisCmd.Err)
	if redisCmd.Err != nil && redisCmd.Err != redis.ErrNil {
		log.Error().Err(redisCmd.Err).
			Str("cmd", redisCmd.CmdString()).
			Str("pos", redisCmd.Caller.Pos()).
			Str("reply", redisCmd.ReplyString()).
			Msg("Redis DoScriptByteSlices fail")
	}
	return result, redisCmd.Err
}

func (r *Redis) DoScriptInt64s(ctx context.Context, script *RedisScript, keysAndArgs ...interface{}) ([]int64, error) {
	var result []int64
	redisCmd := r.doScript(ctx, script, keysAndArgs...)
	if redisCmd.Err != nil {
		return result, redisCmd.Err
	}
	result, redisCmd.Err = redis.Int64s(redisCmd.Reply, redisCmd.Err)
	if redisCmd.Err != nil && redisCmd.Err != redis.ErrNil {
		log.Error().Err(redisCmd.Err).
			Str("cmd", redisCmd.CmdString()).
			Str("pos", redisCmd.Caller.Pos()).
			Str("reply", redisCmd.ReplyString()).
			Msg("Redis DoScriptInt64s fail")
	}
	return result, redisCmd.Err
}

func (r *Redis) DoScriptInts(ctx context.Context, script *RedisScript, keysAndArgs ...interface{}) ([]int, error) {
	var result []int
	redisCmd := r.doScript(ctx, script, keysAndArgs...)
	if redisCmd.Err != nil {
		return result, redisCmd.Err
	}
	result, redisCmd.Err = redis.Ints(redisCmd.Reply, redisCmd.Err)
	if redisCmd.Err != nil && redisCmd.Err != redis.ErrNil {
		log.Error().Err(redisCmd.Err).
			Str("cmd", redisCmd.CmdString()).
			Str("pos", redisCmd.Caller.Pos()).
			Str("reply", redisCmd.ReplyString()).
			Msg("Redis DoScriptInts fail")
	}
	return result, redisCmd.Err
}

func (r *Redis) DoScriptStringMap(ctx context.Context, script *RedisScript, keysAndArgs ...interface{}) (map[string]string, error) {
	var result map[string]string
	redisCmd := r.doScript(ctx, script, keysAndArgs...)
	if redisCmd.Err != nil {
		return result, redisCmd.Err
	}
	result, redisCmd.Err = redis.StringMap(redisCmd.Reply, redisCmd.Err)
	if redisCmd.Err != nil && redisCmd.Err != redis.ErrNil {
		log.Error().Err(redisCmd.Err).
			Str("cmd", redisCmd.CmdString()).
			Str("pos", redisCmd.Caller.Pos()).
			Str("reply", redisCmd.ReplyString()).
			Msg("Redis DoScriptStringMap fail")
	}
	return result, redisCmd.Err
}

func (r *Redis) DoScriptIntMap(ctx context.Context, script *RedisScript, keysAndArgs ...interface{}) (map[string]int, error) {
	var result map[string]int
	redisCmd := r.doScript(ctx, script, keysAndArgs...)
	if redisCmd.Err != nil {
		return result, redisCmd.Err
	}
	result, redisCmd.Err = redis.IntMap(redisCmd.Reply, redisCmd.Err)
	if redisCmd.Err != nil && redisCmd.Err != redis.ErrNil {
		log.Error().Err(redisCmd.Err).
			Str("cmd", redisCmd.CmdString()).
			Str("pos", redisCmd.Caller.Pos()).
			Str("reply", redisCmd.ReplyString()).
			Msg("Redis DoScriptIntMap fail")
	}
	return result, redisCmd.Err
}

func (r *Redis) DoScriptInt64Map(ctx context.Context, script *RedisScript, keysAndArgs ...interface{}) (map[string]int64, error) {
	var result map[string]int64
	redisCmd := r.doScript(ctx, script, keysAndArgs...)
	if redisCmd.Err != nil {
		return result, redisCmd.Err
	}
	result, redisCmd.Err = redis.Int64Map(redisCmd.Reply, redisCmd.Err)
	if redisCmd.Err != nil && redisCmd.Err != redis.ErrNil {
		log.Error().Err(redisCmd.Err).
			Str("cmd", redisCmd.CmdString()).
			Str("pos", redisCmd.Caller.Pos()).
			Str("reply", redisCmd.ReplyString()).
			Msg("Redis DoScriptInt64Map fail")
	}
	return result, redisCmd.Err
}
