package mrcache

// https://github.com/yuwf/gobase

import "github.com/yuwf/gobase/goredis"

// 【注意】
// lua层返回return nil 或者直接return， goredis都会识别为空值，即redis.Nil
// local rst = redis.call 如果命令出错会直接返回error，不会再给rst了
// hmset的返回值有点坑，在lua中返回的table n['ok']='OK'
// 空值nil不要写入到redis中，给reids写nil值时，redis会写入空字符串，对一些自增类型的值，后面自增会有问题

// 自增 总key
// 参数：第一个自增的field，正常情况用tablename，第二个参数表示拆表的个数(tablecount，0:不拆表)，第三个表示第几个表
// 返回值：err=nil时 自增值
var incrScript = goredis.NewScript(`
	local tableCount = tonumber(ARGV[2])
	local incrIndex = tonumber(ARGV[3])
	if tableCount == 0 then
		return redis.call('HINCRBY', KEYS[1], ARGV[1], 1)
	end
	local rst = redis.call('HINCRBY', KEYS[1], ARGV[1], tableCount)
	local mod = rst % tableCount
	if mod ~= incrIndex then
		rst = rst - mod + incrIndex
		redis.call('HSET', KEYS[1], ARGV[1], rst)
	end
	return rst
`)

// row /////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// 读取数据
// key：生成的key key不存在返回值为空
// 参数：第一个是有效期 其他：field field ..
// 返回值：err=nil时 1:空 数据为空   2:value value .. 和上面 field 对应，不存在对应的Value填充nil
var rowGetScript = goredis.NewScript(`
	local rst = redis.call('EXPIRE', KEYS[1], ARGV[1])
	if rst == 0 then
		return
	end
	return redis.call('HMGET', KEYS[1], select(2,unpack(ARGV)))
`)

// row 新增数据，直接保存
// key：生成的key
// 参数：第一个是有效期 其他: field value field value ..
// 返回值：err=nil时 OK
var rowAddScript = goredis.NewScript(`
	redis.call('HMSET', KEYS[1], select(2,unpack(ARGV)))
	redis.call('EXPIRE', KEYS[1], ARGV[1])
	return 'OK'
`)

// row 修改数据 没有返回值
// key：生成的key，key不存在返回值为空
// 参数：第一个是有效期 第二个参数无效 其他: field op value field op value ..
// 返回值 err=nil时 1：空：数据为空  2：OK
var rowModifyScript = goredis.NewScript(`
	local rst = redis.call('EXPIRE', KEYS[1], ARGV[1])
	if rst == 0 then
		return
	end
	if #ARGV == 1 then -- 只有一个过期时间
		return 'OK'
	end
	local setkv = {}
	for i = 3, #ARGV, 3 do
		if ARGV[i+1] == "del" then
			redis.call('HDEL', KEYS[1], ARGV[i])
		elseif ARGV[i+1] == "set" then
			setkv[#setkv+1] = ARGV[i]
			setkv[#setkv+1] = ARGV[i+2]
		elseif ARGV[i+1] == "incr" then
			redis.call('HINCRBY', KEYS[1], ARGV[i], ARGV[i+2])
		elseif ARGV[i+1] == "fincr" then
			redis.call('HINCRBYFLOAT', KEYS[1], ARGV[i], ARGV[i+2])
		end
	end
	if #setkv > 0 then
		redis.call('HMSET', KEYS[1], unpack(setkv))
	end
	return 'OK'
`)

// row 修改数据 有返回值
// key：生成的key，key不存在返回值为空
// 参数：第一个是有效期 第二个参数空 其他: field op value field op value ..
// 返回值：err=nil时 1：空：没加载数据 2：value value .. 和上面field对应
var rowModifyGetScript = goredis.NewScript(`
	local rst = redis.call('EXPIRE', KEYS[1], ARGV[1])
	if rst == 0 then
		return
	end
	local fields = {}
	local setkv = {}
	for i = 3, #ARGV, 3 do
		fields[#fields+1] = ARGV[i]
		if ARGV[i+1] == "del" then
			redis.call('HDEL', KEYS[1], ARGV[i])
		elseif ARGV[i+1] == "set" then
			setkv[#setkv+1] = ARGV[i]
			setkv[#setkv+1] = ARGV[i+2]
		elseif ARGV[i+1] == "incr" then
			redis.call('HINCRBY', KEYS[1], ARGV[i], ARGV[i+2])
		elseif ARGV[i+1] == "fincr" then
			redis.call('HINCRBYFLOAT', KEYS[1], ARGV[i], ARGV[i+2])
		end
	end
	if #setkv > 0 then
		redis.call('HMSET', KEYS[1], unpack(setkv))
	end
	if #fields == 0 then
		return {}
	end
	-- 返回最新的值
	return redis.call('HMGET', KEYS[1], unpack(fields))
`)

// rows /////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// rows 读取数据
// key：索引key
// 参数：第一个是有效期 其他：field field ..
// 返回值：err=nil时 1:空 数据为空  2:{value value ..} {value value ..} .. {}的个数为结果数量 每个{value value}的和上面的field对应，不存在对应的value填充nil
var rowsGetAllScript = goredis.NewScript(`
	local keyValuesStrs = redis.call('SMEMBERS', KEYS[1])
	if #keyValuesStrs == 0 then
		return
	end
	-- 判断key是否存在
	local dataKeys = {}
	for i = 1, #keyValuesStrs do
		dataKeys[i] = KEYS[1] .. "_" .. keyValuesStrs[i]
		local rst = redis.call('EXPIRE', dataKeys[i], ARGV[1])
		if rst == 0 then
			return -- 数据不一致了 返回空 重新读
		end
	end
	redis.call('EXPIRE', KEYS[1], ARGV[1])
	-- 读取key
	local resp = {}
	for i = 1, #dataKeys do
		resp[i] = redis.call('HMGET', dataKeys[i], select(2,unpack(ARGV)))
	end
	return resp
`)

// rows 读取多条数据
// key：索引key
// 参数：第一个是有效期 num keyValuesStr keyValuesStr ..  其他：field field ..
// 返回值：err=nil时 1:空 数据为空  2:{value value ..} {value value ..} .. {}的个数为结果数量 每个{value value}的和上面的keyValuesStr对应，不存在对应的Value填充nil
// 任何一个keyValuesStr对应的datakey不存在返回值为空
var rowsGetsScript = goredis.NewScript(`
	-- 先判断索引key是否存在
	local rst = redis.call('EXPIRE', KEYS[1], ARGV[1])
	if rst == 0 then
		return
	end
	local num = tonumber(ARGV[2])
	-- 判断索引中和对应的key是否存在
	local dataKeys = {}
	for i = 1, num do
		local keyValuesStr = ARGV[2+i]
		if redis.call("SISMEMBER", KEYS[1], keyValuesStr) == 0 then
			return
		end
		dataKeys[i] = KEYS[1] .. "_" .. keyValuesStr
		local rst = redis.call('EXPIRE', dataKeys[i], ARGV[1])
		if rst == 0 then
			return -- 数据不一致了 返回空 重新读
		end
	end
	-- 读取key
	local resp = {}
	for i = 1, #dataKeys do
		resp[i] = redis.call('HMGET', dataKeys[i], select(2+num+1,unpack(ARGV)))
	end
	return resp
`)

// rows 读取数据
// key：索引key
// 参数：第一个是有效期 第二个为keyValuesStr 其他：field field .. 要获取的字段
// 返回值 err=nil时 1：空：没加载数据 2：value value .. 和上面field对应
var rowsGetScript = goredis.NewScript(`
	local rst = redis.call('EXPIRE', KEYS[1], ARGV[1])
	if rst == 0 then
		return
	end
	-- 判断索引中和对应的key是否存在
	local keyValuesStr = ARGV[2]
	if redis.call("SISMEMBER", KEYS[1], keyValuesStr) == 0 then
		return
	end
	local dataKey = KEYS[1] .. "_" .. keyValuesStr
	local rst = redis.call('EXPIRE', dataKey, ARGV[1])
	if rst == 0 then
		return -- 数据不一致了 返回空 重新读
	end
	-- 读取key
	return redis.call('HMGET', dataKey, select(3,unpack(ARGV)))
`)

// rows 新增数据
// key：索引key
// 参数：第一个是有效期 其他为数据组：keyValuesStr num field value field value ..  keyValuesStr num  field value field value ..
// 返回值 err=nil时 OK
var rowsAddScript = goredis.NewScript(`
	local keyValuesStrs = {}
	local pos = 2
	while pos < #ARGV do
		local keyValuesStr = ARGV[pos]
		pos = pos + 1
		local num = tonumber(ARGV[pos])
		pos = pos + 1

		local dataKey = KEYS[1] .. "_" .. keyValuesStr
		keyValuesStrs[#keyValuesStrs+1] = keyValuesStr
		if num > 0 then
			local kv = {}
			for i = 1, num do
				kv[#kv+1] = ARGV[pos]
				pos = pos + 1
			end
			redis.call('HMSET', dataKey, unpack(kv))
			redis.call('EXPIRE', dataKey, ARGV[1])
		end
	end

	-- 保存到索引key
	redis.call('SADD', KEYS[1], unpack(keyValuesStrs))
	redis.call('EXPIRE', KEYS[1], ARGV[1])
	return 'OK'
`)

// rows 读取数据
// key：索引key
// 参数：第一个是有效期 第二个为keyValuesStr
// 返回值 err=nil时 1：空：没加载数据 2:0或者1 1存在
var rowsExistScript = goredis.NewScript(`
	-- 先判断索引key是否存在
	local rst = redis.call('EXPIRE', KEYS[1], ARGV[1])
	if rst == 0 then
		return
	end
	local keyValuesStr = ARGV[2]
	if redis.call("SISMEMBER", KEYS[1], keyValuesStr) == 0 then
		return 0
	end
	return 1
`)

// rows 删除数据
// key：索引key
// 参数：keyValuesStr keyValuesStr ...
// 返回值：err=nil时 OK
var rowsDelsScript = goredis.NewScript(`
	-- 删除索引key中的数据
	local count = redis.call('SREM', KEYS[1], unpack(ARGV))
	-- 删除索引key
	local dataKeys = {}
	for i = 1, #ARGV do
		dataKeys[i] = KEYS[1] .. "_" .. ARGV[i]
	end
	redis.call('DEL', unpack(dataKeys))
	return count
`)

// rows 删除全部数据
// key：索引key
// 返回值：err=nil时 删除数据的个数
var rowsDelAllScript = goredis.NewScript(`
	local keyValuesStrs = redis.call('SMEMBERS', KEYS[1])
	if #keyValuesStrs == 0 then
		return 0
	end
	local dataKeys = {KEYS[1]}
	for i = 1, #keyValuesStrs do
		dataKeys[i+1] = KEYS[1] .. "_" .. keyValuesStrs[i]
	end
	redis.call('DEL', unpack(dataKeys))
	return #dataKeys - 1
`)

// row 设置数据 没有返回值
// key：索引key
// 参数：第一个是有效期 第二个为keyValuesStr 其他: field op value field op value ..
// 返回值：err=nil时 1：空：数据为空  2：OK
var rowsModifyScript = goredis.NewScript(`
	local rst = redis.call('EXPIRE', KEYS[1], ARGV[1])
	if rst == 0 then
		return
	end
	-- 判断索引中和对应的key是否存在
	local keyValuesStr = ARGV[2]
	if redis.call("SISMEMBER", KEYS[1], keyValuesStr) == 0 then
		return
	end
	local dataKey = KEYS[1] .. "_" .. keyValuesStr
	local rst = redis.call('EXPIRE', dataKey, ARGV[1])
	if rst == 0 then
		return -- 数据不一致了 返回空 重新读
	end

	local setkv = {}
	for i = 3, #ARGV, 3 do
		if ARGV[i+1] == "del" then
			redis.call('HDEL', dataKey, ARGV[i])
		elseif ARGV[i+1] == "set" then
			setkv[#setkv+1] = ARGV[i]
			setkv[#setkv+1] = ARGV[i+2]
		elseif ARGV[i+1] == "incr" then
			redis.call('HINCRBY', dataKey, ARGV[i], ARGV[i+2])
		elseif ARGV[i+1] == "fincr" then
			redis.call('HINCRBYFLOAT', dataKey, ARGV[i], ARGV[i+2])
		end
	end
	if #setkv > 0 then
		redis.call('HMSET', dataKey, unpack(setkv))
	end
	return 'OK'
`)

// rows 修改数据 有返回值
// key：索引key
// 参数：第一个是有效期 第二个为keyValuesStr 其他: field op value field op value ..
// 返回值：err=nil时 1：空：没加载数据 2：value value .. 和上面field对应
var rowsModifyGetScript = goredis.NewScript(`
	local rst = redis.call('EXPIRE', KEYS[1], ARGV[1])
	if rst == 0 then
		return
	end
	-- 判断索引中和对应的key是否存在
	local keyValuesStr = ARGV[2]
	if redis.call("SISMEMBER", KEYS[1], keyValuesStr) == 0 then
		return
	end
	local dataKey = KEYS[1] .. "_" .. keyValuesStr
	local rst = redis.call('EXPIRE', dataKey, ARGV[1])
	if rst == 0 then
		return -- 数据不一致了 返回空 重新读
	end

	local fields = {}
	local setkv = {}
	for i = 3, #ARGV, 3 do
		fields[#fields+1] = ARGV[i]
		if ARGV[i+1] == "del" then
			redis.call('HDEL', dataKey, ARGV[i])
		elseif ARGV[i+1] == "set" then
			setkv[#setkv+1] = ARGV[i]
			setkv[#setkv+1] = ARGV[i+2]
		elseif ARGV[i+1] == "incr" then
			redis.call('HINCRBY', dataKey, ARGV[i], ARGV[i+2])
		elseif ARGV[i+1] == "fincr" then
			redis.call('HINCRBYFLOAT', dataKey, ARGV[i], ARGV[i+2])
		end
	end
	if #setkv > 0 then
		redis.call('HMSET', dataKey, unpack(setkv))
	end
	if #fields == 0 then
		return {}
	end
	-- 返回最新的值
	return redis.call('HMGET', dataKey, unpack(fields))
`)

// column /////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// column 新增数据，直接保存
// key：生成的key
// 参数：第一个是有效期 其他：field value field value
// 返回值：err=nil时 OK
var columnAddScript = goredis.NewScript(`
	redis.call('HMSET', KEYS[1], select(2,unpack(ARGV)))
	redis.call('EXPIRE', KEYS[1], ARGV[1])
	return 'OK'
`)

// column 新增数据，直接保存
// key：生成的key
// 参数：第一个是有效期 其他：field value
// 返回值：err=nil时 OK  空不存在
var columnSetScript = goredis.NewScript(`
	local exists = redis.call('HEXISTS', key, ARGV[2])
	if exists == 0 then
		return
	end
	redis.call('HMSET', KEYS[1], ARGV[2], ARGV[3])
	redis.call('EXPIRE', KEYS[1], ARGV[1])
	return 'OK'
`)
