package mrcache

// https://github.com/yuwf/gobase

import (
	"github.com/yuwf/gobase/goredis"
)

// 【注意】
// lua层返回return nil 或者直接return， goredis都会识别为空值，即redis.Nil
// local rst = redis.call 如果命令出错会直接返回error，不会再给rst了
// hmset的返回值有点坑，在lua中返回的table n['ok']='OK'
// 空值nil不要写入到redis中，给reids写nil值时，redis会写入空字符串，对一些自增类型的值，后面自增会有问题，所以空值直接删除字段

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

// dirty /////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// 添加脏数据
// KEYS[1]：脏数据列表key
// ARGV[1..]：脏数据key ..
var dirtyKeyAddScript = goredis.NewScript(`
	redis.replicate_commands()
	local t = redis.call('TIME')
	local stamp = tonumber(t[1]) + tonumber(t[2])/1000000

	local set = {}
	for i = 1, #ARGV do
		set[#set + 1] = stamp
		set[#set + 1] = ARGV[i]
	end

	if #set > 0 then
		redis.call("ZADD", KEYS[1], "NX", unpack(set))
	end
	return 'OK'
`)

// 获取脏数据key
// KEYS[1]：脏数据列表key
// KEYS[2]：正在处理的任务key
// KEYS[3]：上次过期检查时间key
// ARGV[1]：最多同时处理的数量
// ARGV[2]：处理超时时间，单位：秒，超时的会重新放入dirty列表
// ARGV[3] worker uuid
// 返回值：脏数据key ..
var dirtyKeyGetScript = goredis.NewScript(`
	redis.replicate_commands()
	local t = redis.call('TIME')
	local stamp = tonumber(t[1]) + tonumber(t[2])/1000000

	local dirtyKey = KEYS[1]
	local processingKey = KEYS[2]  -- 正在处理的任务
	local lastCheckKey = KEYS[3]   -- 上次过期检查时间
	local maxProcessing = tonumber(ARGV[1])
	local timeout = tonumber(ARGV[2])
	local uuid = ARGV[3]

	local processingMap
	local processingCount = 0

	-- 先处理超时任务，1秒只处理一次
	if redis.call("SET", lastCheckKey, stamp, "NX", "EX", 1) then
		local processing = redis.call("HGETALL", processingKey)
		processingMap = {}

		local delFields = {}
		local dirtyArgs = {}
		for i = 1, #processing, 2 do
			local key = processing[i]
			local value = processing[i + 1]

			local pos = string.find(value, "|", 1, true)
			local beginStamp
			if pos then
				beginStamp = tonumber(string.sub(value, 1, pos - 1))
			end
			if not beginStamp then
				delFields[#delFields + 1] = key -- processing格式异常(stamp|uuid)，直接清理
			elseif beginStamp + timeout <= stamp then
				delFields[#delFields + 1] = key
				dirtyArgs[#dirtyArgs + 1] = stamp
				dirtyArgs[#dirtyArgs + 1] = key
			else
				processingMap[key] = true
				processingCount = processingCount + 1
			end
		end

		if #delFields > 0 then
			redis.call("HDEL", processingKey, unpack(delFields))
			if #dirtyArgs > 0 then
				redis.call("ZADD", dirtyKey, "NX", unpack(dirtyArgs))
			end
		end
	end

	local keys = redis.call("ZRANGE", dirtyKey, 0, maxProcessing * 2 - 1) -- 每次多获取一些，防止队列任务开头正好是正在处理的任务，影响并发
	if #keys == 0 then
		return {}
	end

	if not processingMap then
		-- 读取下正在处理的任务
		local processing = redis.call("HKEYS", processingKey)
		processingMap = {}
		processingCount = #processing
		for i = 1, #processing do
			processingMap[processing[i]] = true
		end
	end
	-- 并发限制
	if processingCount >= maxProcessing then
		return {}
	end
	local canMove = maxProcessing - processingCount

	local moved = {}
	local processingArgs = {}
	for i = 1, #keys do
		local key = keys[i]
		if not processingMap[key] then
			moved[#moved + 1] = key
			processingArgs[#processingArgs + 1] = key
			processingArgs[#processingArgs + 1] = tostring(stamp) .. "|" .. uuid

			if #moved >= canMove then
				break
			end
		end
	end

	if #moved > 0 then
		redis.call("ZREM", dirtyKey, unpack(moved))
		redis.call("HSET", processingKey, unpack(processingArgs))
	end
	return moved
`)

// 任务处理完成
// KEYS[1]：脏数据正在处理列表key
// ARGV[1]：脏数据key
// ARGV[2]：worker uuid
// 返回值：
// 0：任务不存在
// 1：成功完成
// 2：uuid不匹配（任务已超时，被其他worker重新领取）
var dirtyKeyDoneScript = goredis.NewScript(`
	local processingKey = KEYS[1]
	local key = ARGV[1]
	local uuid = ARGV[2]

	local value = redis.call("HGET", processingKey, key)
	if not value then
		return 0
	end

	local pos = string.find(value, "|", 1, true)
	local beginStamp
	if pos then
		beginStamp = tonumber(string.sub(value, 1, pos - 1))
	end
	if not beginStamp then
		redis.call("HDEL", processingKey, key) -- processing格式异常(stamp|uuid)，直接清理
		return 0
	end

	local owner = string.sub(value, pos + 1)
	if owner ~= uuid then
		return 2
	end

	redis.call("HDEL", processingKey, key)
	return 1
`)

// 获取脏数据
// KEYS[1]：相关的key
// ARGV[1...]：field field .. 额外读取的字段
// 返回值：
// - redis.Nil:数据为空
// - {version,{field value ..}}
var dirtyDataGetScript = goredis.NewScript(`
	local key = KEYS[1]
	local values = redis.call("HMGET", key, "_dirty_", "_dver_")
	local dirty = values[1]
	if not dirty or dirty == "" then
		return {0,{}}
	end

	local dver = tonumber(values[2]) or 0
	local fields = {}
	for field in string.gmatch(dirty, "[^,]+") do
		fields[#fields + 1] = field
	end
	if #fields == 0 then
		return {0,{}}
	end

	-- 追加额外字段
	for i = 1, #ARGV do
		fields[#fields + 1] = ARGV[i]
	end

	values  = redis.call("HMGET", key, unpack(fields))
	local result = {}
	for i = 1, #fields do
		result[#result + 1] = fields[i]
		result[#result + 1] = values [i]
	end
	return {dver,result}
`)

// 清除脏标记
// KEYS[1]：相关的key
// ARGV[1]：dirty version
// 返回值：
// 0：数据不存在或版本不一致，未清除
// 1：清除成功
var dirtyDataDoneScript = goredis.NewScript(`
	local key = KEYS[1]
	local expectVersion = tonumber(ARGV[1]) or 0

	local currentVersion = tonumber(redis.call("HGET", key, "_dver_")) or 0
	if currentVersion ~= expectVersion then
		return 0
	end

	redis.call("HDEL", key, "_dirty_")
	return 1
`)

// row /////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// 读取数据
// KEYS[1]：生成的key
// ARGV[1]：有效期
// ARGV[2...]：获取的字段 field field ..
// 返回值：
// - redis.Nil: 数据为空
// - {value value ..} 和上面 field 对应，不存在对应的Value填充nil
var rowGetScript = goredis.NewScript(`
	local rst = redis.call('EXPIRE', KEYS[1], ARGV[1])
	if rst == 0 then
		return
	end
	return redis.call('HMGET', KEYS[1], select(2,unpack(ARGV)))
`)

// row 新增数据，如果Redis存在则不覆盖
// KEYS[1]：生成的key
// ARGV[1]：有效期
// ARGV[2...]：获取的字段 field value field value ..
// 返回值：
// - redis.Nil: 数据已存在，不覆盖
// - OK
var rowAddScript = goredis.NewScript(`
	local key = KEYS[1]
	local rst = redis.call('EXPIRE', key, ARGV[1])
	if rst ~= 0 then
		return -- 数据已存在，不覆盖
	end
	redis.call('HMSET', key, select(2,unpack(ARGV)))
	return 'OK'
`)

// row 修改数据
// KEYS[1]：生成的key
// ARGV[1]：有效期
// ARGV[2]：1-写入脏标记 其他-不写入脏标记
// ARGV[3...]：修改的字段 field op value field op value ..
// op: get del,set,incr,fincr
var rowModifyCommon = `
	local key = KEYS[1]
	local rst = redis.call('EXPIRE', key, ARGV[1])
	if rst == 0 then
		return
	end

	local writeDirty = ARGV[2] == "1"
	local dirty, dirty2
	local dver
	if writeDirty then
		local values = redis.call("HMGET", key, "_dirty_", "_dver_")
		dirty = values[1] or ""
		dver = tonumber(values[2]) or 0
		dirty2 = ',' .. dirty .. ',' -- 首尾有添加了逗号，仅用于查找, 调用方保证不会传入重复 field，因此无需更新 dirty2
	end

	local dirtyChanged = false
	local dverChanged = false
	local setkv = {}
	for i = 3, #ARGV, 3 do
		local field = ARGV[i]
		local op = ARGV[i + 1]
		local value = ARGV[i + 2]

		local changed = false
		if op == "del" then
			redis.call("HDEL", key, field)
			changed = true
		elseif op == "set" then
			setkv[#setkv+1] = field
			setkv[#setkv+1] = value
			changed = true
		elseif op == "incr" then
			redis.call("HINCRBY", key, field, value)
			changed = true
		elseif op == "fincr" then
			redis.call("HINCRBYFLOAT", key, field, value)
			changed = true
		end

		if changed and writeDirty then
			-- 脏标记，记录修改的字段
			local token = "," .. field .. ","
			if not string.find(dirty2, token, 1, true) then
				if #dirty == 0 then
					dirty = field
				else
					dirty = dirty .. "," .. field
				end
				dirtyChanged = true
			end
			dverChanged = true
		end
	end

	if dirtyChanged then
		setkv[#setkv+1] = '_dirty_'
		setkv[#setkv+1] = dirty
	end
	if dverChanged then
		setkv[#setkv+1] = '_dver_'
		setkv[#setkv+1] = dver + 1
	end
	if #setkv > 0 then
		redis.call('HSET', key, unpack(setkv))
	end
`

// 返回值：
// - redis.Nil:数据为空
// - OK
var rowModifyScript = goredis.NewScript(rowModifyCommon + `
	return 'OK'
`)

// 返回值
// - redis.Nil:数据为空
// - {value value ..} 和参数field对应
var rowModifyGetScript = goredis.NewScript(rowModifyCommon + `
	local fields = {}
	for i = 3, #ARGV, 3 do
		fields[#fields+1] = ARGV[i]
	end
	if #fields == 0 then
		return {}
	end
	return redis.call('HMGET', key, unpack(fields))
`)

// 修改json数组字段，支持多个字段同时修改
// KEYS[1]：生成的key
// ARGV[1]：有效期
// ARGV[2]：1-写入脏标记 其他-不写入脏标记
// ARGV[3]：1-去重 其他-不去重
// ARGV[4...]：其他: field op num value..  field op num value..
// - redis.Nil:数据为空
// - {value value ..} {value value ..} ... 返回修改的值列表和参数field对应
var rowJsonArrayModifyScript = goredis.NewScript(goredis.LuaJsonScript + `
	local key = KEYS[1]
	local rst = redis.call('EXPIRE', key, ARGV[1])
	if rst == 0 then
		return -- 数据不一致了 返回空 重新读
	end

	local writeDirty = ARGV[2] == "1"
	local dirty, dirty2
	local dver
	if writeDirty then
		local values = redis.call("HMGET", key, "_dirty_", "_dver_")
		dirty = values[1] or ""
		dver = tonumber(values[2]) or 0
		dirty2 = ',' .. dirty .. ',' -- 首尾有添加了逗号，仅用于查找, 调用方保证不会传入重复 field，因此无需更新 dirty2
	end

	local duplicate = ARGV[3] == "1"

	local dirtyChanged = false
	local dverChanged = false
	local setkv = {}
	rst = {} -- 返回修改的值列表
	local pos = 4
	while pos < #ARGV do
		local field = ARGV[pos]
		pos = pos + 1
		local op = ARGV[pos]
		pos = pos + 1
		local num = tonumber(ARGV[pos])
		pos = pos + 1

		local v = redis.call('HGET', key, field)
		if not v then
			v = "[]"
		end

		local jsonv = json.decode(v)
		local change = {}
		
		if op == "add" then
			for i = 1, num do
				local item = ARGV[pos]
				pos = pos + 1
				if not duplicate then -- 不去重 直接添加
					jsonv[#jsonv + 1] = item
					change[#change+1] = item
				else
					local exist = false
					for i, v in ipairs(jsonv) do
						if v == item then
							exist = true
							break
						end
					end
					if not exist then
						jsonv[#jsonv + 1] = item
						change[#change+1] = item
					end
				end
			end
		elseif op == "del" then
			for i = 1, num do
				local item = ARGV[pos]
				pos = pos + 1
				for i = #jsonv, 1, -1 do -- 涉及删除 需要倒序遍历
					if jsonv[i] == item then
						table.remove(jsonv, i)
						change[#change+1] = item
						if not duplicate then
							break
						end
					end
				end
			end
		end

		setkv[#setkv+1] = field
		setkv[#setkv+1] = json.encode(jsonv)
		rst[#rst+1] = change

		if writeDirty then
			-- 脏标记，记录修改的字段
			local token = "," .. field .. ","
			if not string.find(dirty2, token, 1, true) then
				if #dirty == 0 then
					dirty = field
				else
					dirty = dirty .. "," .. field
				end
				dirtyChanged = true
			end
			dverChanged = true
		end
	end

	if dirtyChanged then
		setkv[#setkv+1] = '_dirty_'
		setkv[#setkv+1] = dirty
	end
	if dverChanged then
		setkv[#setkv+1] = '_dver_'
		setkv[#setkv+1] = dver + 1
	end

	if #setkv > 0 then
		redis.call('HMSET', key, unpack(setkv))
	end
	return rst
`)
