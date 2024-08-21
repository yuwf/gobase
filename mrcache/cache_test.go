package mrcache

import (
	"context"
	"testing"

	"github.com/yuwf/gobase/goredis"
	"github.com/yuwf/gobase/mysql"

	_ "github.com/yuwf/gobase/log"

	"github.com/rs/zerolog/log"
)

// db中
type Test struct {
	Id   int     `db:"Id"json:"Id,omitempty"`     //自增住建  不可为空
	UID  int     `db:"UID"json:"UID,omitempty"`   //用户ID  不可为空
	Type int     `db:"Type"json:"Type,omitempty"` //用户ID  不可为空
	Name string  `db:"Name"json:"Name,omitempty"` //名字  不可为空
	Age  *int    `db:"Age"json:"Age,omitempty"`   //年龄
	T    *string `db:"T"json:"T,omitempty"`       //测试时间
}

func init() {

	var mysqlCfg = &mysql.Config{
		Source: "root:1235@tcp(localhost:3306)/test?charset=utf8",
	}

	var redisCfg = &goredis.Config{
		Addrs:  []string{"127.0.0.1:6379"},
		Passwd: "1235",
	}

	_, err := mysql.InitDefaultMySQL(mysqlCfg)
	if err != nil {
		return
	}

	_, err = mysql.DefaultMySQL().Exec(context.TODO(), "CREATE DATABASE IF NOT EXISTS test")
	if err != nil {
		return
	}

	_, err = mysql.DefaultMySQL().Exec(context.TODO(), "USE test")
	if err != nil {
		return
	}

	sql := `
	CREATE TABLE IF NOT EXISTS test (
		Id bigint NOT NULL AUTO_INCREMENT COMMENT '自增住建',
		UID bigint NOT NULL DEFAULT '0' COMMENT '用户ID',
		Type int NOT NULL DEFAULT '0',
		Name varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL DEFAULT '""' COMMENT '名字',
		Age int DEFAULT NULL COMMENT '年龄',
		T time DEFAULT NULL COMMENT '测试时间',
		PRIMARY KEY (Id),
		UNIQUE KEY uk_UID_Type (UID,Type)
	) ENGINE=InnoDB AUTO_INCREMENT=11019 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
	`
	_, err = mysql.DefaultMySQL().Exec(context.TODO(), sql)
	if err != nil {
		return
	}

	_, err = goredis.InitDefaultRedis(redisCfg)
	if err != nil {
		return
	}
}

func BenchmarkRowGet(b *testing.B) {
	if goredis.DefaultRedis() == nil {
		log.Error().Msg("init not success")
		return
	}
	cache := NewCacheRow[Test](goredis.DefaultRedis(), mysql.DefaultMySQL(), "test")
	err := cache.ConfigHashTag("UID")
	if err != nil {
		log.Error().Err(err).Msg("ConfigHashTag Err")
		return
	}

	user, err := cache.Get(context.TODO(), NewConds().Eq("UID", 123))
	log.Info().Err(err).Interface("user", user).Msg("Get")
}

func BenchmarkRowSet(b *testing.B) {
	if goredis.DefaultRedis() == nil {
		log.Error().Msg("init not success")
		return
	}
	cache := NewCacheRow[Test](goredis.DefaultRedis(), mysql.DefaultMySQL(), "test")
	err := cache.ConfigHashTag("UID")
	if err != nil {
		log.Error().Err(err).Msg("ConfigHashTag Err")
		return
	}
	err = cache.ConfigIncrement(goredis.DefaultRedis(), "Id", "test")
	if err != nil {
		log.Error().Err(err).Msg("ConfigIncrement Err")
		return
	}

	type SetTest struct {
		Name string `db:"Name"json:"Name,omitempty"` //名字  不可为空
		Age  int    `db:"Age"json:"Age,omitempty"`   //年龄
	}

	//a := 10
	s := &SetTest{
		Name: "好u",
		Age:  50,
	}

	user, incrValue, err := cache.Set(context.TODO(), NewConds().Eq("UID", 126).Eq("Type", 9), s, true)

	log.Info().Err(err).Interface("user", user).Interface("incrValue", incrValue).Msg("Set")
}

func BenchmarkRowSet2(b *testing.B) {
	if goredis.DefaultRedis() == nil {
		log.Error().Msg("init not success")
		return
	}
	cache := NewCacheRow[Test](goredis.DefaultRedis(), mysql.DefaultMySQL(), "test")
	err := cache.ConfigHashTag("UID")
	if err != nil {
		log.Error().Err(err).Msg("ConfigHashTag Err")
		return
	}
	err = cache.ConfigIncrement(goredis.DefaultRedis(), "Id", "test")
	if err != nil {
		log.Error().Err(err).Msg("ConfigIncrement Err")
		return
	}

	type SetTest struct {
		Name string `db:"Name"json:"Name,omitempty"` //名字  不可为空
		Age  int    `db:"Age"json:"Age,omitempty"`   //年龄
	}

	//a := 10
	s := &SetTest{
		Name: "好17u7u8888",
		Age:  20,
	}

	incrValue, err := cache.Set2(context.TODO(), NewConds().Eq("UID", 126).Eq("Type", 8), s, true)

	log.Info().Err(err).Interface("incrValue", incrValue).Msg("Set2")
}

func BenchmarkRowModify(b *testing.B) {
	if goredis.DefaultRedis() == nil {
		log.Error().Msg("init not success")
		return
	}
	cache := NewCacheRow[Test](goredis.DefaultRedis(), mysql.DefaultMySQL(), "test")
	err := cache.ConfigHashTag("UID")
	if err != nil {
		log.Error().Err(err).Msg("ConfigHashTag Err")
		return
	}

	type ModifyTest struct {
		Name string `db:"Name"json:"Name,omitempty"` //名字  不可为空
		Age  *int   `db:"Age"json:"Age,omitempty"`   //年龄
	}

	a := 10
	//ss := time.Now().Format(time.TimeOnly)
	s := &ModifyTest{
		Name: "99999",
		Age:  &a,
	}

	user, incrValue, err := cache.Modify(context.TODO(), NewConds().Eq("UID", 123).Eq("Type", 8), s, false)

	log.Info().Err(err).Interface("user", user).Interface("incrValue", incrValue).Msg("Modify")
}

func BenchmarkRowModify2(b *testing.B) {
	if goredis.DefaultRedis() == nil {
		log.Error().Msg("init not success")
		return
	}
	cache := NewCacheRow[Test](goredis.DefaultRedis(), mysql.DefaultMySQL(), "test")
	err := cache.ConfigHashTag("UID")
	if err != nil {
		log.Error().Err(err).Msg("ConfigHashTag Err")
		return
	}

	type ModifyTest struct {
		Name string `db:"Name"json:"Name,omitempty"` //名字  不可为空
		Age  *int   `db:"Age"json:"Age,omitempty"`   //年龄
	}

	a := 10
	//ss := time.Now().Format(time.TimeOnly)
	s := &ModifyTest{
		Name: "88888",
		Age:  &a,
	}

	user, incrValue, err := cache.Modify2(context.TODO(), NewConds().Eq("UID", 123).Eq("Type", 8), s, false)

	log.Info().Err(err).Interface("user", user).Interface("incrValue", incrValue).Msg("Modify")
}

func BenchmarkRowsGetAll(b *testing.B) {
	if goredis.DefaultRedis() == nil {
		log.Error().Msg("init not success")
		return
	}
	cache := NewCacheRows[Test](goredis.DefaultRedis(), mysql.DefaultMySQL(), "test")
	err := cache.ConfigHashTag("UID")
	if err != nil {
		log.Error().Err(err).Msg("ConfigHashTag Err")
		return
	}
	err = cache.ConfigIncrement(goredis.DefaultRedis(), "Id", "Name")
	if err != nil {
		log.Error().Err(err).Msg("ConfigIncrement Err")
		return
	}
	err = cache.ConfigDataKeyField("Type")
	if err != nil {
		return
	}
	err = cache.ConfigQueryCond(NewConds().Ge("Age", 20))
	if err != nil {
		return
	}

	user, err := cache.GetAll(context.TODO(), NewConds().Eq("UID", 123))

	log.Info().Err(err).Interface("user", user).Msg("GetAll")
}

func BenchmarkRowsGet(b *testing.B) {
	if goredis.DefaultRedis() == nil {
		log.Error().Msg("init not success")
		return
	}
	cache := NewCacheRows[Test](goredis.DefaultRedis(), mysql.DefaultMySQL(), "test")
	err := cache.ConfigHashTag("UID")
	if err != nil {
		log.Error().Err(err).Msg("ConfigHashTag Err")
		return
	}
	err = cache.ConfigIncrement(goredis.DefaultRedis(), "Id", "Name")
	if err != nil {
		log.Error().Err(err).Msg("ConfigIncrement Err")
		return
	}
	err = cache.ConfigDataKeyField("Type")
	if err != nil {
		log.Error().Err(err).Msg("ConfigDataKeyField Err")
		return
	}
	err = cache.ConfigQueryCond(NewConds().Ge("Age", 20))
	if err != nil {
		return
	}

	user, err := cache.Get(context.TODO(), NewConds().Eq("UID", 123), 8)
	log.Info().Err(err).Interface("user", user).Msg("Get")
}

func BenchmarkRowsSet(b *testing.B) {
	if goredis.DefaultRedis() == nil {
		log.Error().Msg("init not success")
		return
	}
	cache := NewCacheRows[Test](goredis.DefaultRedis(), mysql.DefaultMySQL(), "test")
	err := cache.ConfigHashTag("UID")
	if err != nil {
		log.Error().Err(err).Msg("ConfigHashTag Err")
		return
	}
	err = cache.ConfigIncrement(goredis.DefaultRedis(), "Id", "Name")
	if err != nil {
		log.Error().Err(err).Msg("ConfigIncrement Err")
		return
	}
	err = cache.ConfigDataKeyField("Type")
	if err != nil {
		log.Error().Err(err).Msg("ConfigDataKeyField Err")
		return
	}
	err = cache.ConfigQueryCond(NewConds().Ge("Age", 20))
	if err != nil {
		return
	}

	type SetTest struct {
		Name string `db:"Name"json:"Name,omitempty"` //名字
		Age  int    `db:"Age"json:"Age,omitempty"`   //年龄
		Type int    `db:"Type"json:"Type,omitempty"` //数据字段
	}

	//a := 10
	s := &SetTest{
		Name: "name==",
		Age:  10000,
		Type: 12,
	}

	user, incrValue, err := cache.Set(context.TODO(), NewConds().Eq("UID", 123), s, true)

	log.Info().Err(err).Interface("user", user).Interface("incrValue", incrValue).Msg("Set")
}

func BenchmarkRowsSet2(b *testing.B) {
	if goredis.DefaultRedis() == nil {
		log.Error().Msg("init not success")
		return
	}
	cache := NewCacheRows[Test](goredis.DefaultRedis(), mysql.DefaultMySQL(), "test")
	err := cache.ConfigHashTag("UID")
	if err != nil {
		log.Error().Err(err).Msg("ConfigHashTag Err")
		return
	}
	err = cache.ConfigIncrement(goredis.DefaultRedis(), "Id", "Name")
	if err != nil {
		log.Error().Err(err).Msg("ConfigIncrement Err")
		return
	}
	err = cache.ConfigDataKeyField("Type")
	if err != nil {
		log.Error().Err(err).Msg("ConfigDataKeyField Err")
		return
	}
	err = cache.ConfigQueryCond(NewConds().Ge("Age", 20))
	if err != nil {
		return
	}

	type SetTest struct {
		Name string `db:"Name"json:"Name,omitempty"` //名字  不可为空
		Age  int    `db:"Age"json:"Age,omitempty"`   //年龄
		Type int    `db:"Type"json:"Type,omitempty"` //数据字段
	}

	//a := 10
	s := &SetTest{
		Name: "name8015",
		Age:  100,
	}

	incrValue, err := cache.Set2(context.TODO(), NewConds().Eq("UID", 123), s, true)

	log.Info().Err(err).Interface("incrValue", incrValue).Msg("Set")
}

func BenchmarkRowsModify(b *testing.B) {
	if goredis.DefaultRedis() == nil {
		log.Error().Msg("init not success")
		return
	}
	cache := NewCacheRows[Test](goredis.DefaultRedis(), mysql.DefaultMySQL(), "test")
	err := cache.ConfigHashTag("UID")
	if err != nil {
		log.Error().Err(err).Msg("ConfigHashTag Err")
		return
	}
	err = cache.ConfigIncrement(goredis.DefaultRedis(), "Id", "Name")
	if err != nil {
		log.Error().Err(err).Msg("ConfigIncrement Err")
		return
	}
	err = cache.ConfigDataKeyField("Type")
	if err != nil {
		log.Error().Err(err).Msg("ConfigDataKeyField Err")
		return
	}
	err = cache.ConfigQueryCond(NewConds().Ge("Age", 20))
	if err != nil {
		log.Error().Err(err).Msg("ConfigQueryCond Err")
		return
	}

	type ModifyTest struct {
		Name string `db:"Name"json:"Name,omitempty"` //名字  不可为空
		Age  int    `db:"Age"json:"Age,omitempty"`   //年龄
		Type int    `db:"Type"json:"Type,omitempty"` //年龄
	}

	//a := 10
	s := &ModifyTest{
		Name: "namenamen8022",
		Age:  1000,
		Type: 10,
	}

	user, incrValue, err := cache.Modify(context.TODO(), NewConds().Eq("UID", 123), s, true)

	log.Info().Err(err).Interface("user", user).Interface("incrValue", incrValue).Msg("Modify")
}

func BenchmarkRowsModify2(b *testing.B) {
	if goredis.DefaultRedis() == nil {
		log.Error().Msg("init not success")
		return
	}
	cache := NewCacheRows[Test](goredis.DefaultRedis(), mysql.DefaultMySQL(), "test")
	err := cache.ConfigHashTag("UID")
	if err != nil {
		log.Error().Err(err).Msg("ConfigHashTag Err")
		return
	}
	err = cache.ConfigIncrement(goredis.DefaultRedis(), "Id", "Name")
	if err != nil {
		log.Error().Err(err).Msg("ConfigIncrement Err")
		return
	}
	err = cache.ConfigDataKeyField("Type")
	if err != nil {
		log.Error().Err(err).Msg("ConfigDataKeyField Err")
		return
	}
	err = cache.ConfigQueryCond(NewConds().Ge("Age", 20))
	if err != nil {
		log.Error().Err(err).Msg("ConfigQueryCond Err")
		return
	}

	type ModifyTest struct {
		Name string `db:"Name"json:"Name,omitempty"` //名字  不可为空
		Age  int    `db:"Age"json:"Age,omitempty"`   //年龄
		Type int    `db:"Type"json:"Type,omitempty"` //年龄
	}

	//a := 10
	s := &ModifyTest{
		Name: "namenamen",
		Age:  1000,
		Type: 10,
	}

	user, incrValue, err := cache.Modify2(context.TODO(), NewConds().Eq("UID", 123), s, true)

	log.Info().Err(err).Interface("user", user).Interface("incrValue", incrValue).Msg("Modify")
}
