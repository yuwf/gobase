package mrcache

import (
	"context"
	"testing"
	"time"

	"github.com/yuwf/gobase/goredis"
	"github.com/yuwf/gobase/mysql"
	"github.com/yuwf/gobase/utils"

	_ "github.com/yuwf/gobase/log"

	"github.com/rs/zerolog/log"
)

// db中
type Test struct {
	Id         int       `db:"Id" json:"Id,omitempty"`                      //自增住建  不可为空
	CreateTime time.Time `db:"create_time" redis:"ct" json:"UID,omitempty"` //用户ID  redis 记录ct
	UpdateTime time.Time `db:"update_time" redis:"ut" json:"UID,omitempty"` //用户ID  redis 记录ut
	UID        int       `db:"UID" redis:"U" json:"UID,omitempty"`          //用户ID  redis 记录U
	Type       int       `db:"Type" json:"Type,omitempty"`                  //用户ID  不可为空
	Name       string    `db:"Name" json:"Name,omitempty"`                  //名字  不可为空
	Age        int       `db:"Age" json:"Age,omitempty"`                    //年龄
	Mark       *string   `db:"Mark" json:"Mark,omitempty"`                  //标记 可以为空
}

var cacheRow *CacheRow[Test]
var cacheRows *CacheRows[Test]

func init() {
	var mysqlCfg = &mysql.Config{
		Source: "root:1235@tcp(localhost:3306)/test?charset=utf8&parseTime=true&loc=Local", // 这里必须添加上&parseTime=true&loc=Local 否则time.Time解析不了
	}

	var redisCfg = &goredis.Config{
		Addrs:  []string{"127.0.0.1:6379"},
		Passwd: "1235",
	}

	ctx := context.WithValue(context.TODO(), utils.CtxKey_nolog, 1)

	_, err := mysql.InitDefaultMySQL(mysqlCfg)
	if err != nil {
		return
	}

	_, err = mysql.DefaultMySQL().Exec(ctx, "CREATE DATABASE IF NOT EXISTS test")
	if err != nil {
		return
	}

	_, err = mysql.DefaultMySQL().Exec(ctx, "USE test")
	if err != nil {
		return
	}

	_, err = mysql.DefaultMySQL().Exec(ctx, "DROP TABLE IF EXISTS test")
	if err != nil {
		return
	}

	sql := `
	CREATE TABLE IF NOT EXISTS test (
		Id bigint NOT NULL AUTO_INCREMENT COMMENT '自增住建',
		create_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
		update_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
		UID bigint NOT NULL DEFAULT '0' COMMENT '用户ID',
		Type int NOT NULL DEFAULT '0' COMMENT '用户类型',
		Name varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL DEFAULT '""' COMMENT '名字',
		Age int DEFAULT NULL COMMENT '年龄',
		Mark varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci COMMENT '标记',
		PRIMARY KEY (Id),
		UNIQUE KEY uk_UID_Type (UID,Type)
	) ENGINE=InnoDB AUTO_INCREMENT=11019 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
	`
	_, err = mysql.DefaultMySQL().Exec(ctx, sql)
	if err != nil {
		return
	}

	sql = `INSERT INTO test (UID,Type,Name,Age,Mark) VALUES
	(123, 0,"Name123_0",  0, "Mark123_0"),
	(123, 1,"Name123_1", 10, "Mark123_1"),
	(123, 2,"Name123_2", 20, "Mark123_2"),
	(123, 3,"Name123_3", 30, "Mark123_3"),
	(123, 4,"Name123_4", 40, "Mark123_4"),
	(123, 5,"Name123_5", 50, "Mark123_5"),
	(123, 6,"Name123_6", 60, "Mark123_6"),
	(123, 7,"Name123_7", 70, "Mark123_7"),
	(123, 8,"Name123_8", 80, "Mark123_8"),
	(123, 9,"Name123_9", 90, "Mark123_9");
	`

	_, err = mysql.DefaultMySQL().Exec(ctx, sql)
	if err != nil {
		return
	}

	_, err = goredis.InitDefaultRedis(redisCfg)
	if err != nil {
		return
	}

	cacheRow = NewCacheRow[Test](goredis.DefaultRedis(), mysql.DefaultMySQL(), "test")
	err = cacheRow.ConfigHashTag("UID")
	if err != nil {
		log.Error().Err(err).Msg("ConfigHashTag Err")
		return
	}
	err = cacheRow.ConfigIncrement(goredis.DefaultRedis(), "Id", "test")
	if err != nil {
		log.Error().Err(err).Msg("ConfigIncrement Err")
		return
	}

	// 多列
	cacheRows = NewCacheRows[Test](goredis.DefaultRedis(), mysql.DefaultMySQL(), "test")
	err = cacheRows.ConfigHashTag("UID")
	if err != nil {
		log.Error().Err(err).Msg("ConfigHashTag Err")
		return
	}
	err = cacheRows.ConfigIncrement(goredis.DefaultRedis(), "Id", "test")
	if err != nil {
		log.Error().Err(err).Msg("ConfigIncrement Err")
		return
	}
	err = cacheRows.ConfigDataKeyField("Type")
	if err != nil {
		return
	}
	// 多行数据，加个额外条件
	err = cacheRows.ConfigQueryCond(NewConds().Ge("Age", 20))
	if err != nil {
		return
	}
}

func BenchmarkRowGet(b *testing.B) {
	if cacheRow == nil {
		log.Error().Msg("init not success")
		return
	}

	// 先删除
	cacheRow.DelCache(context.TODO(), NewConds().Eq("UID", 123).Eq("Type", 1))

	user, err := cacheRow.Get(context.TODO(), NewConds().Eq("UID", 123).Eq("Type", 1))
	log.Info().Err(err).Interface("user", user).Msg("Get")

	// 再次读取
	user, err = cacheRow.Get(context.TODO(), NewConds().Eq("UID", 123).Eq("Type", 1))
	log.Info().Err(err).Interface("user", user).Msg("Get")
}

func BenchmarkRowSet(b *testing.B) {
	if cacheRow == nil {
		log.Error().Msg("init not success")
		return
	}

	type SetTest struct {
		Name string `db:"Name"json:"Name,omitempty"` //名字  不可为空
		Age  int    `db:"Age"json:"Age,omitempty"`   //年龄
	}

	// 先删除
	cacheRow.Del(context.TODO(), NewConds().Eq("UID", 126).Eq("Type", 1))

	s := &SetTest{
		Name: "Hello",
		Age:  1000,
	}
	// 设置一个不存在的
	user, incrValue, err := cacheRow.Set(context.TODO(), NewConds().Eq("UID", 126).Eq("Type", 1), s, true)
	log.Info().Err(err).Interface("user", user).Interface("incrValue", incrValue).Msg("Set")

	s = &SetTest{
		Name: "Hello2",
		Age:  10000,
	}
	// 设置一个存在的
	user, incrValue, err = cacheRow.Set(context.TODO(), NewConds().Eq("UID", 126).Eq("Type", 1), s, true)
	log.Info().Err(err).Interface("user", user).Interface("incrValue", incrValue).Msg("Set")
}

func BenchmarkRowSetM(b *testing.B) {
	if cacheRow == nil {
		log.Error().Msg("init not success")
		return
	}

	// 先删除
	cacheRow.Del(context.TODO(), NewConds().Eq("UID", 126).Eq("Type", 1))

	s := map[string]interface{}{
		"Name": "Hello",
		"Age":  1000,
	}
	// 设置一个不存在的
	user, incrValue, err := cacheRow.SetM(context.TODO(), NewConds().Eq("UID", 126).Eq("Type", 1), s, true)
	log.Info().Err(err).Interface("user", user).Interface("incrValue", incrValue).Msg("SetM")

	s = map[string]interface{}{
		"Name": "Hello2",
		"Age":  10000,
	}
	// 设置一个存在的
	user, incrValue, err = cacheRow.SetM(context.TODO(), NewConds().Eq("UID", 126).Eq("Type", 1), s, true)
	log.Info().Err(err).Interface("user", user).Interface("incrValue", incrValue).Msg("SetM")
}

func BenchmarkRowSet2(b *testing.B) {
	if cacheRow == nil {
		log.Error().Msg("init not success")
		return
	}

	type SetTest struct {
		Name string `db:"Name"json:"Name,omitempty"` //名字  不可为空
		Age  int    `db:"Age"json:"Age,omitempty"`   //年龄
	}

	// 先删除
	cacheRow.Del(context.TODO(), NewConds().Eq("UID", 126).Eq("Type", 1))

	s := &SetTest{
		Name: "Hello",
		Age:  100,
	}
	// 设置一个不存在的
	incrValue, err := cacheRow.Set2(context.TODO(), NewConds().Eq("UID", 126).Eq("Type", 1), s, true)
	log.Info().Err(err).Interface("incrValue", incrValue).Msg("Set2")

	s = &SetTest{
		Name: "Hello2",
		Age:  10000,
	}
	// 设置一个存在的
	incrValue, err = cacheRow.Set2(context.TODO(), NewConds().Eq("UID", 126).Eq("Type", 1), s, true)
	log.Info().Err(err).Interface("incrValue", incrValue).Msg("Set2")
}

func BenchmarkRowModify(b *testing.B) {
	if cacheRow == nil {
		log.Error().Msg("init not success")
		return
	}

	type ModifyTest struct {
		Name string `db:"Name"json:"Name,omitempty"` //名字  不可为空
		Age  int    `db:"Age"json:"Age,omitempty"`   //年龄
	}

	// 先删除
	cacheRow.Del(context.TODO(), NewConds().Eq("UID", 126).Eq("Type", 1))

	s := &ModifyTest{
		Name: "Hello",
		Age:  100,
	}
	// 设置一个不存在的
	user, incrValue, err := cacheRow.Modify(context.TODO(), NewConds().Eq("UID", 126).Eq("Type", 1), s, true)
	log.Info().Err(err).Interface("user", user).Interface("incrValue", incrValue).Msg("Modify")

	s = &ModifyTest{
		Name: "Hello2",
		Age:  10000,
	}
	// 设置一个存在的
	user, incrValue, err = cacheRow.Modify(context.TODO(), NewConds().Eq("UID", 126).Eq("Type", 1), s, true)
	log.Info().Err(err).Interface("user", user).Interface("incrValue", incrValue).Msg("Modify")
}

func BenchmarkRowModifyM(b *testing.B) {
	if cacheRow == nil {
		log.Error().Msg("init not success")
		return
	}

	// 先删除
	cacheRow.Del(context.TODO(), NewConds().Eq("UID", 126).Eq("Type", 1))

	s := map[string]interface{}{
		"Name": "Hello",
		"Age":  100,
	}
	// 设置一个不存在的
	user, incrValue, err := cacheRow.ModifyM(context.TODO(), NewConds().Eq("UID", 126).Eq("Type", 1), s, true)
	log.Info().Err(err).Interface("user", user).Interface("incrValue", incrValue).Msg("Modify")

	s = map[string]interface{}{
		"Name": "Hello2",
		"Age":  10000,
	}
	// 设置一个存在的
	user, incrValue, err = cacheRow.ModifyM(context.TODO(), NewConds().Eq("UID", 126).Eq("Type", 1), s, true)
	log.Info().Err(err).Interface("user", user).Interface("incrValue", incrValue).Msg("Modify")
}

func BenchmarkRowModify2(b *testing.B) {
	if cacheRow == nil {
		log.Error().Msg("init not success")
		return
	}

	// 先删除
	cacheRow.Del(context.TODO(), NewConds().Eq("UID", 126).Eq("Type", 1))

	type ModifyTest struct {
		Name string `db:"Name"json:"Name,omitempty"` //名字  不可为空
		Age  int    `db:"Age"json:"Age,omitempty"`   //年龄
	}

	s := &ModifyTest{
		Name: "Hello",
		Age:  100,
	}
	// 设置一个不存在的
	user, incrValue, err := cacheRow.Modify2(context.TODO(), NewConds().Eq("UID", 126).Eq("Type", 1), s, true)
	log.Info().Err(err).Interface("user", user).Interface("incrValue", incrValue).Msg("Modify")

	// 设置一个存在的
	s = &ModifyTest{
		Name: "Hello2",
		Age:  10000,
	}
	user, incrValue, err = cacheRow.Modify2(context.TODO(), NewConds().Eq("UID", 126).Eq("Type", 1), s, true)
	log.Info().Err(err).Interface("user", user).Interface("incrValue", incrValue).Msg("Modify")
}

func BenchmarkRowModifyM2(b *testing.B) {
	if cacheRow == nil {
		log.Error().Msg("init not success")
		return
	}

	// 先删除
	cacheRow.Del(context.TODO(), NewConds().Eq("UID", 126).Eq("Type", 1))

	s := map[string]interface{}{
		"Name": "Hello",
		"Age":  100,
	}
	// 设置一个不存在的
	user, incrValue, err := cacheRow.ModifyM2(context.TODO(), NewConds().Eq("UID", 126).Eq("Type", 1), s, true)
	log.Info().Err(err).Interface("user", user).Interface("incrValue", incrValue).Msg("Modify2")

	s = map[string]interface{}{
		"Name": "Hello2",
		"Age":  10000,
	}
	// 设置一个存在的
	user, incrValue, err = cacheRow.ModifyM2(context.TODO(), NewConds().Eq("UID", 126).Eq("Type", 1), s, true)
	log.Info().Err(err).Interface("user", user).Interface("incrValue", incrValue).Msg("Modify2")
}

func BenchmarkRowsGetAll(b *testing.B) {
	if cacheRows == nil {
		log.Error().Msg("init not success")
		return
	}

	// 先删除缓存
	cacheRows.DelCache(context.TODO(), NewConds().Eq("UID", 123))

	user, err := cacheRows.GetAll(context.TODO(), NewConds().Eq("UID", 123))
	log.Info().Err(err).Interface("user", user).Msg("GetAll")

	// 在获取一次
	user, err = cacheRows.GetAll(context.TODO(), NewConds().Eq("UID", 123))
	log.Info().Err(err).Interface("user", user).Msg("GetAll")
}

func BenchmarkRowsGet(b *testing.B) {
	if cacheRows == nil {
		log.Error().Msg("init not success")
		return
	}

	// 先删除缓存
	cacheRows.DelCache(context.TODO(), NewConds().Eq("UID", 123))

	user, err := cacheRows.Get(context.TODO(), NewConds().Eq("UID", 123), 8)
	log.Info().Err(err).Interface("user", user).Msg("Get")

	// 在获取一次 别的
	user, err = cacheRows.Get(context.TODO(), NewConds().Eq("UID", 123), 7)
	log.Info().Err(err).Interface("user", user).Msg("Get")
}

func BenchmarkRowsExist(b *testing.B) {
	if cacheRows == nil {
		log.Error().Msg("init not success")
		return
	}

	// 先删除缓存
	cacheRows.DelCache(context.TODO(), NewConds().Eq("UID", 123))

	exist, err := cacheRows.Exist(context.TODO(), NewConds().Eq("UID", 123), 8)
	log.Info().Err(err).Interface("exist", exist).Msg("Exist")

	// 读取一次，应该直接从缓存读取
	user, err := cacheRows.Get(context.TODO(), NewConds().Eq("UID", 123), 8)
	log.Info().Err(err).Interface("user", user).Msg("Exist")

	// 不存在的
	exist, err = cacheRows.Exist(context.TODO(), NewConds().Eq("UID", 123), 100)
	log.Info().Err(err).Interface("exist", exist).Msg("Exist")
}

func BenchmarkRowsSet(b *testing.B) {
	if cacheRows == nil {
		log.Error().Msg("init not success")
		return
	}

	// 先删除缓存
	cacheRows.DelCache(context.TODO(), NewConds().Eq("UID", 123))

	type SetTest struct {
		Name string `db:"Name"json:"Name,omitempty"` //名字
		Age  int    `db:"Age"json:"Age,omitempty"`   //年龄
		Type int    `db:"Type"json:"Type,omitempty"` //数据字段
	}

	s := &SetTest{
		Name: "Hello",
		Age:  100,
		Type: 12,
	}
	// 设置一个不存在的
	user, incrValue, err := cacheRows.Set(context.TODO(), NewConds().Eq("UID", 123), s, true)
	log.Info().Err(err).Interface("user", user).Interface("incrValue", incrValue).Msg("Set")

	s = &SetTest{
		Name: "Hello2",
		Age:  10000,
		Type: 12,
	}
	// 设置一个存在的
	user, incrValue, err = cacheRows.Set(context.TODO(), NewConds().Eq("UID", 123), s, true)
	log.Info().Err(err).Interface("user", user).Interface("incrValue", incrValue).Msg("Set")
}

func BenchmarkRowsSetM(b *testing.B) {
	if cacheRows == nil {
		log.Error().Msg("init not success")
		return
	}

	// 先删除缓存
	cacheRows.DelCache(context.TODO(), NewConds().Eq("UID", 123))

	s := map[string]interface{}{
		"Name": "Hello",
		"Age":  100,
		"Type": 12,
	}
	// 设置一个不存在的
	user, incrValue, err := cacheRows.SetM(context.TODO(), NewConds().Eq("UID", 123), s, true)
	log.Info().Err(err).Interface("user", user).Interface("incrValue", incrValue).Msg("SetM")

	s = map[string]interface{}{
		"Name": "Hello2",
		"Age":  1000,
		"Type": 12,
	}
	// 设置一个存在的
	user, incrValue, err = cacheRows.SetM(context.TODO(), NewConds().Eq("UID", 123), s, true)
	log.Info().Err(err).Interface("user", user).Interface("incrValue", incrValue).Msg("SetM")
}

func BenchmarkRowsSet2(b *testing.B) {
	if cacheRows == nil {
		log.Error().Msg("init not success")
		return
	}

	// 先删除缓存
	cacheRows.DelCache(context.TODO(), NewConds().Eq("UID", 123))

	type SetTest struct {
		Name string `db:"Name"json:"Name,omitempty"` //名字  不可为空
		Age  int    `db:"Age"json:"Age,omitempty"`   //年龄
		Type int    `db:"Type"json:"Type,omitempty"` //数据字段
	}

	//a := 10
	s := &SetTest{
		Name: "Hello",
		Age:  100,
		Type: 12,
	}
	// 设置一个不存在的
	incrValue, err := cacheRows.Set2(context.TODO(), NewConds().Eq("UID", 123), s, true)
	log.Info().Err(err).Interface("incrValue", incrValue).Msg("Set2")

	s = &SetTest{
		Name: "Hello2",
		Age:  10000,
		Type: 12,
	}
	// 设置一个存在的
	incrValue, err = cacheRows.Set2(context.TODO(), NewConds().Eq("UID", 123), s, true)
	log.Info().Err(err).Interface("incrValue", incrValue).Msg("Set2")
}

func BenchmarkRowsSetM2(b *testing.B) {
	if cacheRows == nil {
		log.Error().Msg("init not success")
		return
	}

	// 先删除缓存
	cacheRows.DelCache(context.TODO(), NewConds().Eq("UID", 123))

	s := map[string]interface{}{
		"Name": "Hello",
		"Age":  100,
		"Type": 12,
	}
	// 设置一个不存在的
	incrValue, err := cacheRows.SetM2(context.TODO(), NewConds().Eq("UID", 123), s, true)
	log.Info().Err(err).Interface("incrValue", incrValue).Msg("SetM2")

	s = map[string]interface{}{
		"Name": "Hello2",
		"Age":  10000,
		"Type": 12,
	}
	// 设置一个存在的
	incrValue, err = cacheRows.SetM2(context.TODO(), NewConds().Eq("UID", 123), s, true)
	log.Info().Err(err).Interface("incrValue", incrValue).Msg("SetM2")
}

func BenchmarkRowsModify(b *testing.B) {
	if cacheRows == nil {
		log.Error().Msg("init not success")
		return
	}

	// 先删除缓存
	cacheRows.DelCache(context.TODO(), NewConds().Eq("UID", 123))

	type ModifyTest struct {
		Name string `db:"Name"json:"Name,omitempty"` //名字  不可为空
		Age  int    `db:"Age"json:"Age,omitempty"`   //年龄
		Type int    `db:"Type"json:"Type,omitempty"` //年龄
	}

	s := &ModifyTest{
		Name: "Hello",
		Age:  100,
		Type: 12,
	}
	// 设置一个不存在的
	user, incrValue, err := cacheRows.Modify(context.TODO(), NewConds().Eq("UID", 123), s, true)
	log.Info().Err(err).Interface("user", user).Interface("incrValue", incrValue).Msg("Modify")

	s = &ModifyTest{
		Name: "Hello2",
		Age:  10000,
		Type: 12,
	}
	// 设置一个存在的
	user, incrValue, err = cacheRows.Modify(context.TODO(), NewConds().Eq("UID", 123), s, true)
	log.Info().Err(err).Interface("user", user).Interface("incrValue", incrValue).Msg("Modify")
}

func BenchmarkRowsModifyM(b *testing.B) {
	if cacheRows == nil {
		log.Error().Msg("init not success")
		return
	}

	// 先删除缓存
	cacheRows.DelCache(context.TODO(), NewConds().Eq("UID", 123))

	s := map[string]interface{}{
		"Name": "Hello",
		"Age":  100,
		"Type": 12,
	}
	// 设置一个不存在的
	user, incrValue, err := cacheRows.ModifyM(context.TODO(), NewConds().Eq("UID", 123), s, true)
	log.Info().Err(err).Interface("user", user).Interface("incrValue", incrValue).Msg("ModifyM")

	s = map[string]interface{}{
		"Name": "Hello2",
		"Age":  10000,
		"Type": 12,
	}
	// 设置一个存在的
	user, incrValue, err = cacheRows.ModifyM(context.TODO(), NewConds().Eq("UID", 123), s, true)
	log.Info().Err(err).Interface("user", user).Interface("incrValue", incrValue).Msg("ModifyM")
}

func BenchmarkRowsModify2(b *testing.B) {
	if cacheRows == nil {
		log.Error().Msg("init not success")
		return
	}

	// 先删除缓存
	cacheRows.DelCache(context.TODO(), NewConds().Eq("UID", 123))

	type ModifyTest struct {
		Name string `db:"Name"json:"Name,omitempty"` //名字  不可为空
		Age  int    `db:"Age"json:"Age,omitempty"`   //年龄
		Type int    `db:"Type"json:"Type,omitempty"` //年龄
	}

	s := &ModifyTest{
		Name: "Hello",
		Age:  100,
		Type: 12,
	}
	// 设置一个不存在的
	user, incrValue, err := cacheRows.Modify2(context.TODO(), NewConds().Eq("UID", 123), s, true)
	log.Info().Err(err).Interface("user", user).Interface("incrValue", incrValue).Msg("Modify2")

	s = &ModifyTest{
		Name: "Hello2",
		Age:  10000,
		Type: 12,
	}
	// 设置一个存在的
	user, incrValue, err = cacheRows.Modify2(context.TODO(), NewConds().Eq("UID", 123), s, true)
	log.Info().Err(err).Interface("user", user).Interface("incrValue", incrValue).Msg("Modify2")
}

func BenchmarkRowsModifyM2(b *testing.B) {
	if cacheRows == nil {
		log.Error().Msg("init not success")
		return
	}

	// 先删除缓存
	cacheRows.DelCache(context.TODO(), NewConds().Eq("UID", 123))

	type ModifyTest struct {
		Name string `db:"Name"json:"Name,omitempty"` //名字  不可为空
		Age  int    `db:"Age"json:"Age,omitempty"`   //年龄
		Type int    `db:"Type"json:"Type,omitempty"` //年龄
	}

	s := map[string]interface{}{
		"Name": "Hello",
		"Age":  100,
		"Type": 12,
	}
	// 设置一个不存在的
	user, incrValue, err := cacheRows.ModifyM2(context.TODO(), NewConds().Eq("UID", 123), s, true)
	log.Info().Err(err).Interface("user", user).Interface("incrValue", incrValue).Msg("ModifyM2")

	s = map[string]interface{}{
		"Name": "Hello2",
		"Age":  10000,
		"Type": 12,
	}
	// 设置一个存在的
	user, incrValue, err = cacheRows.ModifyM2(context.TODO(), NewConds().Eq("UID", 123), s, true)
	log.Info().Err(err).Interface("user", user).Interface("incrValue", incrValue).Msg("ModifyM2")
}
