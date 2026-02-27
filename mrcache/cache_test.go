package mrcache

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/yuwf/gobase/goredis"
	"github.com/yuwf/gobase/mysql"
	"github.com/yuwf/gobase/utils"

	_ "github.com/yuwf/gobase/log"

	"github.com/rs/zerolog/log"
)

type TestJ struct {
	Id   int    `json:"Id,omitempty"`   //自增住建  不可为空
	Name string `json:"Name,omitempty"` //名字  不可为空
}

func (m *TestJ) Scan(src any) error {
	if src == nil {
		return nil
	}
	return json.Unmarshal(src.([]byte), &m)
}
func (m TestJ) Value() (driver.Value, error) {
	return json.Marshal(&m)
}

// db中
type Test struct {
	Id         int               `db:"Id" json:"Id,omitempty"`                              //自增住建  不可为空
	CreateTime time.Time         `db:"create_time" redis:"ct" json:"create_time,omitempty"` //用户ID  redis 记录ct
	UpdateTime time.Time         `db:"update_time" redis:"ut" json:"update_time,omitempty"` //用户ID  redis 记录ut
	UID        int               `db:"UID" redis:"U" json:"UID,omitempty"`                  //用户ID  redis 记录U
	Type       int               `db:"Type" json:"Type,omitempty"`                          //用户ID  不可为空
	GroupType  string            `db:"GroupType" json:"GroupType,omitempty"`                //用户ID  不可为空
	Name       string            `db:"Name" json:"Name,omitempty"`                          //名字  不可为空
	Age        int               `db:"Age" json:"Age,omitempty"`                            //年龄
	Mark       *string           `db:"Mark" json:"Mark,omitempty"`                          //标记 可以为空
	Json       *TestJ            `db:"Json" json:"Json,omitempty"`                          //标记 可以为空
	Int64s     mysql.JsonInt64s  `db:"Int64s" json:"Int64s,omitempty"`
	Strs       mysql.JsonStrings `db:"Strs" json:"Strs,omitempty"`
}

var cacheRow *CacheRow[Test]
var cacheRows *CacheRows[Test]

func init() {
	DBNolog = false // 输出底层日志
	passExpire = int64(1000 * 1000)

	var mysqlCfg = &mysql.Config{
		Source: "root:1235@tcp(localhost:3306)/mysql?charset=utf8&parseTime=true&loc=Local", // 这里必须添加上&parseTime=true&loc=Local 否则time.Time解析不了
	}

	var redisCfg = &goredis.Config{
		Addrs:  []string{"127.0.0.1:6379"},
		Passwd: "",
		DB:     10,
	}

	ctx := utils.CtxSetNolog(context.TODO())

	_, err := mysql.InitDefaultMySQL(mysqlCfg)
	if err != nil {
		return
	}

	_, err = mysql.DefaultMySQL().Exec(ctx, "CREATE DATABASE IF NOT EXISTS test")
	if err != nil {
		return
	}

	// use 命令貌似切不了数据库，重新连数据库
	//_, err = mysql.DefaultMySQL().Exec(ctx, "USE test")
	//if err != nil {
	//	return
	//}
	mysqlCfg = &mysql.Config{
		Source: "root:1235@tcp(localhost:3306)/test?charset=utf8&parseTime=true&loc=Local", // 这里必须添加上&parseTime=true&loc=Local 否则time.Time解析不了
	}
	_, err = mysql.InitDefaultMySQL(mysqlCfg)
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
		GroupType varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL DEFAULT '""' COMMENT '组',
		Name varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL DEFAULT '""' COMMENT '名字',
		Age int DEFAULT NULL COMMENT '年龄',
		Mark varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci COMMENT '标记',
		Json JSON NULL COMMENT '标记',
		Int64s JSON NULL COMMENT '',
		Strs JSON NULL COMMENT '',
		PRIMARY KEY (Id),
		UNIQUE KEY uk_UID_Type (UID,Type)
	) ENGINE=InnoDB AUTO_INCREMENT=11019 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
	`
	_, err = mysql.DefaultMySQL().Exec(ctx, sql)
	if err != nil {
		return
	}

	sql = `INSERT INTO test (Id,UID,Type,GroupType,Name,Age,Mark,Json) VALUES
	(1, 123, 0, "G0", "Name123_0",  0, "Mark123_0",null),
	(2, 123, 1, "G0", "Name123_1", 10, "Mark123_1",null),
	(3, 123, 2, "G0", "Name123_2", 20, "Mark123_2",null),
	(4, 123, 3, "G0", "Name123_3", 30, "Mark123_3",null),
	(5, 123, 4, "G0", "Name123_4", 40, "Mark123_4","{\"Id\":123, \"Name\":\"MyName\"}"),
	(6, 123, 5, "G0", "Name123_5", 50, "Mark123_5",null),
	(7, 123, 6, "G0", "Name123_6", 60, "Mark123_6",null),
	(8, 123, 7, "G0", "Name123_7", 70, "Mark123_7",null),
	(9, 123, 8, "G1", "Name123_8", 80, "Mark123_8",null),
	(10,123, 9, "G1", "Mark123_9", 90, "Mark123_9",null);
	`
	_, err = mysql.DefaultMySQL().Exec(ctx, sql)
	if err != nil {
		return
	}

	_, err = goredis.InitDefaultRedis(redisCfg)
	if err != nil {
		return
	}

	cacheRow, err = NewCacheRow[Test](goredis.DefaultRedis(), mysql.DefaultMySQL(), "test", 0, 0, []string{"UID", "Type"})
	if err != nil {
		log.Error().Err(err).Msg("ConfigHashTag Err")
		return
	}
	err = cacheRow.ConfigHashTag("UID")
	if err != nil {
		log.Error().Err(err).Msg("ConfigHashTag Err")
		return
	}
	err = cacheRow.ConfigIncrement(goredis.DefaultRedis(), "Id")
	if err != nil {
		log.Error().Err(err).Msg("ConfigIncrement Err")
		return
	}
	// 加个额外条件,年龄要大于等于20的
	err = cacheRow.ConfigQueryCond(NewConds().Ge("Age", 20))
	if err != nil {
		return
	}

	// 多行
	cacheRows, err = NewCacheRows[Test](goredis.DefaultRedis(), mysql.DefaultMySQL(), "test", 0, 0, []string{"UID"}, []string{"Type", "GroupType"})
	if err != nil {
		log.Error().Err(err).Msg("ConfigHashTag Err")
		return
	}
	err = cacheRows.ConfigHashTag("UID")
	if err != nil {
		log.Error().Err(err).Msg("ConfigHashTag Err")
		return
	}
	err = cacheRows.ConfigIncrement(goredis.DefaultRedis(), "Id")
	if err != nil {
		log.Error().Err(err).Msg("ConfigIncrement Err")
		return
	}
}

func BenchmarkRowGet(b *testing.B) {
	if cacheRow == nil {
		log.Error().Msg("init not success")
		return
	}

	// 先删除
	cacheRow.DelCache(context.TODO(), []interface{}{123, 8})
	// 读取
	cacheRow.Get(context.TODO(), []interface{}{123, 8})
	// 再次读取
	cacheRow.Get(context.TODO(), []interface{}{123, 8})
	// 读取一个不存在的
	cacheRow.Get(context.TODO(), []interface{}{123, 110})
}

func BenchmarkRowGetsBySQLField(b *testing.B) {
	if cacheRow == nil {
		log.Error().Msg("init not success")
		return
	}

	// 先删除
	cacheRow.DelsCacheBySQLField(context.TODO(), map[string]interface{}{"UID": 123})
	// 读取
	cacheRow.GetsBySQLField(context.TODO(), map[string]interface{}{"GroupType": "G1"})
	// 再次读取
	cacheRow.GetsBySQLField(context.TODO(), map[string]interface{}{"GroupType": "G1"})
}

func BenchmarkRowExist(b *testing.B) {
	if cacheRow == nil {
		log.Error().Msg("init not success")
		return
	}

	// 先删除
	cacheRow.DelCache(context.TODO(), []interface{}{123, 8})
	// 判断一个存在的
	cacheRow.Exist(context.TODO(), []interface{}{123, 8})
	// 再次判断
	cacheRow.Exist(context.TODO(), []interface{}{123, 8})
	// 判断一个不存在的
	cacheRow.Exist(context.TODO(), []interface{}{124, 8})
}

func BenchmarkRowAdd(b *testing.B) {
	if cacheRow == nil {
		log.Error().Msg("init not success")
		return
	}

	// 读取
	cacheRow.GetsBySQLField(context.TODO(), map[string]interface{}{"UID": 123})

	// 先删除
	cacheRow.Del(context.TODO(), []interface{}{124, 8})
	cacheRow.Del(context.TODO(), []interface{}{124, 9})

	type AddTest struct {
		Name string `db:"Name" json:"Name,omitempty"` //名字  不可为空
		Age  int    `db:"Age" json:"Age,omitempty"`   //年龄
	}
	s := &AddTest{
		Name: "Hello126",
		Age:  1000,
	}
	// 添加一个不存在的
	cacheRow.Add(context.TODO(), []interface{}{124, 8}, s, nil)
	// 添加一个不存在的
	sm := map[string]interface{}{
		"Name": "Hello127",
		"Age":  nil,
		"Mark": nil,
	}
	cacheRow.Add(context.TODO(), []interface{}{124, 9}, sm, NoRespOptions()) // 不需要返回值
}

func BenchmarkRowDel(b *testing.B) {
	if cacheRow == nil {
		log.Error().Msg("init not success")
		return
	}

	// 读取
	cacheRow.GetsBySQLField(context.TODO(), map[string]interface{}{"UID": 123})

	// 删除
	cacheRow.Del(context.TODO(), []interface{}{123, 8})
	cacheRow.Del(context.TODO(), []interface{}{124, 9}) // 不存在的

	cacheRow.Dels(context.TODO(), []interface{}{123, 5}, []interface{}{124, 6}, []interface{}{124, 9})

	cacheRow.DelsBySQLField(context.TODO(), map[string]interface{}{"UID": 123})
	cacheRow.DelsBySQLField(context.TODO(), map[string]interface{}{"UID": 124})
}

func BenchmarkRowSet(b *testing.B) {
	if cacheRow == nil {
		log.Error().Msg("init not success")
		return
	}

	type SetTest struct {
		Name string `db:"Name" json:"Name,omitempty"` //名字  不可为空
		Age  int    `db:"Age" json:"Age,omitempty"`   //年龄
		Json *TestJ `db:"Json" json:"Json,omitempty"` //标记 可以为空
	}

	// 先删除
	cacheRow.DelCache(context.TODO(), []interface{}{123, 8})
	cacheRow.DelCache(context.TODO(), []interface{}{126, 2})
	cacheRow.DelCache(context.TODO(), []interface{}{126, 3})

	s := &SetTest{
		Name: "Hello",
		Age:  1000,
	}
	// 设置一个不存在的
	// cacheRow.Set(NoExistCreate(context.TODO()), []interface{}{126, 2}, s)
	// cacheRow.Get(context.TODO(), []interface{}{126, 2})

	// // 设置一个不存在的 不创建
	// cacheRow.Set(context.TODO(), []interface{}{126, 3}, s)
	// cacheRow.Get(context.TODO(), []interface{}{126, 3})

	s = &SetTest{
		Name: "Hello2",
		Age:  10000,
	}
	// 设置一个存在的
	cacheRow.Set(context.TODO(), []interface{}{123, 8}, s, NoRespOptions()) // 没有返回值
	time.Sleep(time.Second * 2)
	s.Age = 1010
	cacheRow.Set(context.TODO(), []interface{}{123, 8}, s, NoRespOptions())
	time.Sleep(time.Second * 2)

	sm := map[string]interface{}{
		"Name": "Hello",
		"Age":  1000,
		"Mark": nil,
	}
	// 设置一个不存在的
	cacheRow.Set(context.TODO(), []interface{}{126, 3}, sm, CreateOptions())
	cacheRow.Get(context.TODO(), []interface{}{126, 3})

	// 设置一个不存在的 不创建
	cacheRow.Set(context.TODO(), []interface{}{126, 4}, sm, nil)
	cacheRow.Get(context.TODO(), []interface{}{126, 4})

	sm = map[string]interface{}{
		"Name": "Hello2",
		"Age":  nil,
	}
	// 设置一个存在的
	cacheRow.Set(context.TODO(), []interface{}{123, 8}, sm, NoRespOptions()) // 没有返回值
	time.Sleep(time.Second * 2)
	cacheRow.DelCache(context.TODO(), []interface{}{123, 8})
	cacheRow.Get(context.TODO(), []interface{}{123, 8}) // 有年龄大条件过滤，获取不到
}

func BenchmarkRowModify(b *testing.B) {
	if cacheRow == nil {
		log.Error().Msg("init not success")
		return
	}

	type ModifyTest struct {
		Name string `db:"Name" json:"Name,omitempty"` //名字  不可为空
		Age  int    `db:"Age" json:"Age,omitempty"`   //年龄
	}

	// 先删除
	cacheRow.DelCache(context.TODO(), []interface{}{123, 8})
	cacheRow.DelCache(context.TODO(), []interface{}{126, 2})
	cacheRow.DelCache(context.TODO(), []interface{}{126, 3})

	s := &ModifyTest{
		Name: "Hello",
		Age:  100,
	}
	// 设置一个不存在的
	cacheRow.Modify(context.TODO(), []interface{}{126, 2}, s, CreateOptions())
	cacheRow.Get(context.TODO(), []interface{}{126, 2})

	// 设置一个不存在的 不创建
	cacheRow.Modify(context.TODO(), []interface{}{126, 3}, s, nil)
	cacheRow.Get(context.TODO(), []interface{}{126, 3})

	s = &ModifyTest{
		Name: "Hello2",
		Age:  10000,
	}
	// 设置一个存在的
	cacheRow.Modify(context.TODO(), []interface{}{123, 8}, s, NoRespOptions()) // 没有返回值
	time.Sleep(time.Second * 2)
	cacheRow.DelCache(context.TODO(), []interface{}{123, 8})

	sm := map[string]interface{}{
		"Name": "Hello",
		"Age":  100,
	}
	// 设置一个不存在的
	cacheRow.Modify(context.TODO(), []interface{}{126, 3}, sm, CreateOptions())
	cacheRow.Get(context.TODO(), []interface{}{126, 3})

	// 设置一个不存在的 不创建
	cacheRow.Modify(context.TODO(), []interface{}{126, 4}, sm, nil)
	cacheRow.Get(context.TODO(), []interface{}{126, 4})

	sm = map[string]interface{}{
		"Name": "Hello2",
		"Age":  10000,
	}
	// 设置一个存在的
	cacheRow.Modify(context.TODO(), []interface{}{123, 8}, sm, NoRespOptions()) // 没有返回值
	time.Sleep(time.Second * 2)
	cacheRow.DelCache(context.TODO(), []interface{}{123, 8})
	cacheRow.Get(context.TODO(), []interface{}{123, 8})

	// 性能测试下 转化耗时
	cacheRow.Get(context.TODO(), []interface{}{126, 2})
	ctx := utils.CtxSetNolog(context.TODO())
	entry := time.Now()
	for i := 0; i < 100000; i++ {
		goredis.DefaultRedis().Do2(ctx, []interface{}{"hmget", "mrr_test_{126}_2", "Id", "ct", "ut", "U", "Type", "Name", "Age", "Mark"}...)
	}
	fmt.Println(time.Since(entry))

	entry = time.Now()
	for i := 0; i < 100000; i++ {
		goredis.DefaultRedis().Do(ctx, []interface{}{"hmget", "mrr_test_{126}_2", "Id", "ct", "ut", "U", "Type", "Name", "Age", "Mark"}...)
	}
	fmt.Println(time.Since(entry))

	entry = time.Now()
	for i := 0; i < 100000; i++ {
		cacheRow.Get(ctx, []interface{}{126, 2})
	}
	fmt.Println(time.Since(entry))
}

func BenchmarkRowModify2(b *testing.B) {
	if cacheRow == nil {
		log.Error().Msg("init not success")
		return
	}

	type ModifyTest struct {
		Name string  `db:"Name" json:"Name,omitempty"` //名字  不可为空
		Age  int     `db:"Age" json:"Age,omitempty"`   //年龄
		Mark *string `db:"Mark" json:"Mark,omitempty"` //标记 可以为空
	}

	// 先删除
	cacheRow.DelCache(context.TODO(), []interface{}{123, 8})
	cacheRow.DelCache(context.TODO(), []interface{}{126, 2})
	cacheRow.DelCache(context.TODO(), []interface{}{126, 3})
	cacheRow.DelCache(context.TODO(), []interface{}{126, 4})
	cacheRow.DelCache(context.TODO(), []interface{}{126, 5})

	str := "tttt"
	s := &ModifyTest{
		Name: "Hello",
		Age:  100,
		Mark: &str,
	}
	// 设置一个不存在的
	cacheRow.Modify2(context.TODO(), []interface{}{126, 2}, s, CreateOptions())
	cacheRow.Get(context.TODO(), []interface{}{126, 2})

	// 设置一个不存在的 不创建
	cacheRow.Modify2(context.TODO(), []interface{}{126, 3}, s, nil)
	time.Sleep(time.Second * 2)
	cacheRow.Get(context.TODO(), []interface{}{126, 3})

	s = &ModifyTest{
		Name: "Hello2",
		Age:  10000,
	}
	// 设置一个存在的
	cacheRow.Modify2(context.TODO(), []interface{}{123, 8}, s, NoRespOptions()) // 设置没有返回值是无效 ModifyM2一定有返回值
	time.Sleep(time.Second * 2)
	cacheRow.DelCache(context.TODO(), []interface{}{123, 8})
	cacheRow.Get(context.TODO(), []interface{}{123, 8})

	sm := map[string]interface{}{
		"Name": "Hello",
		"Age":  100,
	}
	// 设置一个不存在的
	cacheRow.Modify2(context.TODO(), []interface{}{126, 4}, sm, CreateOptions())
	cacheRow.Get(context.TODO(), []interface{}{126, 4})

	// 设置一个不存在的 不创建
	cacheRow.Modify2(context.TODO(), []interface{}{126, 5}, sm, nil)
	cacheRow.Get(context.TODO(), []interface{}{126, 5})

	sm = map[string]interface{}{
		"Name": "Hello2",
		"Age":  nil,
	}
	// 设置一个存在的
	cacheRow.Modify2(context.TODO(), []interface{}{123, 8}, sm, NoRespOptions()) // 设置没有返回值是无效 ModifyM2一定有返回值
	time.Sleep(time.Second * 2)
	cacheRow.DelCache(context.TODO(), []interface{}{123, 8})
	cacheRow.Get(context.TODO(), []interface{}{123, 8})
}

func BenchmarkRowsGetAll(b *testing.B) {
	if cacheRows == nil {
		log.Error().Msg("init not success")
		return
	}

	// 先删除缓存
	cacheRows.DelAllCache(context.TODO(), []interface{}{123})

	cacheRows.GetAll(context.TODO(), []interface{}{123})
	// 在获取一次
	cacheRows.GetAll(context.TODO(), []interface{}{123})
}

func BenchmarkRowsGets(b *testing.B) {
	if cacheRows == nil {
		log.Error().Msg("init not success")
		return
	}

	// 先删除缓存
	cacheRows.DelAllCache(context.TODO(), []interface{}{123})

	cacheRows.Gets(context.TODO(), []interface{}{123}, [][]interface{}{{4, "G0"}, {5, "G0"}, {5, "G1"}})
	// 会重新加载
	cacheRows.Gets(context.TODO(), []interface{}{123}, [][]interface{}{{5, "G0"}, {3, "G0"}})
	// 不会重新加载
	cacheRows.Gets(context.TODO(), []interface{}{123}, [][]interface{}{{4, "G0"}, {5, "G0"}, {5, "G1"}})
	// 在获取一次
	cacheRows.Gets(context.TODO(), []interface{}{123}, [][]interface{}{{5, "G0"}, {4, "G0"}})
	// 会重新加载
	cacheRows.Gets(context.TODO(), []interface{}{123}, [][]interface{}{{4, "G0"}, {5, "G0"}, {155, "G1"}})
}

func BenchmarkRowsGet(b *testing.B) {
	if cacheRows == nil {
		log.Error().Msg("init not success")
		return
	}

	// 先删除缓存
	cacheRows.DelAllCache(context.TODO(), []interface{}{123})

	cacheRows.Get(context.TODO(), []interface{}{123}, []interface{}{8, "G1"})
	// 在获取一次
	cacheRows.Get(context.TODO(), []interface{}{123}, []interface{}{8, "G1"})
	// 不存在的
	cacheRows.Get(context.TODO(), []interface{}{123}, []interface{}{100, "G1"})
	cacheRows.Get(context.TODO(), []interface{}{123}, []interface{}{100, "G1"})
	// 不存在的
	cacheRows.Get(context.TODO(), []interface{}{123}, []interface{}{9, "G1"})
}

func BenchmarkRowsGetBySQLField(b *testing.B) {
	if cacheRows == nil {
		log.Error().Msg("init not success")
		return
	}

	// 先删除缓存
	cacheRows.DelAllCache(context.TODO(), []interface{}{123})

	cacheRows.GetBySQLField(context.TODO(), map[string]interface{}{"Name": "Name123_5"})
	// 在获取一次
	cacheRows.GetBySQLField(context.TODO(), map[string]interface{}{"Name": "Name123_5"})
	// 不存在的
	cacheRows.GetBySQLField(context.TODO(), map[string]interface{}{"Name": "Name123_555"})
	cacheRows.GetBySQLField(context.TODO(), map[string]interface{}{"GroupType": "G1"})
	// 不存在的
	cacheRows.GetBySQLField(context.TODO(), map[string]interface{}{"NameA": "Name123_555"})
}

func BenchmarkRowsExist(b *testing.B) {
	if cacheRows == nil {
		log.Error().Msg("init not success")
		return
	}

	// 先删除缓存
	cacheRows.DelAllCache(context.TODO(), []interface{}{123})

	cacheRows.Exist(context.TODO(), []interface{}{123}, []interface{}{8, "G1"})
	// 在获取一次
	cacheRows.Exist(context.TODO(), []interface{}{123}, []interface{}{8, "G1"})

	// 不存在的
	cacheRows.Exist(context.TODO(), []interface{}{123}, []interface{}{18, "G1"})
	cacheRows.Exist(context.TODO(), []interface{}{124}, []interface{}{8, "G1"})
}

func BenchmarkRowsAdd(b *testing.B) {
	if cacheRow == nil {
		log.Error().Msg("init not success")
		return
	}

	// 先删除缓存
	cacheRows.DelAllCache(context.TODO(), []interface{}{123})

	type AddTest struct {
		Name      string `db:"Name" json:"Name,omitempty"`           //名字  不可为空
		Age       int    `db:"Age" json:"Age,omitempty"`             //年龄
		Type      int    `db:"Type" json:"Type,omitempty"`           //数据字段
		GroupType string `db:"GroupType" json:"GroupType,omitempty"` //数据字段
	}
	s := &AddTest{
		Name:      "Hello12",
		Age:       1000,
		Type:      12,
		GroupType: "G1",
	}
	cacheRows.GetAll(context.TODO(), []interface{}{123})
	// 添加一个不存在的
	cacheRows.Add(context.TODO(), []interface{}{123}, s, NoRespOptions())

	cacheRows.Get(context.TODO(), []interface{}{123}, []interface{}{12, "G1"})

	// 添加一个存在的
	s.Type = 8
	cacheRows.Add(context.TODO(), []interface{}{123}, s, nil)

	// 添加一个不存在的
	sm := map[string]interface{}{
		"Name":      "Hello13",
		"Age":       10000,
		"Type":      13,
		"GroupType": "G1",
	}
	cacheRows.Add(context.TODO(), []interface{}{123}, sm, NoRespOptions()) // 没有返回值

	// 添加一个存在的
	sm["Type"] = 8
	cacheRows.Add(context.TODO(), []interface{}{123}, sm, nil)

	// nil 测试
	sm["Age"] = nil
	sm["Type"] = 14
	cacheRows.Add(context.TODO(), []interface{}{123}, sm, nil) // 数据可以写入，但读取会出错，age默认值是NULL 接受值是int 会转化失败，没有默认值的应该用指针接受
}

func BenchmarkRowsDel(b *testing.B) {
	if cacheRows == nil {
		log.Error().Msg("init not success")
		return
	}

	// 先删除缓存
	cacheRows.DelAllCache(context.TODO(), []interface{}{123})

	cacheRows.GetAll(context.TODO(), []interface{}{123})

	// 删除
	cacheRows.Del(context.TODO(), []interface{}{123}, []interface{}{8, "G1"})

	cacheRows.Dels(context.TODO(), []interface{}{123}, [][]interface{}{{9, "G1"}, {7, "G0"}})
	cacheRows.Dels(context.TODO(), []interface{}{123}, [][]interface{}{{9, "G1"}, {7, "G0"}})
	// 再次删除
	cacheRows.Del(context.TODO(), []interface{}{123}, []interface{}{8, "G1"})

	// 不存在
	cacheRows.Exist(context.TODO(), []interface{}{123}, []interface{}{8, "G1"})

	// 再次删除
	cacheRows.DelAll(context.TODO(), []interface{}{123})
}

func BenchmarkRowsSet(b *testing.B) {
	if cacheRows == nil {
		log.Error().Msg("init not success")
		return
	}

	// 先删除缓存
	cacheRows.DelAllCache(context.TODO(), []interface{}{123})

	type SetTest struct {
		Name      string  `db:"Name" json:"Name,omitempty"`           //名字
		Age       int     `db:"Age" json:"Age,omitempty"`             //年龄
		Type      int     `db:"Type" json:"Type,omitempty"`           //数据字段
		GroupType string  `db:"GroupType" json:"GroupType,omitempty"` //数据字段
		Mark      *string `db:"Mark" json:"Mark,omitempty"`           //标记 可以为空
	}

	ss := "sdfasdf"
	s := &SetTest{
		Name:      "Hello",
		Age:       100,
		Type:      8,
		GroupType: "G1",
		Mark:      &ss,
	}
	// 设置一个存在的
	cacheRows.Set(context.TODO(), []interface{}{123}, s, nil)
	time.Sleep(time.Second * 2)
	// 设置一个存在的 没有返回值
	s.Age = 1000
	cacheRows.Set(context.TODO(), []interface{}{123}, s, NoRespOptions()) // 没有返回值
	time.Sleep(time.Second * 2)

	// 设置一个不存在的 并创建
	s.Type = 12
	cacheRows.Set(context.TODO(), []interface{}{123}, s, CreateOptions())
	cacheRows.Get(context.TODO(), []interface{}{123}, []interface{}{12, "G1"})
	time.Sleep(time.Second * 2)

	// 设置一个不存在的，不创建
	s.Type = 13
	cacheRows.Set(context.TODO(), []interface{}{123}, s, NoRespOptions()) // 没有返回值
	time.Sleep(time.Second * 2)

	sm := map[string]interface{}{
		"Name":      "Hello",
		"Age":       100,
		"Type":      8,
		"GroupType": "G1",
	}
	// 设置一个存在的
	cacheRows.Set(context.TODO(), []interface{}{123}, sm, nil)
	// 设置一个存在的 不要返回值
	sm["Age"] = 100000
	cacheRows.Set(context.TODO(), []interface{}{123}, sm, NoRespOptions()) // 没有返回值
	time.Sleep(time.Second * 2)

	// 设置一个不存在的 并创建
	sm["Type"] = 14
	cacheRows.Set(context.TODO(), []interface{}{123}, sm, CreateOptions())
	cacheRows.Get(context.TODO(), []interface{}{123}, []interface{}{14, "G1"})
	time.Sleep(time.Second * 2)

	// 设置一个不存在的，不创建
	sm["Type"] = 15
	cacheRows.Set(context.TODO(), []interface{}{123}, sm, NoRespOptions()) // 没有返回值
	time.Sleep(time.Second * 2)
}

func BenchmarkRowsModify(b *testing.B) {
	if cacheRows == nil {
		log.Error().Msg("init not success")
		return
	}

	// 先删除缓存
	cacheRows.DelAllCache(context.TODO(), []interface{}{123})

	type SetTest struct {
		Name      string  `db:"Name" json:"Name,omitempty"`           //名字
		Age       int     `db:"Age" json:"Age,omitempty"`             //年龄
		Type      int     `db:"Type" json:"Type,omitempty"`           //数据字段
		GroupType string  `db:"GroupType" json:"GroupType,omitempty"` //数据字段
		Mark      *string `db:"Mark" json:"Mark,omitempty"`           //标记 可以为空
	}

	ss := "sdfasdf"
	s := &SetTest{
		Name:      "Hello",
		Age:       100,
		Type:      8,
		GroupType: "G1",
		Mark:      &ss,
	}
	// 设置一个存在的
	cacheRows.Modify(context.TODO(), []interface{}{123}, s, nil)
	time.Sleep(time.Second * 2)
	// 设置一个存在的 没有返回值
	s.Age = 1000
	cacheRows.Modify(context.TODO(), []interface{}{123}, s, NoRespOptions()) // 没有返回值
	time.Sleep(time.Second * 2)

	// 设置一个不存在的 并创建
	s.Type = 12
	cacheRows.Modify(context.TODO(), []interface{}{123}, s, CreateOptions())
	cacheRows.Get(context.TODO(), []interface{}{123}, []interface{}{12, "G1"})
	time.Sleep(time.Second * 2)

	// 设置一个不存在的，不创建
	s.Type = 13
	cacheRows.Modify(context.TODO(), []interface{}{123}, s, NoRespOptions()) // 没有返回值
	time.Sleep(time.Second * 2)

	sm := map[string]interface{}{
		"Name":      "Hello",
		"Age":       100,
		"Type":      8,
		"GroupType": "G1",
	}
	// 设置一个存在的
	cacheRows.Modify(context.TODO(), []interface{}{123}, sm, nil)
	time.Sleep(time.Second * 2)
	// 设置一个存在的 不要返回值
	sm["Age"] = 100000
	cacheRows.Modify(context.TODO(), []interface{}{123}, sm, NoRespOptions()) // 没有返回值
	time.Sleep(time.Second * 2)

	// 设置一个不存在的 并创建
	sm["Type"] = 14
	cacheRows.Modify(context.TODO(), []interface{}{123}, sm, CreateOptions())
	cacheRows.Get(context.TODO(), []interface{}{123}, []interface{}{14, "G1"})
	time.Sleep(time.Second * 2)

	// 设置一个不存在的，不创建
	sm["Type"] = 15
	cacheRows.Modify(context.TODO(), []interface{}{123}, sm, NoRespOptions()) // 没有返回值
	time.Sleep(time.Second * 2)
}

func BenchmarkRowsModify2(b *testing.B) {
	if cacheRows == nil {
		log.Error().Msg("init not success")
		return
	}

	// 先删除缓存
	cacheRows.DelAllCache(context.TODO(), []interface{}{123})

	type SetTest struct {
		Name      string  `db:"Name" json:"Name,omitempty"`           //名字
		Age       int     `db:"Age" json:"Age,omitempty"`             //年龄
		Type      int     `db:"Type" json:"Type,omitempty"`           //数据字段
		GroupType string  `db:"GroupType" json:"GroupType,omitempty"` //数据字段
		Mark      *string `db:"Mark" json:"Mark,omitempty"`           //标记 可以为空
	}

	ss := "sdfasdf"
	s := &SetTest{
		Name:      "Hello",
		Age:       100,
		Type:      8,
		GroupType: "G1",
		Mark:      &ss,
	}
	// 设置一个存在的
	cacheRows.Modify2(context.TODO(), []interface{}{123}, s, nil)
	time.Sleep(time.Second * 2)
	// 设置一个存在的 没有返回值
	s.Age = 1000
	cacheRows.Modify2(context.TODO(), []interface{}{123}, s, NoRespOptions()) // 一定有返回值 设置了NoRespOptions无效
	time.Sleep(time.Second * 2)

	// 设置一个不存在的 并创建
	s.Type = 12
	cacheRows.Modify2(context.TODO(), []interface{}{123}, s, CreateOptions())
	cacheRows.Get(context.TODO(), []interface{}{123}, []interface{}{12, "G1"})
	time.Sleep(time.Second * 2)

	// 设置一个不存在的，不创建
	s.Type = 13
	cacheRows.Modify2(context.TODO(), []interface{}{123}, s, NoRespOptions()) // 一定有返回值 设置了NoRespOptions无效
	time.Sleep(time.Second * 2)

	sm := map[string]interface{}{
		"Name":      "Hello",
		"Age":       100,
		"Type":      8,
		"GroupType": "G1",
	}
	// 设置一个存在的
	cacheRows.Modify2(context.TODO(), []interface{}{123}, sm, nil)
	time.Sleep(time.Second * 2)
	// 设置一个存在的 不要返回值
	sm["Age"] = 100000
	cacheRows.Modify2(context.TODO(), []interface{}{123}, sm, NoRespOptions()) // 一定有返回值 设置了NoRespOptions无效
	time.Sleep(time.Second * 2)

	// 设置一个不存在的 并创建
	sm["Type"] = 14
	cacheRows.Modify2(context.TODO(), []interface{}{123}, sm, CreateOptions())
	cacheRows.Get(context.TODO(), []interface{}{123}, []interface{}{14, "G1"})
	time.Sleep(time.Second * 2)

	// 设置一个不存在的，不创建
	sm["Type"] = 15
	cacheRows.Modify2(context.TODO(), []interface{}{123}, sm, NoRespOptions()) // 一定有返回值 设置了NoRespOptions无效
	time.Sleep(time.Second * 2)
}

func BenchmarkRowsJsonArrayFieldAdd(b *testing.B) {
	if cacheRows == nil {
		log.Error().Msg("init not success")
		return
	}

	// 先删除缓存
	cacheRows.DelAllCache(context.TODO(), []interface{}{123})

	// 设置一个存在的
	data := map[string][]string{}
	data["Strs"] = []string{"abc"}
	cacheRows.JsonArrayFieldDel(context.TODO(), []interface{}{123}, []interface{}{9, "G1"}, data, CreateOptions().JsonArrayDuplicate())

	data["Strs"] = []string{"abc", "left"}
	cacheRows.JsonArrayFieldAdd(context.TODO(), []interface{}{123}, []interface{}{9, "G1"}, data, nil)
	data["Strs"] = []string{"abc", "abcd"}
	cacheRows.JsonArrayFieldAdd(context.TODO(), []interface{}{123}, []interface{}{9, "G1"}, data, nil)
	time.Sleep(time.Second * 2)

	// 清除下缓存
	cacheRows.DelAllCache(context.TODO(), []interface{}{123})
	// abcd 应该加不进去
	data["Strs"] = []string{"123", "abcd"}
	cacheRows.JsonArrayFieldAdd(context.TODO(), []interface{}{123}, []interface{}{9, "G1"}, data, CreateOptions().JsonArrayDuplicate())
	time.Sleep(time.Second * 2)

	data["Strs"] = []string{"123", "abcd"}
	cacheRows.JsonArrayFieldDel(context.TODO(), []interface{}{123}, []interface{}{9, "G1"}, data, nil)

	time.Sleep(time.Second * 2)

	// abc 全删掉
	data["Strs"] = []string{"abc"}
	cacheRows.JsonArrayFieldDel(context.TODO(), []interface{}{123}, []interface{}{9, "G1"}, data, CreateOptions().JsonArrayDuplicate())

	time.Sleep(time.Second * 2)

}
