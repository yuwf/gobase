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

// UID 和 Type 作为查询条件，有个Age>80的大条件
var cacheRow *CacheRow[Test]
var cacheRowAsnyc *CacheRow[Test]

func init() {
	DBNolog = false // 输出底层日志

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

	opts := &CacheOptions{
		HashTagField:   "UID",
		IncrementField: "Id",
		QueryCond:      NewConds().Ge("Age", 20),
	}
	cacheRow, err = NewCacheRow[Test](goredis.DefaultRedis(), mysql.DefaultMySQL(), "test", []string{"UID", "Type"}, opts)
	if err != nil {
		log.Error().Err(err).Msg("ConfigHashTag Err")
		return
	}
	opts.AsyncToMysql = true
	cacheRowAsnyc, err = NewCacheRow[Test](goredis.DefaultRedis(), mysql.DefaultMySQL(), "test", []string{"UID", "Type"}, opts)
	if err != nil {
		log.Error().Err(err).Msg("ConfigHashTag Err")
		return
	}
}

func BenchmarkDirtyKeyScript(b *testing.B) {
	goredis.DefaultRedis().Del(context.TODO(), "table_dirty_test")
	goredis.DefaultRedis().Del(context.TODO(), "table_dirty_test_processing")
	goredis.DefaultRedis().Del(context.TODO(), "table_dirty_test_lastcheck")

	goredis.DefaultRedis().Script(context.TODO(), dirtyKeyAddScript, []string{"table_dirty_test"}, "kkk1")
	goredis.DefaultRedis().Script(context.TODO(), dirtyKeyAddScript, []string{"table_dirty_test"}, "kkk2")
	goredis.DefaultRedis().Script(context.TODO(), dirtyKeyAddScript, []string{"table_dirty_test"}, "kkk3")
	goredis.DefaultRedis().Script(context.TODO(), dirtyKeyAddScript, []string{"table_dirty_test"}, "kkk4")
	goredis.DefaultRedis().Script(context.TODO(), dirtyKeyAddScript, []string{"table_dirty_test"}, "kkk5")

	keys := []string{}
	// 读取2个key kkk1 kkk2
	goredis.DefaultRedis().Script(context.TODO(), dirtyKeyGetScript, []string{"table_dirty_test", "table_dirty_test_processing", "table_dirty_test_lastcheck"}, 2, 10, "test_uuid").Bind(&keys)
	// kk1 完成处理
	goredis.DefaultRedis().Script(context.TODO(), dirtyKeyDoneScript, []string{"table_dirty_test_processing"}, keys[0], "test_uuid")

	time.Sleep(time.Second * 11)

	// 读取2个key kkk3 kkk4, kkk2超时返回列表
	keys = []string{}
	goredis.DefaultRedis().Script(context.TODO(), dirtyKeyGetScript, []string{"table_dirty_test", "table_dirty_test_processing", "table_dirty_test_lastcheck"}, 2, 10, "test_uuid").Bind(&keys)
}

func BenchmarkRowGet(b *testing.B) {
	if cacheRow == nil {
		log.Error().Msg("init not success")
		return
	}

	// 先删除
	cacheRow.DelCacheByQuery(context.TODO(), NewConds().Eq("UID", 123))
	// 读取
	cacheRow.Get(context.TODO(), []interface{}{123, 8})
	// 再次读取
	cacheRow.Get(context.TODO(), []interface{}{123, 8})
	// 读取一个不存在的
	cacheRow.Get(context.TODO(), []interface{}{123, 110})
}

func BenchmarkRowGetCondValuessByQuery(b *testing.B) {
	if cacheRow == nil {
		log.Error().Msg("init not success")
		return
	}

	// 先删除
	cacheRow.DelCacheByQuery(context.TODO(), NewConds().Eq("UID", 123))
	// 读取
	cacheRow.GetCondValuessByQuery(context.TODO(), NewConds().Eq("GroupType", "G1"))
}

func BenchmarkRowGetByQuery(b *testing.B) {
	if cacheRow == nil {
		log.Error().Msg("init not success")
		return
	}

	// 先删除
	cacheRow.DelCacheByQuery(context.TODO(), NewConds().Eq("UID", 123))
	// 读取
	cacheRow.GetByQuery(context.TODO(), NewConds().Eq("GroupType", "G1"))
	// 再次读取
	cacheRow.GetByQuery(context.TODO(), NewConds().Eq("GroupType", "G1"))
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
	cacheRow.GetByQuery(context.TODO(), NewConds().Eq("UID", 123))

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
	cacheRow.GetByQuery(context.TODO(), NewConds().Eq("UID", 123))

	// 删除
	cacheRow.Del(context.TODO(), []interface{}{123, 8})
	cacheRow.Del(context.TODO(), []interface{}{124, 9}) // 不存在的

	cacheRow.Dels(context.TODO(), []interface{}{123, 5}, []interface{}{124, 6}, []interface{}{124, 9})

	cacheRow.DelByQuery(context.TODO(), NewConds().Eq("UID", 123))
	cacheRow.DelByQuery(context.TODO(), NewConds().Eq("UID", 123))
	cacheRow.DelByQuery(context.TODO(), NewConds().Eq("UID", 124))
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
		Name: "Hello2",
		Age:  10000,
	}
	// 设置一个存在的
	cacheRow.Set(context.TODO(), []interface{}{123, 8}, s, NoRespOptions()) // 没有返回值
	s.Age = 1010
	cacheRow.Set(context.TODO(), []interface{}{123, 8}, s, NewOptions()) // 有返回值

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
	cacheRow.DelCache(context.TODO(), []interface{}{123, 8})
	cacheRow.Get(context.TODO(), []interface{}{123, 8}) // 有年龄大条件过滤，获取不到
}

func BenchmarkRowSetAsync(b *testing.B) {
	if cacheRowAsnyc == nil {
		log.Error().Msg("init not success")
		return
	}

	type SetTest struct {
		Name string `db:"Name" json:"Name,omitempty"` //名字  不可为空
		Age  int    `db:"Age" json:"Age,omitempty"`   //年龄
		Json *TestJ `db:"Json" json:"Json,omitempty"` //标记 可以为空
	}

	// 先删除
	cacheRow.DelDirtyKey(context.TODO())
	cacheRow.DelCache(context.TODO(), []interface{}{123, 8})
	cacheRow.DelCache(context.TODO(), []interface{}{126, 2})
	cacheRow.DelCache(context.TODO(), []interface{}{126, 3})

	s := &SetTest{
		Name: "Hello2",
		Age:  10000,
	}
	// 设置一个存在的
	cacheRowAsnyc.Set(context.TODO(), []interface{}{123, 8}, s, NoRespOptions()) // 没有返回值
	cacheRowAsnyc.Get(context.TODO(), []interface{}{123, 8})
	time.Sleep(time.Second * 5)
	s.Age = 1010
	cacheRowAsnyc.Set(context.TODO(), []interface{}{123, 8}, s, NewOptions()) // 有返回值
	time.Sleep(time.Second * 5)

	sm := map[string]interface{}{
		"Name": "Hello",
		"Age":  1000,
		"Mark": nil,
	}
	// 设置一个不存在的
	cacheRowAsnyc.Set(context.TODO(), []interface{}{126, 3}, sm, CreateOptions())
	cacheRowAsnyc.Get(context.TODO(), []interface{}{126, 3})

	// 设置一个不存在的 不创建
	cacheRowAsnyc.Set(context.TODO(), []interface{}{126, 4}, sm, nil)
	cacheRowAsnyc.Get(context.TODO(), []interface{}{126, 4})

	sm = map[string]interface{}{
		"Name": "Hello2",
		"Age":  nil,
	}
	// 设置一个存在的
	cacheRowAsnyc.Set(context.TODO(), []interface{}{123, 8}, sm, NoRespOptions()) // 没有返回值
	time.Sleep(time.Second * 2)
	cacheRowAsnyc.DelCache(context.TODO(), []interface{}{123, 8})
	cacheRowAsnyc.Get(context.TODO(), []interface{}{123, 8}) // 有年龄大条件过滤，获取不到
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
	// 设置一个不存在的 创建
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
	cacheRow.Modify(context.TODO(), []interface{}{123, 8}, s, NewOptions())    // 有返回值
	cacheRow.DelCache(context.TODO(), []interface{}{123, 8})

	sm := map[string]interface{}{
		"Name": "Hello",
		"Age":  100,
	}
	// 设置一个不存在的
	cacheRow.Modify(context.TODO(), []interface{}{126, 3}, sm, CreateOptions())
	cacheRow.Get(context.TODO(), []interface{}{126, 3})

	// 设置一个不存在的 不创建，会报错
	cacheRow.Modify(context.TODO(), []interface{}{126, 4}, sm, nil)
	cacheRow.Get(context.TODO(), []interface{}{126, 4})

	sm = map[string]interface{}{
		"Name": "Hello2",
		"Age":  10000,
	}
	// 设置一个存在的
	cacheRow.Modify(context.TODO(), []interface{}{123, 8}, sm, NoRespOptions()) // 没有返回值
	cacheRow.Modify(context.TODO(), []interface{}{123, 8}, sm, NewOptions())    // 有返回值
	cacheRow.DelCache(context.TODO(), []interface{}{123, 8})
	cacheRow.Get(context.TODO(), []interface{}{123, 8})

	// 性能测试下 转化耗时
	cacheRow.Get(context.TODO(), []interface{}{126, 2})
	ctx := utils.CtxSetNolog(context.TODO())
	entry := time.Now()
	for i := 0; i < 100000; i++ {
		goredis.DefaultRedis().Cmd(ctx, []interface{}{"hmget", "mrr_test_{126}_2", "Id", "ct", "ut", "U", "Type", "Name", "Age", "Mark"}...)
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

func BenchmarkRowModifySync(b *testing.B) {
	if cacheRow == nil {
		log.Error().Msg("init not success")
		return
	}

	type ModifyTest struct {
		Name string `db:"Name" json:"Name,omitempty"` //名字  不可为空
		Age  int    `db:"Age" json:"Age,omitempty"`   //年龄
	}

	// 先删除
	cacheRowAsnyc.DelDirtyKey(context.TODO())
	cacheRowAsnyc.DelCache(context.TODO(), []interface{}{123, 8})
	cacheRowAsnyc.DelCache(context.TODO(), []interface{}{126, 2})
	cacheRowAsnyc.DelCache(context.TODO(), []interface{}{126, 3})

	s := &ModifyTest{
		Name: "Hello",
		Age:  100,
	}
	// 设置一个不存在的
	cacheRowAsnyc.Modify(context.TODO(), []interface{}{126, 2}, s, CreateOptions())
	cacheRowAsnyc.Get(context.TODO(), []interface{}{126, 2})

	// 设置一个不存在的 不创建
	cacheRowAsnyc.Modify(context.TODO(), []interface{}{126, 3}, s, nil)
	cacheRowAsnyc.Get(context.TODO(), []interface{}{126, 3})

	s = &ModifyTest{
		Name: "Hello2",
		Age:  10000,
	}
	// 设置一个存在的
	cacheRowAsnyc.Modify(context.TODO(), []interface{}{123, 8}, s, NoRespOptions()) // 没有返回值
	time.Sleep(time.Second * 2)
	cacheRowAsnyc.Modify(context.TODO(), []interface{}{123, 8}, s, NewOptions()) // 有返回值
	time.Sleep(time.Second * 2)
	cacheRowAsnyc.DelCache(context.TODO(), []interface{}{123, 8})

	sm := map[string]interface{}{
		"Name": "Hello",
		"Age":  100,
	}
	// 设置一个不存在的
	cacheRowAsnyc.Modify(context.TODO(), []interface{}{126, 3}, sm, CreateOptions())
	cacheRowAsnyc.Get(context.TODO(), []interface{}{126, 3})

	// 设置一个不存在的 不创建，会报错
	cacheRowAsnyc.Modify(context.TODO(), []interface{}{126, 4}, sm, nil)
	cacheRowAsnyc.Get(context.TODO(), []interface{}{126, 4})

	sm = map[string]interface{}{
		"Name": "Hello2",
		"Age":  10000,
	}
	// 设置一个存在的 当前age=20080
	cacheRowAsnyc.Modify(context.TODO(), []interface{}{123, 8}, sm, NoRespOptions()) // 没有返回值
	cacheRowAsnyc.Modify(context.TODO(), []interface{}{123, 8}, sm, NewOptions())    // 有返回值
	cacheRowAsnyc.DelCache(context.TODO(), []interface{}{123, 8})
	time.Sleep(time.Second * 2) // 缓存删掉了，修改不会同步到mysql,日志记录下一个警告 下面获取的age还是20080
	cacheRowAsnyc.Get(context.TODO(), []interface{}{123, 8})
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
	// 设置一个不存在的 创建
	cacheRow.Modify2(context.TODO(), []interface{}{126, 2}, s, CreateOptions())
	cacheRow.Get(context.TODO(), []interface{}{126, 2})

	// 设置一个不存在的 不创建，会报错
	cacheRow.Modify2(context.TODO(), []interface{}{126, 3}, s, nil)
	cacheRow.Get(context.TODO(), []interface{}{126, 3})

	s = &ModifyTest{
		Name: "Hello2",
		Age:  10000,
	}
	// 设置一个存在的
	cacheRow.Modify2(context.TODO(), []interface{}{123, 8}, s, NoRespOptions()) // 设置没有返回值是无效 ModifyM2一定有返回值
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
	cacheRow.DelCache(context.TODO(), []interface{}{123, 8})
	cacheRow.Get(context.TODO(), []interface{}{123, 8})
}

func BenchmarkRowModify2Async(b *testing.B) {
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
	cacheRowAsnyc.DelDirtyKey(context.TODO())
	cacheRowAsnyc.DelCache(context.TODO(), []interface{}{123, 8})
	cacheRowAsnyc.DelCache(context.TODO(), []interface{}{126, 2})
	cacheRowAsnyc.DelCache(context.TODO(), []interface{}{126, 3})
	cacheRowAsnyc.DelCache(context.TODO(), []interface{}{126, 4})
	cacheRowAsnyc.DelCache(context.TODO(), []interface{}{126, 5})

	str := "tttt"
	s := &ModifyTest{
		Name: "Hello",
		Age:  100,
		Mark: &str,
	}
	// 设置一个不存在的 创建
	cacheRowAsnyc.Modify2(context.TODO(), []interface{}{126, 2}, s, CreateOptions())
	cacheRowAsnyc.Get(context.TODO(), []interface{}{126, 2})

	// 设置一个不存在的 不创建，会报错
	cacheRowAsnyc.Modify2(context.TODO(), []interface{}{126, 3}, s, nil)
	cacheRowAsnyc.Get(context.TODO(), []interface{}{126, 3})

	s = &ModifyTest{
		Name: "Hello2",
		Age:  10000,
	}
	// 设置一个存在的
	cacheRowAsnyc.Modify2(context.TODO(), []interface{}{123, 8}, s, NoRespOptions()) // 设置没有返回值是无效 ModifyM2一定有返回值
	time.Sleep(time.Second * 2)
	cacheRowAsnyc.DelCache(context.TODO(), []interface{}{123, 8})
	cacheRowAsnyc.Get(context.TODO(), []interface{}{123, 8})

	sm := map[string]interface{}{
		"Name": "Hello",
		"Age":  100,
	}
	// 设置一个不存在的 创建
	cacheRowAsnyc.Modify2(context.TODO(), []interface{}{126, 4}, sm, CreateOptions())
	cacheRowAsnyc.Get(context.TODO(), []interface{}{126, 4})

	// 设置一个不存在的 不创建
	cacheRowAsnyc.Modify2(context.TODO(), []interface{}{126, 5}, sm, nil)
	cacheRowAsnyc.Get(context.TODO(), []interface{}{126, 5})

	sm = map[string]interface{}{
		"Name": "Hello2",
		"Age":  nil,
	}
	// 设置一个存在的
	cacheRowAsnyc.Modify2(context.TODO(), []interface{}{123, 8}, sm, NoRespOptions()) // 设置没有返回值是无效 ModifyM2一定有返回值
	time.Sleep(time.Second * 2)
	cacheRowAsnyc.DelCache(context.TODO(), []interface{}{123, 8})
	cacheRowAsnyc.Get(context.TODO(), []interface{}{123, 8})
}

func BenchmarkRowJsonArray(b *testing.B) {
	if cacheRow == nil {
		log.Error().Msg("init not success")
		return
	}

	// 先删除缓存
	cacheRow.DelCacheByQuery(context.TODO(), NewConds().Eq("UID", 123))

	// 设置一个存在的
	data := map[string][]string{}
	data["Strs"] = []string{"abc"}
	cacheRow.JsonArrayDel(context.TODO(), []interface{}{123, 8}, data, CreateOptions().JsonArrayDuplicate())

	data["Strs"] = []string{"abc", "left"}
	cacheRow.JsonArrayAdd(context.TODO(), []interface{}{123, 8}, data, nil)
	data["Strs"] = []string{"abc", "abcd"}
	cacheRow.DelCacheByQuery(context.TODO(), NewConds().Eq("UID", 123)) // 删除缓存后再添加
	cacheRow.JsonArrayAdd(context.TODO(), []interface{}{123, 8}, data, nil)

	// 清除下缓存
	cacheRow.DelCacheByQuery(context.TODO(), NewConds().Eq("UID", 123))
	// abcd 应该加不进去
	data["Strs"] = []string{"123", "abcd"}
	cacheRow.JsonArrayAdd(context.TODO(), []interface{}{123, 8}, data, CreateOptions().JsonArrayDuplicate())
	data["Strs"] = []string{"123", "abcd"}
	cacheRow.JsonArrayDel(context.TODO(), []interface{}{123, 8}, data, nil)

	// abc 全删掉
	data["Strs"] = []string{"abc"}
	cacheRow.JsonArrayDel(context.TODO(), []interface{}{123, 8}, data, CreateOptions().JsonArrayDuplicate())
}

func BenchmarkRowJsonArrayAsync(b *testing.B) {
	if cacheRow == nil {
		log.Error().Msg("init not success")
		return
	}

	// 先删除缓存
	cacheRowAsnyc.DelDirtyKey(context.TODO())
	cacheRowAsnyc.DelCacheByQuery(context.TODO(), NewConds().Eq("UID", 123))

	// 设置一个存在的
	data := map[string][]string{}
	data["Strs"] = []string{"abc"}
	cacheRowAsnyc.JsonArrayDel(context.TODO(), []interface{}{123, 8}, data, CreateOptions().JsonArrayDuplicate())
	time.Sleep(time.Second * 2)

	data["Strs"] = []string{"abc", "left"}
	cacheRowAsnyc.JsonArrayAdd(context.TODO(), []interface{}{123, 8}, data, nil)
	data["Strs"] = []string{"abc", "abcd"}
	cacheRowAsnyc.JsonArrayAdd(context.TODO(), []interface{}{123, 8}, data, nil)
	time.Sleep(time.Second * 2)

	// 清除下缓存
	cacheRowAsnyc.DelCacheByQuery(context.TODO(), NewConds().Eq("UID", 123))
	// abcd 应该加不进去
	data["Strs"] = []string{"123", "abcd"}
	cacheRowAsnyc.JsonArrayAdd(context.TODO(), []interface{}{123, 8}, data, CreateOptions().JsonArrayDuplicate())
	time.Sleep(time.Second * 2)

	data["Strs"] = []string{"123", "abcd"}
	cacheRowAsnyc.JsonArrayDel(context.TODO(), []interface{}{123, 8}, data, nil)
	time.Sleep(time.Second * 2)

	// abc 全删掉
	data["Strs"] = []string{"abc"}
	cacheRowAsnyc.JsonArrayDel(context.TODO(), []interface{}{123, 8}, data, CreateOptions().JsonArrayDuplicate())

	time.Sleep(time.Second * 2)
}
