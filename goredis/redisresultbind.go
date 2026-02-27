package goredis

import "reflect"

// https://github.com/yuwf/gobase

/*绑定声明变量 注意slice和map对象没有初始化，内部会进行初始化
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

// 默认读取tag中的redis，可以通过全局RedisTag变量修改
type Test struct {
	F1 int `redis:"f1"`
	F2 int `redis:"f2"`
}
var t Test
pipeline.HMSetObj("testmap", &t)
pipeline.HMGetObj("testmap", &t)

pipeline.Do()                           //执行管道命令，并解析返回值到绑定的对象上
*/

// Redis结果绑定，用这种方式认为不需要太关心Redis执行结果真的正确与否
type RedisResultBind interface {
	// BindKind : Bool, Int, Int8, Int16, Int32, Int64, Uint, Uint8, Uint16, Uint32, Uint64, Uintptr, Float32, Float64, String, []byte
	// 其他类型 : 通过Json转化
	// 传入的参数为对象的地址
	Bind(v interface{}) error

	// BindKind : Slice
	// SliceElemKind : Bool, Int, Int8, Int16, Int32, Int64, Uint, Uint8, Uint16, Uint32, Uint64, Uintptr, Float32, Float64, String
	// 其他类型 : 通过Json转化
	// 传入的参数为Slice的地址 nil的Slice也可以
	BindSlice(v interface{}) error

	// BindType : Map
	// MapElemType : Bool, Int, Int8, Int16, Int32, Int64, Uint, Uint8, Uint16, Uint32, Uint64, Uintptr, Float32, Float64, String
	// 其他类型 : 通过Json转化
	// 传入的参数为Map的地址 nil的Map也可以
	BindMap(v interface{}) error

	// BindType reflect.Value Value必须能调用CanSet
	BindValue(value reflect.Value) error

	// BindType []reflect.Value Value必须能调用CanSet
	BindValues(values []reflect.Value) error

	// BindType : Struct
	// 结构支持json格式化
	// 传入的参数为结构的地址
	BindJsonObj(v interface{}) error

	// BindType : []Struct 或者 []*Struct
	// SliceElemType 支持json格式化
	// 传入的参数为Slice的地址 nil的Slice也可以
	BindJsonObjSlice(v interface{}) error

	// BindType : []Struct 或者 []*Struct
	// SliceElemType 支持json格式化
	// 传入的参数为Map的地址 nil的Map也可以
	BindJsonObjMap(v interface{}) error
}
