package goredis

// https://github.com/yuwf

// Redis结果绑定，用这种方式认为不需要太关心Redis执行结果真的正确与否
type RedisResultBind interface {
	// BindKind : Bool, Int, Int8, Int16, Int32, Int64, Uint, Uint8, Uint16, Uint32, Uint64, Uintptr, Float32, Float64, String, []byte
	// 传入的参数为对象的地址
	Bind(v interface{}) error

	// BindKind : Slice
	// SliceElemKind : Bool, Int, Int8, Int16, Int32, Int64, Uint, Uint8, Uint16, Uint32, Uint64, Uintptr, Float32, Float64, String
	// 传入的参数为Slice的地址 nil的Slice也可以
	BindSlice(v interface{}) error

	// BindType : Map
	// MapElemType : Bool, Int, Int8, Int16, Int32, Int64, Uint, Uint8, Uint16, Uint32, Uint64, Uintptr, Float32, Float64, String
	// 传入的参数为Map的地址 nil的Map也可以
	BindMap(v interface{}) error

	// 针对HMGET命令 调用Cmd时，参数不需要包括field
	// 结构成员首字母需要大写，tag中必须是包含 `redis:"hello"`  其中hello就表示在redis中存储的field名称
	// 结构成员类型 : Bool, Int, Int8, Int16, Int32, Int64, Uint, Uint8, Uint16, Uint32, Uint64, Uintptr, Float32, Float64, String, []byte
	// 传入的参数为结构的地址
	// 参数组织调用 hmsetObjArgs hmgetObjArgs
	HMGetBindObj(v interface{}) error

	// BindType : Struct
	// 结构支持json格式化
	// 传入的参数为结构的地址
	BindJsonObj(v interface{}) error

	// BindType : []Struct 或者 []*Struct
	// SliceElemType 支持json格式化
	// 传入的参数为Slice的地址 nil的Slice也可以
	BindJsonObjSlice(v interface{}) error
}
