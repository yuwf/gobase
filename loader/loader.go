package loader

// https://github.com/yuwf/gobase

// ConfLoader 配置接口
type Loader interface {
	// 获取原始配置 返回nil表示未加载
	GetSrc() []byte

	// 加载接口 src：原始配置数据 path：相关的路径
	Load(src []byte, path string) error

	// 从本地文件加载
	LoadFile(path string) error

	// 保存本地
	SaveFile(path string) error
}

// Loaders 多配置接口
type Loaders interface {
	// 获取原始配置 nil表示未加载过
	GetSrc() map[string][]byte

	// 获取原始配置 nil表示未加载过
	GetSrcItem(key string) []byte

	// 加载接口 src：原始配置数据 path：相关的路径
	Load(src map[string][]byte, path string) error
}

// 创建对象时调用
type Creater interface {
	Create()
}

// json读取完后调用
type Normalizer interface {
	Normalize()
}
