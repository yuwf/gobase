package utils

// https://github.com/yuwf/gobase

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/rs/zerolog/log"
)

// json不支持的格式化类型 Complex64，Complex128，Chan，Func，UnsafePointer
// Marshal时会返回一个错误 json: unsupported type: ***
// 所以下面涉及转化的地方也遵循这个规则

// 获取对象的tag值列表(第一个)和成员的item
/*
type Test struct {
	F1 int `redis:"f1"`
	F2 int `redis:"f2"`
}
*/

// 结构信息
type StructInfo struct {
	I      interface{}
	T      reflect.Type
	V      reflect.Value
	Tags   []interface{} // string类型，为了方便外部使用
	Elemts []reflect.Value
}

// 根据tag获取结构信息
func GetStructInfoByTag(i interface{}, tagName string) (*StructInfo, error) {
	// 验证参数 structptr可以是结构也可以是结构地址
	var structtype reflect.Type
	var structvalue reflect.Value
	vo := reflect.ValueOf(i)
	if vo.Kind() == reflect.Ptr {
		if vo.IsNil() {
			err := errors.New("pointer is nil")
			return nil, err
		}
		structtype = vo.Elem().Type() // 第一层是指针，第二层是结构
		structvalue = vo.Elem()
		if structtype.Kind() != reflect.Struct {
			err := errors.New("not struct or struct pointer")
			return nil, err
		}
	} else if vo.Kind() == reflect.Struct {
		structtype = vo.Type()
		structvalue = vo
	} else {
		err := errors.New("not struct or struct pointer")
		return nil, err
	}

	numField := structtype.NumField()
	tags := make([]interface{}, 0, numField)
	elemts := make([]reflect.Value, 0, numField) // 结构中成员的变量地址
	for i := 0; i < numField; i += 1 {
		v := structvalue.Field(i)
		tag := structtype.Field(i).Tag.Get(tagName)
		if tag == "-" || tag == "" {
			continue
		}
		sAt := strings.IndexByte(tag, ',')
		if sAt != -1 {
			tag = tag[0:sAt]
		}
		elemts = append(elemts, v)
		tags = append(tags, tag)
	}

	sInfo := &StructInfo{
		I:      i,
		T:      structtype,
		V:      structvalue,
		Tags:   tags,
		Elemts: elemts,
	}
	return sInfo, nil
}

func (si *StructInfo) FindIndexByTag(tag interface{}) int {
	for i, f := range si.Tags {
		if f == tag {
			return i
		}
	}
	return -1
}

// 不区分大消息查找
func (si *StructInfo) FindIndexByTagFold(tag string) int {
	for i, f := range si.Tags {
		if strings.EqualFold(f.(string), tag) {
			return i
		}
	}
	return -1
}

// 拷贝tag相同的字段，相同类型的引用为浅拷贝
func (si *StructInfo) CopyTo(sInfo *StructInfo) {
	for i, v := range si.Elemts {
		if !v.CanInterface() {
			continue
		}
		if at := sInfo.FindIndexByTag(si.Tags[i]); at != -1 {
			InterfaceToValue(v.Interface(), sInfo.Elemts[at])
		}
	}
}

// tag和elemt格式化 []interface{}{"f1" F1 "f2" F2} error
// 无法转化的，或者空数据，填充nil
// 如果成员是复核对象，则使用json格式化，总之返回值中都是可读的值类型
func (s *StructInfo) TagElemtFmt() []interface{} {
	rst := make([]interface{}, 0, len(s.Tags)*2)
	for i, v := range s.Elemts {
		rst = append(rst, s.Tags[i])
		rst = append(rst, ValueFmt(v))
	}
	return rst
}

// 过滤掉nil的
func (s *StructInfo) TagElemtNoNilFmt() []interface{} {
	rst := make([]interface{}, 0, len(s.Tags)*2)
	for i, v := range s.Elemts {
		vfmt := ValueFmt(v)
		if vfmt == nil {
			continue
		}
		rst = append(rst, s.Tags[i])
		rst = append(rst, vfmt)
	}
	return rst
}

// 无法格式化的 返回一个nil
func ValueFmt(v reflect.Value) interface{} {
	if !v.CanInterface() {
		return nil
	}
	switch v.Kind() {
	case reflect.Bool:
		return v.Interface()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Interface()
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return v.Interface()
	case reflect.Float32, reflect.Float64:
		return v.Interface()
	case reflect.Complex64:
		return nil
	case reflect.Complex128:
		return nil
	case reflect.Array:
		if v.Type().Elem().Kind() == reflect.Uint8 {
			return v.Interface()
		}
		return nil
	case reflect.Interface:
		if v.IsNil() {
			return nil
		}
		return ValueFmt(v.Elem())
	case reflect.Chan:
		return nil
	case reflect.Func:
		return nil
	case reflect.Map:
		if v.IsNil() {
			return nil
		}
		return valueFmtJson(v)
	case reflect.Pointer:
		if v.IsNil() {
			return nil
		}
		return ValueFmt(v.Elem())
	case reflect.Slice:
		if v.IsNil() {
			return nil
		}
		if v.Type().Elem().Kind() == reflect.Uint8 {
			return v.Interface()
		}
		return valueFmtJson(v)
	case reflect.String:
		return v.Interface()
	case reflect.Struct:
		t, ok := v.Interface().(time.Time) // Time类型 存时间戳,毫秒级别
		if ok {
			return t.UnixMilli()
		}
		return valueFmtJson(v)
	case reflect.UnsafePointer:
		return nil
	}
	return nil
}

func valueFmtJson(v reflect.Value) interface{} {
	data, err := json.Marshal(v.Interface())
	if err == nil {
		return data
	}
	return ""
}

// interface到Value的转化
// 如果i和v是指针，会取他的Elem，只取一层
// v对象必须有效
// 相同的值类型都是拷贝方式
// 相同引用类型也是引用赋值，不会拷贝
// 不同类型转化，都是拷贝的方式，无论引用到引用 或者引用到值
// []byte 和 string 不能直接转化的，都尝试用json转化
// Slice 和 Map 他们子元素类型一样 也能相互转化 slice->map slice必须是偶数 Map->slice待验证
func InterfaceToValue(i interface{}, v reflect.Value) (rstErr error) {
	if i == nil {
		rstErr = fmt.Errorf("nil can not to %s : Not CanSet", v.Type().String())
		return
	}
	io := reflect.ValueOf(i)
	src := io
	if src.Kind() == reflect.Pointer || src.Kind() == reflect.Interface {
		if src.IsNil() {
			rstErr = fmt.Errorf("nil can not to %s : Not CanSet", v.Type().String())
			return
		}
		src = src.Elem()
	}
	dst := v
	dstNew := false // 记录dst是否调用了new，如果转化失败，在设置会nil
	if dst.Kind() == reflect.Pointer {
		if dst.IsNil() {
			dstNew = true
			dst.Set(reflect.New(dst.Type().Elem()))
		}
		dst = dst.Elem()
	}
	if !dst.CanSet() {
		rstErr = fmt.Errorf("%s can not to %s : Not CanSet", io.Type().String(), v.Type().String())
		return
	}

	// 防止崩溃
	defer func() {
		if r := recover(); r != nil {
			// 修改返回值
			rstErr = fmt.Errorf("%s can not to %s : %v", io.String(), v.Type().String(), r)

			caller := GetCallerDesc(2)
			log.Error().Err(rstErr).Str("callPos", caller.Pos()).Msg("InterfaceToValueErr")
		}
	}()

	var ok bool
	switch src.Kind() {
	case reflect.Bool:
		if ok, rstErr = boolToValue(src.Bool(), dst); ok {
			return
		}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if ok, rstErr = intToValue(src.Int(), dst); ok {
			return
		}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		if ok, rstErr = uintToValue(src.Uint(), dst); ok {
			return
		}
	case reflect.Float32, reflect.Float64:
		if ok, rstErr = floatToValue(src.Float(), dst); ok {
			return
		}
	case reflect.Complex64, reflect.Complex128:
		switch dst.Kind() {
		case reflect.Complex64, reflect.Complex128:
			dst.SetComplex(src.Complex())
			return nil
		}
	case reflect.Array:
		if ok, rstErr = arrayToValue(src, dst); ok {
			return
		}
	case reflect.Chan:
		switch dst.Kind() {
		case reflect.Chan:
			if dst.Type().Elem() == src.Type().Elem() {
				dst.Set(src)
			}
		}
	case reflect.Func:
	case reflect.Interface:
	case reflect.Map:
		if ok, rstErr = mapToValue(src, dst); ok {
			return
		}
	case reflect.Pointer:
	case reflect.Slice:
		if ok, rstErr = sliceToValue(src, dst); ok {
			return
		}
	case reflect.String:
		if ok, rstErr = stringToValue(src, dst); ok {
			return
		}
	case reflect.Struct:
		if ok, rstErr = structToValue(src, dst); ok {
			return
		}
	case reflect.UnsafePointer:
	}

	if dstNew {
		dst.SetZero()
	}

	rstErr = fmt.Errorf("%s can not to %s : %v", io.Type().String(), v.Type().String(), rstErr)
	return
}

func boolToValue(b bool, dst reflect.Value) (bool, error) {
	switch dst.Kind() {
	case reflect.Bool:
		dst.SetBool(b)
		return true, nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		dst.SetInt(If[int64](b, 1, 0))
		return true, nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		dst.SetUint(If[uint64](b, 1, 0))
		return true, nil
	case reflect.Float32, reflect.Float64:
		dst.SetFloat(If[float64](b, 1, 0))
		return true, nil
	case reflect.String:
		dst.SetString(strconv.FormatBool(b))
		return true, nil
	}
	return false, nil
}

func intToValue(i int64, dst reflect.Value) (bool, error) {
	switch dst.Kind() {
	case reflect.Bool:
		dst.SetBool(i != 0)
		return true, nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		dst.SetInt(i)
		return true, nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		dst.SetUint(uint64(i))
		return true, nil
	case reflect.Float32, reflect.Float64:
		dst.SetFloat(float64(i))
		return true, nil
	case reflect.String:
		dst.SetString(strconv.FormatInt(i, 10))
		return true, nil
	}
	return false, nil
}

func uintToValue(u uint64, dst reflect.Value) (bool, error) {
	switch dst.Kind() {
	case reflect.Bool:
		dst.SetBool(u != 0)
		return true, nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		dst.SetInt(int64(u))
		return true, nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		dst.SetUint(u)
		return true, nil
	case reflect.Float32, reflect.Float64:
		dst.SetFloat(float64(u))
		return true, nil
	case reflect.String:
		dst.SetString(strconv.FormatUint(u, 10))
		return true, nil
	}
	return false, nil
}

func floatToValue(f float64, dst reflect.Value) (bool, error) {
	switch dst.Kind() {
	case reflect.Bool:
		dst.SetBool(FloatEqual(f, 0))
		return true, nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		dst.SetInt(int64(f))
		return true, nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		dst.SetUint(uint64(f))
		return true, nil
	case reflect.Float32, reflect.Float64:
		dst.SetFloat(f)
		return true, nil
	case reflect.String:
		dst.SetString(strconv.FormatFloat(f, 'f', -1, 64))
		return true, nil
	}
	return false, nil
}

func arrayToValue(src reflect.Value, dst reflect.Value) (bool, error) {
	switch dst.Kind() {
	case reflect.Array:
		fallthrough
	case reflect.Slice:
		if dst.Type().Elem() == src.Type().Elem() {
			reflect.Copy(dst, src)
			return true, nil
		}
	case reflect.String:
		if src.Type().Elem().Kind() == reflect.Uint8 {
			dst.SetString(BytesToString(src.Bytes())) // 需要取Array的地址取值
			return true, nil
		}
	}
	return false, nil
}

func mapToValue(src reflect.Value, dst reflect.Value) (bool, error) {
	switch dst.Kind() {
	case reflect.Map:
		if dst.Type().Elem() == src.Type().Elem() {
			dst.Set(src)
		}
	case reflect.Slice:
		if src.Type().Key() == dst.Type().Elem() && src.Type().Elem() == dst.Type().Elem() {
			d := reflect.New(dst.Type().Elem())
			for _, key := range src.MapKeys() {
				d = reflect.Append(d, key)
				d = reflect.Append(d, src.MapIndex(key))
			}
			dst.Set(d)
			return true, nil
		}
	}
	return false, nil
}

func sliceToValue(src reflect.Value, dst reflect.Value) (bool, error) {
	switch dst.Kind() {
	case reflect.Bool:
		if src.Type().Elem().Kind() == reflect.Uint8 {
			if len(src.Bytes()) == 0 {
				dst.SetZero()
				return true, nil
			}
			r, err := strconv.ParseBool(BytesToString(src.Bytes()))
			if err == nil {
				dst.SetBool(r)
				return true, nil
			}
			return false, err
		}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if src.Type().Elem().Kind() == reflect.Uint8 {
			if len(src.Bytes()) == 0 {
				dst.SetZero()
				return true, nil
			}
			r, err := strconv.ParseInt(BytesToString(src.Bytes()), 10, 0)
			if err == nil {
				dst.SetInt(r)
				return true, nil
			}
			return false, err
		}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		if src.Type().Elem().Kind() == reflect.Uint8 {
			if len(src.Bytes()) == 0 {
				dst.SetZero()
				return true, nil
			}
			r, err := strconv.ParseUint(BytesToString(src.Bytes()), 10, 0)
			if err == nil {
				dst.SetUint(r)
				return true, nil
			}
			return false, err
		}
	case reflect.Float32, reflect.Float64:
		if src.Type().Elem().Kind() == reflect.Uint8 {
			if len(src.Bytes()) == 0 {
				dst.SetZero()
				return true, nil
			}
			r, err := strconv.ParseFloat(BytesToString(src.Bytes()), 10)
			if err == nil {
				dst.SetFloat(r)
				return true, nil
			}
			return false, err
		}
	case reflect.Array:
		if dst.Type().Elem() == src.Type().Elem() {
			reflect.Copy(dst, src)
			return true, nil
		}
	case reflect.Map:
		if dst.Type().Key() == src.Type().Elem() && dst.Type().Elem() == src.Type().Elem() && src.Len()%2 == 0 {
			if dst.Pointer() == 0 { // 空map 创建下
				dst.Set(reflect.MakeMap(dst.Type()))
			}
			for i := 0; i+1 < src.Len(); i += 2 {
				dst.SetMapIndex(src.Index(i), src.Index(i+1))
			}
			return true, nil
		}
	case reflect.Slice:
		if dst.Type().Elem() == src.Type().Elem() {
			dst.Set(src)
			return true, nil
		}
	case reflect.String:
		if src.Type().Elem().Kind() == reflect.Uint8 {
			dst.SetString(BytesToString(src.Bytes()))
			return true, nil
		}
	case reflect.Complex64, reflect.Complex128, reflect.Chan, reflect.Func, reflect.UnsafePointer:
		return false, nil // json不支持这些类型
	}
	// 如果是[]byte 尝试通过json转化
	if src.Type().Elem().Kind() == reflect.Uint8 && dst.CanAddr() && dst.Addr().CanInterface() {
		err := json.Unmarshal(src.Bytes(), dst.Addr().Interface())
		if err == nil {
			return true, nil
		}
		return false, err
	}
	return false, nil
}

func stringToValue(src reflect.Value, dst reflect.Value) (bool, error) {
	if len(src.String()) == 0 { // 长度为空都能匹配
		return true, nil
	}
	switch dst.Kind() {
	case reflect.Bool:
		r, err := strconv.ParseBool(src.String())
		if err == nil {
			dst.SetBool(r)
			return true, nil
		}
		return false, err
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		r, err := strconv.ParseInt(src.String(), 10, 0)
		if err == nil {
			dst.SetInt(r)
			return true, nil
		}
		return false, err
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		r, err := strconv.ParseUint(src.String(), 10, 0)
		if err == nil {
			dst.SetUint(r)
			return true, nil
		}
		return false, err
	case reflect.Float32, reflect.Float64:
		r, err := strconv.ParseFloat(src.String(), 10)
		if err == nil {
			dst.SetFloat(r)
			return true, nil
		}
		return false, err
	case reflect.Array:
		if dst.Type().Elem().Kind() == reflect.Uint8 {
			reflect.Copy(dst, src)
			return true, nil
		}
	case reflect.Slice:
		if dst.Type().Elem().Kind() == reflect.Uint8 {
			dst.SetBytes([]byte(src.String())) // 不同类型都要拷贝，用StringToBytes并不是拷贝，外出修改会崩溃
			return true, nil
		}
	case reflect.String:
		dst.SetString(src.String())
		return true, nil
	case reflect.Complex64, reflect.Complex128, reflect.Chan, reflect.Func, reflect.UnsafePointer:
		return false, nil // json不支持这些类型
	case reflect.Struct:
		if dst.CanAddr() && dst.Addr().CanInterface() {
			t, ok := dst.Addr().Interface().(*time.Time)
			if ok {
				r, err := strconv.ParseInt(src.String(), 10, 0)
				if err == nil {
					l := len(src.String())
					if l == 10 {
						*t = time.Unix(r, 0)
						return true, nil
					} else if l == 13 {
						*t = time.UnixMilli(r)
						return true, nil
					} else if l == 16 {
						*t = time.UnixMicro(r)
						return true, nil
					} else if l == 19 {
						*t = time.Unix(r/1e9, r%1e9)
						return true, nil
					}
				} else {
					err := t.UnmarshalText(StringToBytes(src.String()))
					if err != nil {
						return true, nil
					}
				}
			}
		}
	}
	// 其他对象尝试通过json转化 必须指针，否则没有写的必要
	if dst.CanAddr() && dst.Addr().CanInterface() {
		err := json.Unmarshal(StringToBytes(src.String()), dst.Addr().Interface())
		if err == nil {
			return true, nil
		}
		return false, err
	}
	return false, nil
}

func structToValue(src reflect.Value, dst reflect.Value) (bool, error) {
	switch dst.Kind() {
	case reflect.Slice:
		data, err := json.Marshal(src.Interface())
		if err == nil {
			dst.SetBytes(data)
			return true, nil
		}
	case reflect.String:
		data, err := json.Marshal(src.Interface())
		if err == nil {
			dst.SetString(BytesToString(data))
			return true, nil
		}
		return false, err
	case reflect.Struct:
		if dst.Type() == src.Type() {
			dst.Set(src)
			return true, nil
		}
	}
	return false, nil
}
