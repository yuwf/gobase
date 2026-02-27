package utils

// https://github.com/yuwf/gobase

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"reflect"
	"strconv"
	"strings"

	"github.com/rs/zerolog"
)

// 方便ctx中内容输出使用
func LogCtx(e *zerolog.Event, ctx context.Context) *zerolog.Event {
	if e != nil && ctx != nil {
		if traceId := ctx.Value(CtxKey_traceId); traceId != nil {
			e = e.Int64("_tid_", traceId.(int64))
		}
		if traceName := ctx.Value(CtxKey_traceName); traceName != nil {
			if s, ok := traceName.(string); ok && len(s) > 0 {
				e = e.Str("_tname_", s)
			}
		}
		if callers, ok := ctx.Value(CtxKey_callers).(*_ctx_callers_); ok {
			strs := make([]string, 0, len(callers.callers))
			for _, caller := range callers.callers {
				strs = append(strs, caller.Loc())
			}
			e = e.Strs("_callers_", strs)
		}

		if log, ok := ctx.Value(CtxKey_log).(*_ctx_logs_); ok {
			for _, v := range log.logs {
				switch val := v.Value.(type) {
				case string:
					e = e.Str(v.Key, val)
				case int:
					e = e.Int(v.Key, val)
				case int64:
					e = e.Int64(v.Key, val)
				case bool:
					e = e.Bool(v.Key, val)
				case float64:
					e = e.Float64(v.Key, val)
				case error:
					e = e.Err(val)
				default:
					e = e.Interface(v.Key, v.Value)
				}
			}
		}
	}
	return e
}

// 截断日志使用，防止日志过大，默认为4096字符
// 支持zerolog的Interface和Array接口
func TruncatedLog(obj interface{}) *TruncatedLog_ {
	return &TruncatedLog_{Obj: obj, MaxChars: 4096}
}

func TruncatedLog2(obj interface{}, max int) *TruncatedLog_ {
	return &TruncatedLog_{Obj: obj, MaxChars: max}
}

type TruncatedLog_ struct {
	Obj      interface{} // 原始数据
	MaxChars int         // 最大字符数（总输出长度）
}

func (l *TruncatedLog_) MarshalZerologObject(e *zerolog.Event) {
	if l.Obj == nil {
		e.Err(errors.New("nil"))
		return
	}

	v := reflect.ValueOf(l.Obj)
	if v.Kind() == reflect.Pointer {
		if v.IsNil() {
			e.Err(errors.New("nil pointer"))
			return
		}
		v = v.Elem()
	}

	switch v.Kind() {
	case reflect.Struct:
		t := v.Type()
		totalChars := 0
		for i := 0; i < v.NumField(); i++ {
			field := t.Field(i)
			vField := v.Field(i)
			if !vField.CanInterface() {
				continue // 跳过未导出字段
			}
			fieldVal := vField.Interface()

			var b []byte
			var err error
			if zerolog.InterfaceMarshalFunc != nil {
				b, err = zerolog.InterfaceMarshalFunc(fieldVal)
			} else {
				b, err = json.Marshal(fieldVal)
			}

			if err != nil {
				e.Err(err)
				continue
			}

			if l.MaxChars > 0 && len(b) > l.MaxChars-totalChars {
				if totalChars >= l.MaxChars {
					e.Str(field.Name, "...truncated")
				} else {
					e.Str(field.Name, string(b[:l.MaxChars-totalChars])+"...truncated")
				}
				return
			} else {
				e.RawJSON(field.Name, b)
				totalChars += len(b)

				// 如果还剩下分隔符的空间（可选）
				if i < v.NumField()-1 {
					totalChars++
				}
			}
		}
	case reflect.Map:
		iter := v.MapRange()
		totalChars := 0
		for iter.Next() {
			key := iter.Key()
			val := iter.Value()
			fieldName := fmt.Sprintf("%v", key.Interface())
			fieldVal := val.Interface()

			var b []byte
			var err error
			if zerolog.InterfaceMarshalFunc != nil {
				b, err = zerolog.InterfaceMarshalFunc(fieldVal)
			} else {
				b, err = json.Marshal(fieldVal)
			}
			if err != nil {
				e.Err(err)
				continue
			}
			if l.MaxChars > 0 && len(b) > l.MaxChars-totalChars {
				if totalChars >= l.MaxChars {
					e.Str(fieldName, "...truncated")
				} else {
					e.Str(fieldName, string(b[:l.MaxChars-totalChars])+"...truncated")
				}
			} else {
				e.RawJSON(fieldName, b)
				totalChars += len(b)

				// 如果还剩下分隔符的空间（可选）
				if iter.Next() {
					totalChars++
				}
			}
		}
	default:
		// 不是结构体，直接序列化为字符串写一个字段名 "value"
		var b []byte
		var err error
		if zerolog.InterfaceMarshalFunc != nil {
			b, err = zerolog.InterfaceMarshalFunc(l.Obj)
		} else {
			b, err = json.Marshal(l.Obj)
		}
		if err != nil {
			e.Err(err)
		}

		if l.MaxChars > 0 && len(b) > l.MaxChars {
			e.Str("value", string(b[:l.MaxChars])+"...("+strconv.Itoa(len(b))+")...truncated")
		} else {
			e.RawJSON("value", b)
		}
	}
}

func (l *TruncatedLog_) MarshalZerologArray(a *zerolog.Array) {
	if l.Obj == nil {
		return
	}

	v := reflect.ValueOf(l.Obj)
	if v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return
		}
		v = v.Elem()
	}

	switch v.Kind() {
	case reflect.Slice, reflect.Array:
		totalChars := 0
		// 遍历每个元素
		for i := 0; i < v.Len(); i++ {
			elem := v.Index(i).Interface()
			var b []byte
			var err error
			if zerolog.InterfaceMarshalFunc != nil {
				b, err = zerolog.InterfaceMarshalFunc(elem)
			} else {
				b, err = json.Marshal(elem)
			}

			if err != nil {
				a.Str(fmt.Sprintf("marshal error: %v", err))
				continue
			}

			// 检查是否超出限制
			if l.MaxChars > 0 && len(b) > l.MaxChars-totalChars {
				if totalChars >= l.MaxChars {
					a.Str("...(" + strconv.Itoa(v.Len()) + ")...truncated")
				} else {
					a.Str(string(b[:l.MaxChars-totalChars]) + "...(" + strconv.Itoa(v.Len()) + ")...truncated")
				}
				return
			} else {
				a.RawJSON(b)
				totalChars += len(b)

				// 如果还剩下分隔符的空间（可选）
				if i < v.Len()-1 {
					totalChars++
				}
			}
		}
	default:
		// 不是数组，直接序列化为字符串写入
		var b []byte
		var err error
		if zerolog.InterfaceMarshalFunc != nil {
			b, err = zerolog.InterfaceMarshalFunc(l.Obj)
		} else {
			b, err = json.Marshal(l.Obj)
		}
		if err != nil {
			a.Err(err)
		}

		if l.MaxChars > 0 && len(b) > l.MaxChars {
			a.Str(string(b[:l.MaxChars]) + "...(" + strconv.Itoa(len(b)) + ")...truncated")
		} else {
			a.RawJSON(b)
		}
	}
}

// 打印http相关的内容日志，细节待完善
func LogHttpBody(e *zerolog.Event, header http.Header, logkey string, data []byte, limit int) *zerolog.Event {
	val := header["Content-Type"]
	var contentType string
	if len(val) > 0 {
		contentType = val[0]
	}
	val = header["Content-Encoding"]
	var contentEncoding string
	if len(val) > 0 {
		contentEncoding = val[0]
	}
	if strings.Contains(contentType, "application/") {
		if limit > 0 && len(data) > limit {
			b := [][]byte{}
			b = append(b, data[0:limit/2])
			b = append(b, []byte("   ...("))
			b = append(b, []byte(strconv.Itoa(len(data))))
			b = append(b, []byte(")...   "))
			b = append(b, data[len(data)-limit/2:])
			e = e.Bytes(logkey, bytes.Join(b, []byte("")))
		} else {
			e = e.Bytes(logkey, data)
		}
	} else if strings.Contains(contentType, "text/") {
		if contentEncoding == "gzip" {
			// 读取压缩数据
			reader, err := gzip.NewReader(bytes.NewBuffer(data))
			if err == nil {
				all, err := ioutil.ReadAll(reader)
				if err == nil {
					data = all
				}
			}
		}
		str := string(data)
		if limit > 0 && len(str) > limit {
			str = fmt.Sprint(str[0:limit/2], "  ...(", len(str), ")...  ", str[len(str)-limit/2:])
		}
		e = e.Str(logkey, str)
	} else {
		e = e.Int(logkey+"size", len(data))
		return e
	}
	return e
}

func LogHttpInterface(e *zerolog.Event, header http.Header, logkey string, body interface{}, limit int) *zerolog.Event {
	data, err := json.Marshal(body)
	if err != nil {
		e = e.Interface(logkey, body)
		return e
	}
	if limit > 0 && len(data) > limit {
		b := [][]byte{}
		b = append(b, data[0:limit/2])
		b = append(b, []byte("   ...("))
		b = append(b, []byte(strconv.Itoa(len(data))))
		b = append(b, []byte(")...   "))
		b = append(b, data[len(data)-limit/2:])
		e = e.Bytes(logkey, bytes.Join(b, []byte("")))
	} else {
		e = e.RawJSON(logkey, data)
	}
	return e
}
