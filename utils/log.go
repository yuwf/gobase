package utils

// https://github.com/yuwf/gobase

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"

	"github.com/rs/zerolog"
)

// 方便ctx中内容输出使用
func LogCtx(e *zerolog.Event, ctx context.Context) *zerolog.Event {
	if e != nil && ctx != nil {
		if traceId := ctx.Value(CtxKey_traceId); traceId != nil {
			e = e.Interface("TID", traceId)
		}
		if callers, ok := ctx.Value(CtxKey_callers).([]*CallerDesc); ok {
			var strs []string
			for _, caller := range callers {
				strs = append(strs, caller.Loc())
			}
			e = e.Strs("callPos", strs)
		}
	}
	return e
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
