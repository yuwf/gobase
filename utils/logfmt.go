package utils

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"

	"github.com/rs/zerolog"
)

// 打印http相关的内容日志，细节待完善
func LogFmtHttpBody(l *zerolog.Event, logkey string, header http.Header, data []byte, limit int) *zerolog.Event {
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
			l = l.Bytes(logkey, bytes.Join(b, []byte("")))
		} else {
			l = l.Bytes(logkey, data)
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
		l = l.Str(logkey, str)
	} else {
		l = l.Int(logkey+"size", len(data))
		return l
	}
	return l
}

func LogFmtHttpInterface(l *zerolog.Event, logkey string, header http.Header, body interface{}, limit int) *zerolog.Event {
	data, err := json.Marshal(body)
	if err != nil {
		l = l.Interface(logkey, body)
		return l
	}
	if limit > 0 && len(data) > limit {
		b := [][]byte{}
		b = append(b, data[0:limit/2])
		b = append(b, []byte("   ...("))
		b = append(b, []byte(strconv.Itoa(len(data))))
		b = append(b, []byte(")...   "))
		b = append(b, data[len(data)-limit/2:])
		l = l.Bytes(logkey, bytes.Join(b, []byte("")))
	} else {
		l = l.RawJSON(logkey, data)
	}
	return l
}
