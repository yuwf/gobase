package alert

// https://github.com/yuwf/gobase

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"sync"
	"time"

	"gobase/httprequest"

	"github.com/rs/zerolog/log"
)

// 使用飞书报警 飞书的机器人webhook
var (
	feishuConf feishuConfig
)

type feishuConfig struct {
	sync.RWMutex
	// 配置层设置 SetAlertAddr
	alertAddr  string // 报警地址
	secret     string // 秘钥
	serverName string // 本服务器名
}

// 报警地址由配置层来填充
func SetFeiShuAddr(addr, secret, serverName string) {
	feishuConf.Lock()
	defer feishuConf.Unlock()
	feishuConf.alertAddr = addr
	feishuConf.secret = secret
	feishuConf.serverName = serverName
}

func SendFeiShuAlert(format string, a ...interface{}) {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				buf := make([]byte, 2048)
				l := runtime.Stack(buf, false)
				err := fmt.Errorf("%v: %s", r, buf[:l])
				log.Error().Err(err).Msg("Panic")
			}
		}()

		var addr string
		feishuConf.RLock()
		defer feishuConf.RUnlock()
		addr = feishuConf.alertAddr
		if len(addr) == 0 {

			return
		}
		hostname, _ := os.Hostname()
		title := fmt.Sprintf("%s %s", hostname, feishuConf.serverName)
		body := genFeiShuCardData(title, feishuConf.secret, format, a...)

		type response struct {
			Code int    `json:"code"`
			Msg  string `json:"msg"`
		}
		ctx := context.WithValue(context.TODO(), httprequest.CtxKey_nolog, 1)
		httprequest.JsonRequest[response](ctx, "POST", feishuConf.alertAddr, body, nil)
	}()
}

// 不开启协程 等待http送达返回后 函数结束
func SendFeiShuAlert2(format string, a ...interface{}) {
	var addr string
	feishuConf.RLock()
	defer feishuConf.RUnlock()
	addr = feishuConf.alertAddr
	if len(addr) == 0 {
		return
	}
	hostname, _ := os.Hostname()
	title := fmt.Sprintf("%s %s", hostname, feishuConf.serverName)
	body := genFeiShuCardData(title, feishuConf.secret, format, a...)

	type response struct {
		Code int    `json:"code"`
		Msg  string `json:"msg"`
	}
	ctx := context.WithValue(context.TODO(), httprequest.CtxKey_nolog, 1)
	httprequest.JsonRequest[response](ctx, "POST", addr, body, nil)
}

func genFeiShuData(secret string, format string, a ...interface{}) interface{} {
	type altert_content struct {
		Text string `json:"text"`
	}
	type altert_msg struct {
		Timestamp string         `json:"timestamp"`
		Sign      string         `json:"sign"`
		Msg_type  string         `json:"msg_type"`
		Content   altert_content `json:"content"`
	}

	data := &altert_msg{}
	now := time.Now()
	data.Timestamp = strconv.FormatInt(now.Unix(), 10)
	if len(secret) != 0 {
		data.Sign, _ = genFeiShuSign(secret, now.Unix())
	}
	data.Msg_type = "text"
	data.Content.Text = fmt.Sprintf(format, a...)

	return data
}

func genFeiShuCardData(title, secret string, format string, a ...interface{}) interface{} {
	type altert_config struct {
		WideScreenMode bool `json:"wide_screen_mode"`
	}
	type altert_element_text struct {
		Tag     string `json:"tag"`
		Content string `json:"content"`
	}
	type altert_element struct {
		Tag  string              `json:"tag"`
		Text altert_element_text `json:"text"`
	}
	type altert_header_title struct {
		Content string `json:"content"`
		Tag     string `json:"tag"`
	}
	type altert_header struct {
		Template string              `json:"template"`
		Title    altert_header_title `json:"title"`
	}
	type altert_card struct {
		Config   altert_config    `json:"config"`
		Header   altert_header    `json:"header"`
		Elements []altert_element `json:"elements"`
	}
	type altert_msg struct {
		Timestamp string      `json:"timestamp"`
		Sign      string      `json:"sign"`
		Msg_type  string      `json:"msg_type"`
		Card      altert_card `json:"card"`
	}

	data := &altert_msg{}
	now := time.Now()
	data.Timestamp = strconv.FormatInt(now.Unix(), 10)
	if len(secret) != 0 {
		data.Sign, _ = genFeiShuSign(secret, now.Unix())
	}
	data.Msg_type = "interactive"
	data.Card.Config.WideScreenMode = true

	data.Card.Header.Template = "yellow"
	data.Card.Header.Title.Content = title
	data.Card.Header.Title.Tag = "plain_text"

	data.Card.Elements = make([]altert_element, 0)
	var element altert_element
	element.Tag = "div"
	element.Text.Tag = "plain_text"
	element.Text.Content = fmt.Sprintf(format, a...)
	element.Text.Content = element.Text.Content + "\n\n" + time.Now().String()
	data.Card.Elements = append(data.Card.Elements, element)

	return data
}

func genFeiShuSign(secret string, timestamp int64) (string, error) {
	//timestamp + key 做sha256, 再进行base64 encode
	stringToSign := fmt.Sprintf("%v", timestamp) + "\n" + secret

	var data []byte
	h := hmac.New(sha256.New, []byte(stringToSign))
	_, err := h.Write(data)
	if err != nil {
		return "", err
	}

	signature := base64.StdEncoding.EncodeToString(h.Sum(nil))
	return signature, nil
}
