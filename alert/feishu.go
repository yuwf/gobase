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
	"time"

	"github.com/yuwf/gobase/httprequest"
	"github.com/yuwf/gobase/utils"

	"github.com/rs/zerolog/log"
)

func SendFeiShuAlert(addr *AlertAddr, format string, a ...interface{}) {
	if addr == nil || len(addr.Addr) == 0 {
		return
	}
	go func() {
		defer func() {
			if r := recover(); r != nil {
				buf := make([]byte, 2048)
				l := runtime.Stack(buf, false)
				err := fmt.Errorf("%v: %s", r, buf[:l])
				log.Error().Err(err).Msg("Panic")
			}
		}()

		hostname, _ := os.Hostname()
		title := fmt.Sprintf("%s %s", hostname, ParamConf.Get().ServerName)
		body := genFeiShuCardData(title, addr.Secret, format, a...)

		type response struct {
			Code int    `json:"code"`
			Msg  string `json:"msg"`
		}
		ctx := utils.CtxCaller(context.TODO(), 1)
		httprequest.JsonRequest[response](ctx, "POST", addr.Addr, body, nil)
	}()
}

// 不开启协程 等待http送达返回后 函数结束
func SendFeiShuAlert2(addr *AlertAddr, format string, a ...interface{}) {
	if addr == nil || len(addr.Addr) == 0 {
		return
	}
	hostname, _ := os.Hostname()
	title := fmt.Sprintf("%s %s", hostname, ParamConf.Get().ServerName)
	body := genFeiShuCardData(title, addr.Secret, format, a...)

	type response struct {
		Code int    `json:"code"`
		Msg  string `json:"msg"`
	}
	ctx := utils.CtxCaller(context.TODO(), 1)
	httprequest.JsonRequest[response](ctx, "POST", addr.Addr, body, nil)
}

/*func genFeiShuData(secret string, format string, a ...interface{}) interface{} {
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
}*/

func genFeiShuCardData(title, secret string, format string, a ...interface{}) interface{} {
	type altert_config struct {
		EnableForward bool `json:"enable_forward"`
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
	data.Card.Config.EnableForward = true

	data.Card.Header.Template = "yellow"
	data.Card.Header.Title.Content = title
	data.Card.Header.Title.Tag = "plain_text"

	data.Card.Elements = make([]altert_element, 0)
	var element altert_element
	element.Tag = "div"
	element.Text.Tag = "plain_text"
	element.Text.Content = fmt.Sprintf(format, a...)
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
