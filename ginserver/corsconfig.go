package ginserver

// https://github.com/yuwf/gobase

import (
	"net/http"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
)

// 跨域控制配置
// 参考 https://github.com/rs/cors/
type CorsConfig struct {
	AllowOrigin      []string `json:"alloworigin,omitempty"`      // Access-Control-Allow-Origin:
	AllowMethods     []string `json:"allowmethods,omitempty"`     // Access-Control-Allow-Methods
	AllowHeaders     []string `json:"allowheaders,omitempty"`     // Access-Control-Allow-Headers
	MaxAge           int      `json:"maxage,omitempty"`           // Access-Control-Max-Age 秒
	AllowCredentials bool     `json:"allowcredentials,omitempty"` // Access-Control-Allow-Credentials
}

var defaultCorsOptions = &CorsConfig{
	AllowOrigin: []string{"*"},
	AllowMethods: []string{
		http.MethodHead,
		http.MethodGet,
		http.MethodPost,
		http.MethodPut,
		http.MethodPatch,
		http.MethodDelete,
	},
	AllowHeaders:     []string{"*"},
	MaxAge:           86400,
	AllowCredentials: false,
}

func (c *CorsConfig) Normalize() {
	c.AllowHeaders = c.convert(c.AllowHeaders, http.CanonicalHeaderKey)
	c.AllowMethods = c.convert(c.AllowMethods, strings.ToUpper)
}

func (c *CorsConfig) convert(s []string, o func(string) string) []string {
	out := []string{}
	for _, i := range s {
		out = append(out, o(i))
	}
	return out
}

func (c *CorsConfig) isMethodAllowed(method string) bool {
	if len(c.AllowMethods) == 0 {
		return false
	}
	method = strings.ToUpper(method)
	if method == http.MethodOptions {
		return true
	}
	for _, m := range c.AllowMethods {
		if m == method {
			return true
		}
	}
	return false
}

func cors(c *gin.Context) {
	cors := ParamConf.Get().Cors

	// 要求客户端验证源
	//c.Writer.Header().Add("Vary", "Origin")

	if !cors.isMethodAllowed(c.Request.Method) {
		c.AbortWithStatus(http.StatusMethodNotAllowed)
		return
	}

	// 每次请求都要加进来
	if len(cors.AllowMethods) > 0 {
		c.Header("Access-Control-Allow-Origin", strings.Join(cors.AllowOrigin, ","))
	} else {
		c.Header("Access-Control-Allow-Origin", "*")
	}

	// 预检请求
	if c.Request.Method == http.MethodOptions {
		// 要求客户端验证请求方法和头
		//c.Writer.Header().Add("Vary", "Access-Control-Request-Method")
		//c.Writer.Header().Add("Vary", "Access-Control-Request-Headers")

		if len(cors.AllowMethods) > 0 {
			c.Header("Access-Control-Allow-Methods", strings.Join(cors.AllowMethods, ","))
		}
		if len(cors.AllowHeaders) > 0 {
			c.Header("Access-Control-Allow-Headers", strings.Join(cors.AllowHeaders, ","))
		} else {
			reqHeader := c.Request.Header.Get("Access-Control-Request-Headers")
			if len(reqHeader) > 0 {
				c.Header("Access-Control-Allow-Headers", reqHeader)
			} else {
				c.Header("Access-Control-Allow-Headers", "*")
			}
		}
		if cors.MaxAge > 0 {
			c.Header("Access-Control-Max-Age", strconv.Itoa(cors.MaxAge))
		}
		if cors.AllowCredentials {
			c.Header("Access-Control-Allow-Credentials", "true")
		}

		c.AbortWithStatus(http.StatusNoContent)
		return
	}
}
