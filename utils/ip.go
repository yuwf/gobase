package utils

// https://github.com/yuwf/gobase

import (
	"net"
	"net/http"
	"strings"
	"sync"

	"github.com/rs/zerolog/log"
)

// LocalIP ...
func LocalIP() (net.IP, error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return nil, err
	}
	// 优先取全局单播地址
	for _, addr := range addrs {
		if ipnet, ok := addr.(*net.IPNet); ok && ipnet.IP.IsGlobalUnicast() {
			if ipnet.IP.To4() != nil || ipnet.IP.To16() != nil {
				return ipnet.IP, nil
			}
		}
	}
	// 取回环地址
	for _, addr := range addrs {
		if ipnet, ok := addr.(*net.IPNet); ok && ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP, nil
			}
		}
	}
	return nil, nil
}

// LocalIPString ...
var getIpOnce sync.Once
var localIp string

func LocalIPString() string {
	getIpOnce.Do(func() {
		ip, err := LocalIP()
		if err != nil {
			log.Error().Msgf("LocalIPString error:%v", err)
			return
		}
		if ip == nil {
			log.Error().Msg("LocalIPString empty")
		}
		localIp = ip.String()
	})
	return localIp
}

var remoteIPHeaders = []string{"True-Client-IP", "X-Forwarded-For", "X-Real-IP"}

func ClientIPHeader(header http.Header) *net.IPAddr {
	for _, name := range remoteIPHeaders {
		if value := header.Get(name); value != "" {
			ipStr := strings.TrimSpace(strings.Split(value, ",")[0])
			if ip, err := net.ResolveIPAddr("ip", ipStr); err == nil {
				return ip
			}
		}
	}
	return nil
}

func ClientTCPIPHeader(header http.Header) *net.TCPAddr {
	for _, name := range remoteIPHeaders {
		if value := header.Get(name); value != "" {
			ipStr := strings.TrimSpace(strings.Split(value, ",")[0])
			if ip, err := net.ResolveTCPAddr("tcp", ipStr); err == nil {
				return ip
			}
		}
	}
	return nil
}

func ClientIPRequest(req *http.Request) *net.IPAddr {
	for _, name := range remoteIPHeaders {
		if value := req.Header.Get(name); value != "" {
			ipStr := strings.TrimSpace(strings.Split(value, ",")[0])
			if ip, err := net.ResolveIPAddr("ip", ipStr); err == nil {
				return ip
			}
		}
	}
	if ipStr, _, err := net.SplitHostPort(strings.TrimSpace(req.RemoteAddr)); err == nil {
		if ip, err := net.ResolveIPAddr("ip", ipStr); err == nil {
			return ip
		}
	}
	return nil
}

func ParseIP(addr net.Addr) string {
	ip := strings.Split(addr.String(), ":")[0]
	return ip
}
