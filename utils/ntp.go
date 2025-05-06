package utils

// https://github.com/yuwf/gobase

import (
	"sort"
	"sync"
	"time"

	"github.com/beevik/ntp"
)

const ntprecordcount = int64(16)

var ntpSortSeq Sequence

type ntpAddress struct {
	Addr string // 地址
	// 最近访问记录
	reqCount int64                         // 访问次数
	reqTime  [ntprecordcount]time.Duration //  最近访问耗时
	avgTime  time.Duration                 // 最近评价耗时
	avgCalc  bool                          // 是否需要计算avgTime
}

var ntpLock sync.RWMutex

// ntpServers 用于ntp同步时间的服务器域名列表
var ntpServers = []*ntpAddress{
	{"time.windows.com", 0, [ntprecordcount]time.Duration{}, 0, false},
	{"time.tencent.com", 0, [ntprecordcount]time.Duration{}, 0, false},
	{"ntp.tencent.com", 0, [ntprecordcount]time.Duration{}, 0, false},
	{"0.centos.pool.ntp.org", 0, [ntprecordcount]time.Duration{}, 0, false},
	{"0.rhel.pool.ntp.org", 0, [ntprecordcount]time.Duration{}, 0, false},
	{"0.debian.pool.ntp.org", 0, [ntprecordcount]time.Duration{}, 0, false},
	{"time.izatcloud.net", 0, [ntprecordcount]time.Duration{}, 0, false},
	{"time.gpsonextra.net", 0, [ntprecordcount]time.Duration{}, 0, false},
	{"0.beevik-ntp.pool.ntp.org", 0, [ntprecordcount]time.Duration{}, 0, false},
	{"0.cn.pool.ntp.org", 0, [ntprecordcount]time.Duration{}, 0, false},
	{"ntp.aliyun.com", 0, [ntprecordcount]time.Duration{}, 0, false},
	{"time.apple.com", 0, [ntprecordcount]time.Duration{}, 0, false},
	{"pool.ntp.org", 0, [ntprecordcount]time.Duration{}, 0, false},
	{"0.pool.ntp.org", 0, [ntprecordcount]time.Duration{}, 0, false},
	{"asia.pool.ntp.org", 0, [ntprecordcount]time.Duration{}, 0, false},
	{"cn.pool.ntp.org", 0, [ntprecordcount]time.Duration{}, 0, false},
	{"ntp0.ntp-servers.net", 0, [ntprecordcount]time.Duration{}, 0, false},
	{"time.pool.aliyun.com", 0, [ntprecordcount]time.Duration{}, 0, false},
	{"time.cloud.tencent.com", 0, [ntprecordcount]time.Duration{}, 0, false},
	{"time.asia.apple.com", 0, [ntprecordcount]time.Duration{}, 0, false},
	{"time.euro.apple.com", 0, [ntprecordcount]time.Duration{}, 0, false},
	{"time.google.com", 0, [ntprecordcount]time.Duration{}, 0, false},
	{"time.facebook.com", 0, [ntprecordcount]time.Duration{}, 0, false},
	{"time.cloudflare.com", 0, [ntprecordcount]time.Duration{}, 0, false},
	{"clock.sjc.he.net", 0, [ntprecordcount]time.Duration{}, 0, false},
	{"clock.fmt.he.net", 0, [ntprecordcount]time.Duration{}, 0, false},
	{"clock.nyc.he.net", 0, [ntprecordcount]time.Duration{}, 0, false},
	{"ntp.ix.ru", 0, [ntprecordcount]time.Duration{}, 0, false},
	{"ntp.jst.mfeed.ad.jp", 0, [ntprecordcount]time.Duration{}, 0, false},
	{"ntp.ntsc.ac.cn", 0, [ntprecordcount]time.Duration{}, 0, false},
	{"ntp1.nim.ac.cn", 0, [ntprecordcount]time.Duration{}, 0, false},
	{"stdtime.gov.hk", 0, [ntprecordcount]time.Duration{}, 0, false},
	{"time.hko.hk", 0, [ntprecordcount]time.Duration{}, 0, false},
	{"time.smg.gov.mo", 0, [ntprecordcount]time.Duration{}, 0, false},
	{"tick.stdtime.gov.tw", 0, [ntprecordcount]time.Duration{}, 0, false},
	{"time.kriss.re.kr", 0, [ntprecordcount]time.Duration{}, 0, false},
	{"ntp.nict.jp", 0, [ntprecordcount]time.Duration{}, 0, false},
	{"s2csntp.miz.nao.ac.jp", 0, [ntprecordcount]time.Duration{}, 0, false},
	{"time.nist.gov", 0, [ntprecordcount]time.Duration{}, 0, false},
	{"time.nplindia.org", 0, [ntprecordcount]time.Duration{}, 0, false},
	{"ntp.pagasa.dost.gov.ph", 0, [ntprecordcount]time.Duration{}, 0, false},
	{"ntp1.sirim.my", 0, [ntprecordcount]time.Duration{}, 0, false},
	{"ntp1.vniiftri.ru", 0, [ntprecordcount]time.Duration{}, 0, false},
	{"vniiftri.khv.ru", 0, [ntprecordcount]time.Duration{}, 0, false},
	{"master.ntp.mn", 0, [ntprecordcount]time.Duration{}, 0, false},
	{"slave.ntp.mn", 0, [ntprecordcount]time.Duration{}, 0, false},
	{"ntp.mn", 0, [ntprecordcount]time.Duration{}, 0, false},
	{"ntp.atlas.ripe.net", 0, [ntprecordcount]time.Duration{}, 0, false},
	{"time.ustc.edu.cn", 0, [ntprecordcount]time.Duration{}, 0, false},
	{"ntp.sjtu.edu.cn", 0, [ntprecordcount]time.Duration{}, 0, false},
	{"ntp.neu.edu.cn", 0, [ntprecordcount]time.Duration{}, 0, false},
	{"time.jmu.edu.cn", 0, [ntprecordcount]time.Duration{}, 0, false},
	{"ntp.neusoft.edu.cn", 0, [ntprecordcount]time.Duration{}, 0, false},
	{"time.edu.cn", 0, [ntprecordcount]time.Duration{}, 0, false},
	{"ntp.nc.u-tokyo.ac.jp", 0, [ntprecordcount]time.Duration{}, 0, false},
	{"ntp.kuins.kyoto-u.ac.jp", 0, [ntprecordcount]time.Duration{}, 0, false},
	{"ntp.tohoku.ac.jp", 0, [ntprecordcount]time.Duration{}, 0, false},
}

func NtpTime() (t time.Time) {
	ntpLock.RLock()
	defer ntpLock.RUnlock()
	for i := 0; i < len(ntpServers); i++ {
		entry := time.Now()
		ok := false
		r, err := ntp.QueryWithOptions(ntpServers[i].Addr, ntp.QueryOptions{Timeout: time.Millisecond * 500})
		if err == nil {
			err = r.Validate()
			if err == nil {
				t = time.Now().Add(r.ClockOffset)
				ok = true
			}
		}

		// 记录耗时
		at := ntpServers[i].reqCount % ntprecordcount // 放上面
		ntpServers[i].reqCount++
		ntpServers[i].reqTime[at] = time.Since(entry) // takeTime不需要锁保护
		ntpServers[i].avgCalc = true

		if !ok {
			continue
		}

		ntpSort(i)
		return
	}
	t = time.Now()
	return
}

func ntpSort(geti int) {
	ntpSortSeq.Submit(func() {
		ntpLock.Lock() // 写锁排序
		defer ntpLock.Unlock()

		// 如是第一个获取的时间 且第一个的平均数仍然小于第二个，不需要排序
		ntpAvgCalc(0)
		ntpAvgCalc(1)
		if geti == 0 && ntpServers[0].avgTime <= ntpServers[1].avgTime {
			return
		}

		sort.SliceStable(ntpServers, func(i, j int) bool {
			ntpAvgCalc(i)
			ntpAvgCalc(j)
			return ntpServers[i].avgTime < ntpServers[j].avgTime
		})
	})
}

func ntpAvgCalc(i int) {
	if ntpServers[i].avgCalc {
		ntpServers[i].avgCalc = false
		var totalTime time.Duration
		count := ntprecordcount
		if ntpServers[i].reqCount < count {
			count = ntpServers[i].reqCount
		}
		for k := int64(0); k < count; k++ {
			totalTime += ntpServers[i].reqTime[k]
		}
		ntpServers[i].avgTime = totalTime / time.Duration(count)
	}
}

// 定时获取一次，interval单位秒 f返回false不再获取
func NtpTime2(interval int, f func(t time.Time) bool) {
	go func() {
		for {
			t := NtpTime()
			if f(t) {
				time.Sleep(time.Second * time.Duration(interval))
			} else {
				break
			}
		}
	}()
}
