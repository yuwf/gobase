package utils

// https://github.com/yuwf

import (
	"math/rand"
	"time"

	"github.com/rs/zerolog/log"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

// 标准库内的函数 过渡下，加下判断，防止panic

//RandInt int随机数
func RandInt() int {
	return rand.Int()
}

//RandIntn [0,n)的随机数
func RandIntn(n int) int {
	if n <= 0 {
		caller := GetCallerDesc(1)
		log.Error().Int("n", n).Str("pos", caller.Pos()).Msg("RandParamErr")
		return 0
	}
	return rand.Intn(n)
}

//RandInt31 int32随机数
func RandInt31() int32 {
	return rand.Int31()
}

//RandInt31n [0,n)的随机数
func RandInt31n(n int32) int32 {
	if n <= 0 {
		caller := GetCallerDesc(1)
		log.Error().Int32("n", n).Str("pos", caller.Pos()).Msg("RandParamErr")
		return 0
	}
	return rand.Int31n(n)
}

//RandUint32 uint32
func RandUint32() uint32 {
	return rand.Uint32()
}

//RandInt63 int64随机数
func RandInt63() int64 {
	return rand.Int63()
}

//RandInt63n [0,n)的随机数
func RandInt63n(n int64) int64 {
	if n <= 0 {
		caller := GetCallerDesc(1)
		log.Error().Int64("n", n).Str("pos", caller.Pos()).Msg("RandParamErr")
		return 0
	}
	return rand.Int63n(n)
}

//RandUint64 uint64
func RandUint64() uint64 {
	return rand.Uint64()
}

//RandFloat32 float32[0,1.0)
func RandFloat32() float32 {
	return rand.Float32()
}

//RandFloat64 float64[0,1.0)
func RandFloat64() float64 {
	return rand.Float64()
}

// 自定义的函数

//RandIntnm [n,m)的随机数
func RandIntnm(n, m int) int {
	if !(n >= 0 && m > n) {
		caller := GetCallerDesc(1)
		log.Error().Int("n", n).Int("m", m).Str("pos", caller.Pos()).Msg("RandParamErr")
		return 0
	}
	return rand.Int()%(m-n) + n
}

//RandIntnm2 [n,m]的随机数
func RandIntnm2(n, m int) int {
	if !(n >= 0 && m >= n) {
		caller := GetCallerDesc(1)
		log.Error().Int("n", n).Int("m", m).Str("pos", caller.Pos()).Msg("RandParamErr")
		return 0
	}
	return rand.Int()%(m-n+1) + n
}

//RandInt31nm [n,m)的随机数
func RandInt31nm(n, m int32) int32 {
	if !(n >= 0 && m > n) {
		caller := GetCallerDesc(1)
		log.Error().Int32("n", n).Int32("m", m).Str("pos", caller.Pos()).Msg("RandParamErr")
		return 0
	}
	return rand.Int31()%(m-n) + n
}

//RandInt31nm2 [n,m]的随机数
func RandInt31nm2(n, m int32) int32 {
	if !(n >= 0 && m >= n) {
		caller := GetCallerDesc(1)
		log.Error().Int32("n", n).Int32("m", m).Str("pos", caller.Pos()).Msg("RandParamErr")
		return 0
	}
	return rand.Int31()%(m-n+1) + n
}

//RandInt63nm [n,m)的随机数
func RandInt63nm(n, m int64) int64 {
	if !(n >= 0 && m > n) {
		caller := GetCallerDesc(1)
		log.Error().Int64("n", n).Int64("m", m).Str("pos", caller.Pos()).Msg("RandParamErr")
		return 0
	}
	return rand.Int63()%(m-n) + n
}

//RandInt63nm2 [n,m]的随机数
func RandInt63nm2(n, m int64) int64 {
	if !(n >= 0 && m >= n) {
		caller := GetCallerDesc(1)
		log.Error().Int64("n", n).Int64("m", m).Str("pos", caller.Pos()).Msg("RandParamErr")
		return 0
	}
	return rand.Int63()%(m-n+1) + n
}

func RandShuffle(n int, swap func(i, j int)) {
	if n < 0 {
		caller := GetCallerDesc(1)
		log.Error().Int("n", n).Str("pos", caller.Pos()).Msg("RandParamErr")
		return
	}
	rand.Shuffle(n, swap)
}

func RandShuffleSlice[T any](collection []T) []T {
	rand.Shuffle(len(collection), func(i, j int) {
		collection[i], collection[j] = collection[j], collection[i]
	})
	return collection
}

var (
	LowerCaseLettersCharset = []byte("abcdefghijklmnopqrstuvwxyz")
	UpperCaseLettersCharset = []byte("ABCDEFGHIJKLMNOPQRSTUVWXYZ")
	LettersCharset          = append(LowerCaseLettersCharset, UpperCaseLettersCharset...)
	NumbersCharset          = []byte("0123456789")
	AlphanumericCharset     = append(LettersCharset, NumbersCharset...)
	SpecialCharset          = []byte("!@#$%^&*()_+-=[]{}|;':\",./<>?")
	AllCharset              = append(AlphanumericCharset, SpecialCharset...)
)

//生成随机字符串
func RandString(size int) string {
	var charactersCount = len(AlphanumericCharset)
	token := []byte{}
	for i := 0; i < size; i++ {
		index := RandIntn(charactersCount)
		token = append(token, AlphanumericCharset[index])
	}
	return string(token)
}

func RandString2(size int, charset []byte) string {
	if size <= 0 {
		caller := GetCallerDesc(1)
		log.Error().Int("size", size).Str("pos", caller.Pos()).Msg("RandString2")
		return ""
	}
	if len(charset) <= 0 {
		caller := GetCallerDesc(1)
		log.Error().Int("charset", len(charset)).Str("pos", caller.Pos()).Msg("RandString2")
		return ""
	}
	b := make([]byte, size)
	charactersCount := len(charset)
	for i := range b {
		b[i] = charset[RandIntn(charactersCount)]
	}
	return string(b)
}
