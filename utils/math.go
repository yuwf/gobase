package utils

// https://github.com/yuwf/gobase

// 下面几种约束 参考https://cs.opensource.google/go/x/exp
type Signed interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64
}
type Unsigned interface {
	~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~uintptr
}
type Integer interface {
	Signed | Unsigned
}
type Float interface {
	~float32 | ~float64
}
type Complex interface {
	~complex64 | ~complex128
}
type Ordered interface {
	Integer | Float | ~string
}

// 泛型参考库 参考https://github.com/samber/lo/

func IndexOf[T comparable](collection []T, element T) int {
	for i, item := range collection {
		if item == element {
			return i
		}
	}

	return -1
}

func LastIndexOf[T comparable](collection []T, element T) int {
	length := len(collection)

	for i := length - 1; i >= 0; i-- {
		if collection[i] == element {
			return i
		}
	}

	return -1
}

func Contains[T comparable](collection []T, value T) bool {
	for _, v := range collection {
		if v == value {
			return true
		}
	}
	return false
}

func Clamp[T Ordered](value T, min T, max T) T {
	if value < min {
		return min
	} else if value > max {
		return max
	}
	return value
}

func Sum[T Float | Integer | Complex](collection []T) T {
	var sum T = 0
	for _, val := range collection {
		sum += val
	}
	return sum
}

func Min[T Ordered](collection []T) T {
	var min T
	if len(collection) == 0 {
		return min
	}
	min = collection[0]
	for i := 1; i < len(collection); i++ {
		item := collection[i]

		if item < min {
			min = item
		}
	}
	return min
}

func Reverse[T any](collection []T) []T {
	length := len(collection)
	half := length / 2

	for i := 0; i < half; i = i + 1 {
		j := length - 1 - i
		collection[i], collection[j] = collection[j], collection[i]
	}

	return collection
}

func Count[T comparable](collection []T, value T) (count int) {
	for _, item := range collection {
		if item == value {
			count++
		}
	}

	return count
}

func CountValues[T comparable](collection []T) map[T]int {
	result := make(map[T]int)

	for _, item := range collection {
		result[item]++
	}

	return result
}

func Slice[T any](collection []T, start int, end int) []T {
	size := len(collection)

	if start >= end {
		return []T{}
	}

	if start > size {
		start = size
	}
	if start < 0 {
		start = 0
	}

	if end > size {
		end = size
	}
	if end < 0 {
		end = 0
	}

	return collection[start:end]
}

func Replace[T comparable](collection []T, old T, new T, n int) []T {
	result := make([]T, len(collection))
	copy(result, collection)

	for i := range result {
		if result[i] == old && n != 0 {
			result[i] = new
			n--
		}
	}

	return result
}

func ReplaceAll[T comparable](collection []T, old T, new T) []T {
	return Replace(collection, old, new, -1)
}

func Compact[T comparable](collection []T) []T {
	var zero T

	result := make([]T, 0, len(collection))

	for _, item := range collection {
		if item != zero {
			result = append(result, item)
		}
	}

	return result
}

func IsSorted[T Ordered](collection []T) bool {
	for i := 1; i < len(collection); i++ {
		if collection[i-1] > collection[i] {
			return false
		}
	}

	return true
}
