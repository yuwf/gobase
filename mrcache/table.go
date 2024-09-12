package mrcache

// https://github.com/yuwf/gobase

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
)

// 表结构数据
type TableStruct struct {
	T          reflect.Type
	TS         reflect.Type  // []*T类型 用来快速创建数据
	MySQLTags  []interface{} // MySQL字段名 string类型，为了方便外部使用interface，Set或者Modify时外部的子结构中的tag以此为准
	RedisTags  []interface{} // 存储Reids使用的tag 大小和Tags一致 在组织Redis参数时使用
	ElemtsType []reflect.Type
}

// 根据tag获取结构信息
func GetTableStruct[T any]() *TableStruct {
	dest := new(T)
	vo := reflect.ValueOf(dest)
	structtype := vo.Elem().Type() // 第一层是指针，第二层是结构
	structvalue := vo.Elem()
	if structtype.Kind() != reflect.Struct {
		err := errors.New("not struct")
		panic(err) // 直接panic
	}

	numField := structtype.NumField()
	mysqlTags := make([]interface{}, 0, numField)
	redisTags := make([]interface{}, 0, numField)
	elemtsType := make([]reflect.Type, 0, numField)
	for i := 0; i < numField; i += 1 {
		v := structvalue.Field(i)
		dbTag := structtype.Field(i).Tag.Get(DBTag)
		if dbTag == "-" || dbTag == "" {
			continue
		}
		sAt := strings.IndexByte(dbTag, ',')
		if sAt != -1 {
			dbTag = dbTag[0:sAt]
		}
		redisTag := structtype.Field(i).Tag.Get(RedisTag)
		if redisTag != "-" && redisTag != "" {
			sAt = strings.IndexByte(redisTag, ',')
			if sAt != -1 {
				redisTag = redisTag[0:sAt]
			}
		} else {
			redisTag = dbTag
		}
		mysqlTags = append(mysqlTags, dbTag)
		redisTags = append(redisTags, redisTag)
		elemtsType = append(elemtsType, v.Type())
	}

	table := &TableStruct{
		T:          structtype,
		TS:         reflect.ValueOf([]*T{}).Type(),
		MySQLTags:  mysqlTags,
		RedisTags:  redisTags,
		ElemtsType: elemtsType,
	}
	return table
}

func (t *TableStruct) FindIndexByTag(tag interface{}) int {
	for i, f := range t.MySQLTags {
		if f == tag {
			return i
		}
	}
	return -1
}

func (t *TableStruct) GetRedisTagByTag(tag interface{}) interface{} {
	for i, f := range t.MySQLTags {
		if f == tag {
			return t.RedisTags[i]
		}
	}
	return ""
}

// 查询条件
type TableCond struct {
	field string      // 字段
	op    string      // 条件和值的连接符
	value interface{} // 值
	link  string      // 和下个条件的连接值 不填充默认为AND
}

type TableConds []*TableCond

func (cond TableConds) Find(field string) *TableCond {
	for _, v := range cond {
		if v.field == field {
			return v
		}
	}
	return nil
}

func NewConds() TableConds {
	return TableConds{}
}

func (cond TableConds) Eq(field string, value interface{}) TableConds {
	return append(cond, &TableCond{field: field, op: "=", value: value})
}

// 为了减少理解的复杂读，在Get Set Modify的条件中不要使用下面的条件

func (cond TableConds) Ne(field string, value interface{}) TableConds {
	return append(cond, &TableCond{field: field, op: "<>", value: value})
}

func (cond TableConds) Gt(field string, value interface{}) TableConds {
	return append(cond, &TableCond{field: field, op: ">", value: value})
}

func (cond TableConds) Ge(field string, value interface{}) TableConds {
	return append(cond, &TableCond{field: field, op: ">=", value: value})
}

func (cond TableConds) Lt(field string, value interface{}) TableConds {
	return append(cond, &TableCond{field: field, op: "<", value: value})
}

func (cond TableConds) Le(field string, value interface{}) TableConds {
	return append(cond, &TableCond{field: field, op: "<=", value: value})
}

func (cond TableConds) Between(field string, left interface{}, right interface{}) TableConds {
	return append(cond, &TableCond{field: field, op: "BEWEEN", value: fmt.Sprintf("%v and %v", left, right)})
}

func (cond TableConds) NotBetween(field string, left interface{}, right interface{}) TableConds {
	return append(cond, &TableCond{field: field, op: "NOT BEWEEN", value: fmt.Sprintf("%v and %v", left, right)})
}

func (cond TableConds) Like(field string, value interface{}) TableConds {
	return append(cond, &TableCond{field: field, op: "LIKE", value: value})
}

func (cond TableConds) NotLike(field string, value interface{}) TableConds {
	return append(cond, &TableCond{field: field, op: "NOT LIKE", value: value})
}

func (cond TableConds) IsNull(field string) TableConds {
	return append(cond, &TableCond{field: field, op: "IS", value: "NULL"})
}

func (cond TableConds) IsNotNull(field string) TableConds {
	return append(cond, &TableCond{field: field, op: "IN NOT", value: "NULL"})
}

func (cond TableConds) In(field string, args ...interface{}) TableConds {
	return append(cond, &TableCond{field: field, op: "IN", value: "[" + fmt.Sprint(args...) + "]"})
}

func (cond TableConds) NoIn(field string, args ...interface{}) TableConds {
	return append(cond, &TableCond{field: field, op: "NOT IN", value: "[" + fmt.Sprint(args...) + "]"})
}

func (cond TableConds) Or() TableConds {
	if len(cond) > 0 {
		cond[len(cond)-1].link = "OR"
	}
	return cond
}
