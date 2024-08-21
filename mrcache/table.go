package mrcache

// https://github.com/yuwf/gobase

import (
	"fmt"
	"reflect"
)

// 表结构数据
type TableStruct struct {
	T          reflect.Type
	TS         reflect.Type  // []*T类型 用来快速创建数据
	Tags       []interface{} // string类型，为了方便外部使用
	ElemtsType []reflect.Type
}

func (t *TableStruct) FindIndexByTag(tag interface{}) int {
	for i, f := range t.Tags {
		if f == tag {
			return i
		}
	}
	return -1
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
