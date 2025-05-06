package mrcache

// https://github.com/yuwf/gobase

import (
	"fmt"
	"io"
	"strings"
)

// 查询条件
type TableCond struct {
	field   string      // 字段
	opvalue string      // len>0 合并了op和value
	op      string      // 条件和值的连接符
	value   interface{} // 值
	link    string      // 和下个条件的连接值 不填充默认为AND
}

type TableConds []*TableCond

func NewConds() TableConds {
	return TableConds{}
}

func (cond TableConds) Find(field string) *TableCond {
	for _, v := range cond {
		if v.field == field {
			return v
		}
	}
	return nil
}

// 日志输出时调用
func (m TableConds) Log() string {
	var str strings.Builder
	for i, t := range m {
		if i > 0 {
			if len(t.link) > 0 {
				str.WriteString(" " + t.link + " ")
			} else {
				str.WriteString(" AND ")
			}
		}
		str.WriteString(t.field)
		str.WriteString(t.op)
		str.WriteString(fmt.Sprintf("%v", t.value))
	}
	return str.String()
}

func (cond TableConds) Eq(field string, value interface{}) TableConds {
	return append(cond, &TableCond{field: field, op: "=", value: value})
}

// 内部方便使用，调用层保证condFields和condValues大小一样
func (cond TableConds) eqs(condFields []string, condValues []interface{}) TableConds {
	if len(condFields) == len(condValues) {
		for i, tag := range condFields {
			cond = cond.Eq(tag, condValues[i])
		}
	}
	return cond
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

func (cond TableConds) In(field string, values ...interface{}) TableConds {
	var strArgs []string
	for _, v := range values {
		strArgs = append(strArgs, fmt.Sprintf("'%v'", v)) // 将元素转为字符串
	}
	return append(cond, &TableCond{field: field, opvalue: "IN (" + strings.Join(strArgs, ",") + ")", op: "IN", value: values})
}

func (cond TableConds) NoIn(field string, values []interface{}) TableConds {
	var strArgs []string
	for _, v := range values {
		strArgs = append(strArgs, fmt.Sprintf("'%v'", v)) // 将元素转为字符串
	}
	return append(cond, &TableCond{field: field, opvalue: "NOT IN(" + strings.Join(strArgs, ",") + ")", op: "NOT IN", value: values})
}

func (cond TableConds) Ins(fields []string, valuess ...[]interface{}) TableConds {
	var strArgs []string
	for _, values := range valuess {
		var strArgs2 []string
		for _, v := range values {
			strArgs2 = append(strArgs2, fmt.Sprintf("'%v'", v)) // 将元素转为字符串
		}
		strArgs = append(strArgs, "("+strings.Join(strArgs2, ",")+")")
	}
	return append(cond, &TableCond{field: "(" + strings.Join(fields, ",") + ")", opvalue: "IN (" + strings.Join(strArgs, ",") + ")", op: "IN", value: valuess})
}

func (cond TableConds) Or() TableConds {
	if len(cond) > 0 {
		cond[len(cond)-1].link = "OR"
	}
	return cond
}

func (cond TableConds) fmtCond(sqlStr io.StringWriter) []interface{} {
	if len(cond) == 0 {
		return make([]interface{}, 0)
	}
	args := make([]interface{}, 0, len(cond))
	for i, v := range cond {
		if i > 0 {
			if len(cond[i-1].link) > 0 {
				sqlStr.WriteString(" " + cond[i-1].link + " ")
			} else {
				sqlStr.WriteString(" AND ")
			}
		}
		sqlStr.WriteString(v.field)
		if len(v.opvalue) == 0 {
			sqlStr.WriteString(v.op + "?")
			args = append(args, v.value)
		} else {
			sqlStr.WriteString(" " + v.opvalue)
		}
	}
	return args
}
