package mysql

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
)

var cfg = &Config{
	//Source: "root:1235@tcp(localhost:3306)/mysql?charset=utf8",
	Addr:     "localhost:3306",
	UserName: "root",
	Passwd:   "1235",
	DB:       "mysql",
	Params:   map[string]string{"charset": "utf8"},
}

type Users struct {
	Host  string `db:"host"`
	User  string `db:"User,cond"`
	Testt string `db:"Testt"`
}

type Name struct {
	User string `db:"User"`
}

func BenchmarkMySQL(b *testing.B) {
	var m map[int]string
	s := `{"1":"1", "2":"22"}`
	fmt.Println(json.Unmarshal([]byte(s), &m))
	mysql, err := InitDefaultMySQL(cfg)
	if err != nil {
		return
	}

	var name Name
	err = mysql.Get(context.TODO(), &name, "SELECT User FROM user WHERE User=? and Host=?", "root", "localhost")
	fmt.Println(name, err)

	var users []Users
	mysql.Select(context.TODO(), &users, "SELECT host, User FROM user")
	fmt.Println(users, err)
}

func BenchmarkMySQL2(b *testing.B) {
	mysql, err := InitDefaultMySQL(cfg)
	if err != nil {
		return
	}

	result, err := mysql.Exec(context.TODO(), "SELECT Host, User FROM user WHERE User=?", "root")
	fmt.Println(result, err)
}
