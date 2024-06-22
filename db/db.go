package db

import "github.com/godis/data"

type GodisDB struct {
	Data   *data.Dict //存储Godis中的有效数据
	Expire *data.Dict //存储Godis中的过期数据
}
