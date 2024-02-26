package db

import "github.com/godis/data"

type GodisDB struct {
	Data   *data.Dict
	Expire *data.Dict
}
