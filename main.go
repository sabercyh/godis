package main

import (
	"log"

	"github.com/godis/server"
)

func main() {
	server, err := server.InitGodisServerInstance(6767)
	if err != nil {
		log.Printf("init server error: %v\n", err)
	}
	server.AeLoop.AeMain()
}
