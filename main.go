package main

import (
	"os"
	"web"
)

func main() {
	if len(os.Args) < 2 {
		return
	}
	if os.Args[1] == "web" {
		if os.Args[2] == "client" {
			web.RunClient()
		} else if os.Args[2] == "server" {
			web.RunWebServer()
		}
	} else {
		runDaemon(os.Args[:])
	}
}
