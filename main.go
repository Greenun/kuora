package main

import (
	"os"
	"web"
)

func main() {
	if len(os.Args) < 2 {
		runDaemon(os.Args[:])
	} else if os.Args[1] == "web" {
		web.RunWebServer()
	} else {
		runDaemon(os.Args[:])
	}
}
