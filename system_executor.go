package main

import (
	"common"
	"fmt"
	"sync"

	//"bufio"
	//"common"
	logger "github.com/Sirupsen/logrus"
	//"io"
	//"os"
	"path"
	"strconv"
	//"time"

	//"syscall"
	//"os"
	//"os/exec"
)

const (
	MASTER_ADDR = "127.0.0.1:40000"
	DATANODE_BASE = "127.0.0.1:"
	BASE_PORT = 40001
	USER_DIRECTORY = "/home/wessup"

)

func Executor(nodeNum int) {
	logger.SetLevel(logger.DebugLevel)
	//ch := make(chan bool, 1)
	ports := make([]string, 0)
	for i := 0; i < nodeNum; i++ {
		ports = append(ports, strconv.Itoa(BASE_PORT + i))
	}

	go func(){
		fmt.Println(common.GetGoroutineID())
		runMaster(MASTER_ADDR, path.Join(USER_DIRECTORY, "master_dir"))
	}()
	wg := new(sync.WaitGroup)
	fp := func(port string){
		wg.Add(1)
		address := DATANODE_BASE+port
		n, err := strconv.Atoi(port)
		if err != nil {
			panic(err)
		}
		testd := "testd" + strconv.Itoa(n - 40000)
		directory := path.Join(USER_DIRECTORY, testd)
		runDataNode(common.NodeAddress(address), MASTER_ADDR, directory, common.HOT)
	}
	for _, port := range ports {
		fmt.Println(common.GetGoroutineID())
		go fp(port)
	}
	wg.Wait()
}