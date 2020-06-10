package main

import (
	"common"
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
	ch := make(chan bool, 1)
	//mChannel := make(chan string, 100)
	//dnChannel := make(chan string ,100)
	//commands := []string{
	//	"-c",
	//	"go",
	//	"run",
	//	"daemon.go",
	//	"master",
	//	MASTER_ADDR,
	//	path.Join(USER_DIRECTORY, "master_dir"),
	//}
	//logger.Infof("%v", commands)
	ports := make([]string, 0)
	for i := 0; i < nodeNum; i++ {
		ports = append(ports, strconv.Itoa(BASE_PORT + i))
	}

	go func(){
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
		go fp(port)
	}
	wg.Wait()

	//writer.Close()
	//writer.Close()
	//os.Stdout = old

	//masterProcess := exec.Command("go", commands...)
	//masterProcess := exec.Command("bash", commands...)
	//masterPipe, err := masterProcess.Output()
	//if err != nil {
	//	panic(err)
	//}
	//masterOut, err := masterProcess.StdoutPipe()
	//
	//if err != nil {
	//	panic(err)
	//}
	//rErr := masterProcess.Start()
	//if rErr != nil {
	//	panic(rErr)
	//}
	//defer masterProcess.Wait()
	//Reader := bufio.NewReader(masterOut)
	//Scanner := bufio.NewScanner(masterOut)
	//go func(){
	//	for Scanner.Scan(){
	//		logger.Infof(Scanner.Text())
	//		time.Sleep(1*time.Second)
	//	}
	//}()
	//masterProcess.Wait()
	//for {
		//line, err := Reader.ReadString('\n')
		//if err != nil {
		//	logger.Errorf("%v", err.Error())
		//	time.Sleep(1*time.Second)
		//	continue
		//}
		//logger.Infof("%s", line)
	//}
	//go io.Copy(os.Stdout, masterOut)

	//for {
	//	var o string
	//	select {
	//		case o = <- mChannel:
	//			logger.Infof("Master: %s", o)
	//		case o = <- dnChannel:
	//			logger.Infof("DataNode: %s", o)
	//	}
	//}
	//for _, port := range ports {
	//	DATANODE_BASE + port
	//}
	<- ch

}