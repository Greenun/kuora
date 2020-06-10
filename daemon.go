//package kuora
package main

import (
	"common"
	"datanode"
	"fmt"
	logger "github.com/Sirupsen/logrus"
	"master"
	"os"
)

func runMaster(address common.NodeAddress, rootDir string) {
	master.Run(address, rootDir)

	ch := make(chan bool, 1)
	<- ch
}

func runDataNode(address, masterAddress common.NodeAddress, rootDir string, nodeType common.NodeType) {
	datanode.Run(rootDir, address, masterAddress, nodeType)

	ch := make(chan bool, 1)
	<- ch
}

func main() {
	logger.SetLevel(logger.DebugLevel)

	ch := make(chan bool, 1)
	if len(os.Args) < 2 {
		go func(){
			Executor(2)
		}()
		//fmt.Println("Insufficient Arguments")
		//return
		<- ch
	}

	if os.Args[1] == "master" {
		if len(os.Args) < 4 {
			fmt.Println("Not Enough Arguments for Master")
			return
		}
		address := common.NodeAddress(os.Args[2])
		rootDir := os.Args[3]
		runMaster(address, rootDir)

	} else if os.Args[1] == "datanode" {
		if len(os.Args) < 6 {
			fmt.Println("Not Enough Arguments for DataNode")
			return
		}
		address := common.NodeAddress(os.Args[2])
		masterAddress := common.NodeAddress(os.Args[3])
		rootDir := os.Args[4]
		var nodeType common.NodeType
		if os.Args[5] == "hot" {
			nodeType = common.HOT
		} else if os.Args[5] == "cold" {
			nodeType = common.COLD
		} else {
			fmt.Errorf("INVALID NODE TYPE")
			return
		}
		runDataNode(address, masterAddress, rootDir, nodeType)
	} else {
		fmt.Println("Invalid Arguments")
		return
	}
}
