package main

import (
	"common"
	"datanode"
	"fmt"
	logger "github.com/Sirupsen/logrus"
	"master"
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

func runDaemon(args []string) {
	logger.SetLevel(logger.DebugLevel)

	ch := make(chan bool, 1)
	if len(args) < 2 {
		logger.Info("Run Executor")
		go func(){
			Executor(3) //common.ReplicaNum
		}()
		<- ch
	}

	if args[1] == "master" {
		if len(args) < 4 {
			fmt.Println("Not Enough Arguments for Master")
			return
		}
		address := common.NodeAddress(args[2])
		rootDir := args[3]
		runMaster(address, rootDir)

	} else if args[1] == "datanode" {
		if len(args) < 5 {
			fmt.Println("Not Enough Arguments for DataNode")
			return
		}
		address := common.NodeAddress(args[2])
		masterAddress := common.NodeAddress(args[3])
		rootDir := args[4]
		var nodeType common.NodeType
		nodeType = common.HOT
		//if args[5] == "hot" {
		//	nodeType = common.HOT
		//} else if args[5] == "cold" {
		//	nodeType = common.COLD
		//} else {
		//	fmt.Errorf("INVALID NODE TYPE")
		//	return
		//}
		runDataNode(address, masterAddress, rootDir, nodeType)
	} else {
		fmt.Println("Invalid Arguments")
		return
	}
}
