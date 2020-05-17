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

func runMaster() {
	if len(os.Args) < 4 {
		fmt.Println("Not Enough Arguments for Master")
		return
	}
	address := common.NodeAddress(os.Args[2])
	rootDir := os.Args[3]

	master.Run(address, rootDir)

	ch := make(chan bool, 1)
	<- ch
}

func runDataNode() {
	if len(os.Args) < 6 {
		fmt.Println("Not Enough Arguments for DataNode")
		return
	}
	address := common.NodeAddress(os.Args[2])
	rootDir := os.Args[3]
	masterAddress := common.NodeAddress(os.Args[4])
	var nodeType common.NodeType
	if os.Args[5] == "hot" {
		nodeType = common.HOT
	} else if os.Args[5] == "cold" {
		nodeType = common.COLD
	} else {
		fmt.Println("Node Type Must Be cold or hot")
		return
	}
	datanode.Run(rootDir, address, masterAddress, nodeType)

	ch := make(chan bool, 1)
	<- ch
}

func main() {
	logger.SetLevel(logger.DebugLevel)
	if len(os.Args) < 2 {
		return
	}
	if os.Args[1] == "master" {
		runMaster()
	} else if os.Args[1] == "datanode" {
		runDataNode()
	} else {
		fmt.Println("Invalid Arguments")
	}
}
