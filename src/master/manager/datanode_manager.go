package manager

import (
	"common"
	"common/ipc"
	"fmt"
	logger "github.com/Sirupsen/logrus"
	"math/rand"
	"sync"
	"time"
)

type DataNodeManager struct {
	sync.RWMutex
	HotNodes map[common.NodeAddress]*Node
	ColdNodes map[common.NodeAddress]*Node
}

func NewDataNodeManager() *DataNodeManager {
	dataNodeManager := &DataNodeManager{
		HotNodes:   make(map[common.NodeAddress]*Node),
		ColdNodes:   make(map[common.NodeAddress]*Node),
	}
	return dataNodeManager
}

func (manager *DataNodeManager) Heartbeat(address common.NodeAddress, nodeType common.NodeType,
	response *ipc.HeartBeatResponse) bool {
	manager.Lock()
	defer manager.Unlock()
	var nodeInfo *Node
	var exist bool
	if nodeType == common.HOT {
		nodeInfo, exist = manager.HotNodes[address]
		if !exist {
			// not exist == first beat
			logger.Infof("New DataNode %s", address)
			manager.HotNodes[address] = &Node{
				Blocks: make(map[common.BlockHandle]bool),
				Garbage: make([]common.BlockHandle, 0),
				LastBeat: time.Now(),
			}
			return true
		} else {
			// copy garbage info
			response.Garbage = nodeInfo.Garbage
			manager.HotNodes[address].Garbage = make([]common.BlockHandle, 0)
			manager.HotNodes[address].LastBeat = time.Now()
			return false
		}
	} else if nodeType == common.COLD {
		nodeInfo, exist = manager.ColdNodes[address]
		if !exist {
			// not exist == first beat
			logger.Infof("New DataNode %s", address)
			manager.ColdNodes[address] = &Node{
				Blocks: make(map[common.BlockHandle]bool),
				Garbage: make([]common.BlockHandle, 0),
				LastBeat: time.Now(),
			}
			return true
		} else {
			// copy garbage info
			response.Garbage = nodeInfo.Garbage
			manager.ColdNodes[address].Garbage = make([]common.BlockHandle, 0)
			manager.ColdNodes[address].LastBeat = time.Now()
			return false
		}
	} else {
		panic("Invalid Node Type")
		return false
	}
}

func (manager *DataNodeManager) SelectReplication(number int) ([]common.NodeAddress, error){
	// replication when creation
	if len(manager.HotNodes) < number {
		return nil, fmt.Errorf("LESS NODES THAN REPLICATION NUMBER")
	}

	manager.RLock()
	defer manager.RUnlock()
	// select server (random)
	var allNodes []common.NodeAddress
	var result []common.NodeAddress
	for addr, _ := range manager.HotNodes {
		allNodes = append(allNodes, addr)
	}
	selected := rand.Perm(len(manager.HotNodes))[:number]

	for _, idx := range selected {
		result = append(result, allNodes[idx])
	}
	return result, nil

}

func (manager *DataNodeManager) SelectReReplication(number int, block common.BlockHandle) ([]common.NodeAddress, error) {
	// select server for re-replication
	// do not have specific block
	return nil, nil // temp
}