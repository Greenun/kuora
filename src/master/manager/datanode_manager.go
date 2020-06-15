package manager

import (
	"common"
	"common/ipc"
	"fmt"
	//logrus "github.com/Sirupsen/logrus"
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
	logger.Info("INIT NEW DATANODE MANAGER")
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
			//logger.Infof("Last beat: %v", manager.HotNodes[address].LastBeat)
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

func (manager *DataNodeManager) SelectReReplication(number int, handle common.BlockHandle) (common.NodeAddress, []common.NodeAddress, error) {
	// select server for re-replication
	// do not have specific block

	// for HOT Nodes -- not enough time
	manager.RLock()
	defer manager.RUnlock()

	var holder common.NodeAddress
	var receivers []common.NodeAddress

	for addr, node := range manager.HotNodes {
		_, exist := node.Blocks[handle]
		if !exist {
			receivers = append(receivers, addr)
		} else{
			if len(holder) == 0 {
				holder = addr
			}
		}
		if len(receivers) >= number && len(holder) > 0 {
			return holder, receivers, nil
		}
	}

	return "", nil, fmt.Errorf("NOT ENOUGH NODES FOR REPLICATION")
}

func (manager *DataNodeManager) HealthCheckNodes() map[common.NodeType][]common.NodeAddress {
	manager.RLock()
	defer manager.RUnlock()

	deadNodes := make(map[common.NodeType][]common.NodeAddress)
	currentTime := time.Now()
	for addr, info := range manager.HotNodes {
		if info.LastBeat.Add(common.HeartBeatTimeout).Before(currentTime) {
			deadNodes[common.HOT] = append(deadNodes[common.HOT], addr)
		}
	}

	for addr, info := range manager.ColdNodes {
		if info.LastBeat.Add(common.HeartBeatTimeout).Before(currentTime) {
			deadNodes[common.COLD] = append(deadNodes[common.COLD], addr)
		}
	}
	return deadNodes
}

func (manager *DataNodeManager) RemoveNode(address common.NodeAddress, nt common.NodeType) ([]common.BlockHandle, error) {
	manager.Lock()
	defer manager.Unlock()
	handles := make([]common.BlockHandle, 0)
	var target map[common.NodeAddress]*Node
	if nt == common.HOT {
		target = manager.HotNodes
	} else {
		target = manager.ColdNodes
	}
	node, exist := target[address]
	if !exist {
		return nil, fmt.Errorf("NODE DOES NOT EXIST %s", address)
	}
	for handle, blockExist := range node.Blocks {
		if blockExist {
			handles = append(handles, handle)
		}
	}
	// delete node from memory
	delete(target, address)

	return handles, nil // temp
}

// add block map during create file
func (manager *DataNodeManager) AddBlocks(handles []common.BlockHandle, addrs []common.NodeAddress) error {
	for _, addr := range addrs {
		nodeInfo, _ := manager.HotNodes[addr]
		for _, handle := range handles {
			nodeInfo.Blocks[handle] = true
		}
	}
	return nil
}

func (manager *DataNodeManager) PushGarbage(handle common.BlockHandle,
	address common.NodeAddress, dnt common.NodeType) error {
	if dnt == common.HOT {
		manager.HotNodes[address].Garbage = append(manager.HotNodes[address].Garbage, handle)
	} else {
		manager.ColdNodes[address].Garbage = append(manager.ColdNodes[address].Garbage, handle)
	}
	return nil
}