package manager

import (
	"common"
	"common/ipc"
	"fmt"
	"strconv"
	"sync"
	"time"
)

type BlockManager struct {
	sync.RWMutex

	Blocks map[common.BlockHandle]*Block
	Files map[common.HotKey]*FileInfo
	KeySet map[common.HotKey]common.ColdKey // map of hot key - cold key

	HandleCount common.BlockHandle
	ExpiredBlocks []common.BlockHandle
	RequireCopy []common.BlockHandle
	//Status --> unstable / stable (like enum)
}

func NewBlockManager() *BlockManager {
	manager := &BlockManager{
		Blocks:      make(map[common.BlockHandle]*Block),
		Files:       make(map[common.HotKey]*FileInfo),
		HandleCount: 0,
		RequireCopy: make([]common.BlockHandle, 0),
	}
	logger.Info("INIT NEW BLOCK MANAGER")
	return manager
}

func generateKey(prev common.BlockHandle, numBlock int, offset common.Offset) common.HotKey{
	newKey := ""
	start := int(prev)
	for i := 0; i < numBlock; i++ {
		newKey += strconv.Itoa(start+i)
		newKey += "_"
	}
	newKey += strconv.Itoa(int(offset))
	return common.HotKey(newKey)
}

func (blockManager *BlockManager) CreateBlocks(addrs []common.NodeAddress,
	numBlock int, length int64) (common.HotKey, []common.BlockHandle, error) {
	// create file with number of blocks
	blockManager.Lock()
	defer blockManager.Unlock()

	blockHandle := blockManager.HandleCount
	start := int(blockHandle)
	blockManager.HandleCount += common.BlockHandle(numBlock)
	blocks := make([]common.BlockHandle, 0)
	// generate file key
	key := generateKey(blockHandle, numBlock, 0)
	file := new(FileInfo)
	for i := 0; i < numBlock; i++ {
		file.Blocks = append(file.Blocks, common.BlockHandle(start + i))
		blocks = append(blocks, common.BlockHandle(start + i))
	}
	file.Length = length
	blockManager.Files[key] = file

	rpcErrors := map[common.NodeAddress]error{}
	rpcSucceed := map[common.NodeAddress]ipc.CreateBlockResponse{}

	for _, addr := range addrs {
		var response ipc.CreateBlockResponse
		// create block in datanode needs
		err := ipc.Single(addr, "DataNode.CreateBlock", ipc.CreateBlockArgs{Handles:file.Blocks}, &response)
		if err != nil {
			rpcErrors[addr] = err
		} else {
			rpcSucceed[addr] = response
		}
	}
	// set primary, locations
	logger.Infof("%v", rpcSucceed)
	logger.Infof("%v", rpcErrors)
	for _, handle := range file.Blocks {
		if _, ok := blockManager.Blocks[handle]; !ok {
			blockManager.Blocks[handle] = &Block{
				Expired:   time.Now().Add(common.ExpireTime),
				Locations: make([]common.NodeAddress, 0),
				Primary:   common.NodeAddress(""),
			}
		}
		for k, _ := range rpcSucceed {
			blockManager.Blocks[handle].Locations = append(blockManager.Blocks[handle].Locations, k)
		}
		blockManager.Blocks[handle].Primary = blockManager.Blocks[handle].Locations[0]
		logger.Infof("Create Block Info - %v", blockManager.Blocks[handle])
	}
	return key, blocks, nil
}

func (blockManager *BlockManager) AddBlock(key common.HotKey) error {
	blockManager.Lock()
	defer blockManager.Unlock()

	info, exist := blockManager.Files[key]
	if !exist {
		return fmt.Errorf("FILE DOES NOT EXIST")
	}
	// temp
	fmt.Println(info)
	return nil
}

func (blockManager *BlockManager) CopyCompleted(handles []common.BlockHandle) error {
	newList := make([]common.BlockHandle, 0)
	existMap := make(map[common.BlockHandle]bool)
	for _, handle := range handles {
		existMap[handle] = true
	}
	for _, origin := range blockManager.RequireCopy {
		if _, exist := existMap[origin]; !exist {
			newList = append(newList, origin)
		}
	}
	// change new list
	blockManager.RequireCopy = newList
	return nil
}

func (blockManager *BlockManager) SweepBlocks(handles []common.BlockHandle, address common.NodeAddress) error {
	logger.Infof("HANDLES : %v", handles)
	primary := false
	errorMsg := ""
	for _, handle := range handles {
		blockManager.RLock()
		block, exist := blockManager.Blocks[handle]
		blockManager.RUnlock()
		if !exist {
			logger.Warningf("Block %d Does Not Exist", handle)
		}
		if block.Primary == address {
			primary = true
		}
		block.Lock()
		var remains []common.NodeAddress
		for _, a := range block.Locations {
			if a != address {
				remains = append(remains, a)
			}
		}
		logger.Infof("block - %v", block)
		block.Locations = remains
		numReplicas := len(remains)
		// change primary node
		if primary && numReplicas > 0 {
			block.Primary = remains[0]
		}
		block.Unlock()

		if numReplicas == 0 {
			msg := fmt.Sprintf("NO REPLICAS EXIST FOR HANDLE %d", handle)
			logger.Errorf(msg)
			blockManager.RequireCopy = append(blockManager.RequireCopy, handle)
			errorMsg += msg + "\n"
		} else if numReplicas < common.ReplicaNum {
			blockManager.RequireCopy = append(blockManager.RequireCopy, handle)
		}
	}

	if len(errorMsg) == 0 {
		return nil
	} else {
		return fmt.Errorf(errorMsg)
	}
}

func (blockManager *BlockManager) registerBlocks(handle common.BlockHandle,
	address common.NodeAddress, primary bool) error {
	blockManager.RLock()
	blockInfo, exist := blockManager.Blocks[handle]
	blockManager.RUnlock()
	if !exist {
		return fmt.Errorf("BLOCK DOES NOT EXIST - function: registerBlocks")
	}
	blockInfo.Locations = append(blockInfo.Locations, address)
	if primary {
		blockInfo.Primary = address
	}
	return nil
}

func (blockManager *BlockManager) RequiredCheck() []common.BlockHandle {
	requiredHandles := make([]common.BlockHandle, 0)
	blockManager.RLock()
	for _, handle := range blockManager.RequireCopy {
		if len(blockManager.Blocks[handle].Locations) < common.ReplicaNum {
			requiredHandles = append(requiredHandles, handle)
		}
	}
	blockManager.RUnlock()

	// required handle duplicate check
	checkedHandles := make([]common.BlockHandle, 0)
	existMap := make(map[common.BlockHandle]bool)
	for _, handle := range requiredHandles {
		if _, exist := existMap[handle]; !exist {
			existMap[handle] = true
			checkedHandles = append(checkedHandles, handle)
		}
	}
	blockManager.Lock()
	blockManager.RequireCopy = make([]common.BlockHandle, len(checkedHandles))
	copy(blockManager.RequireCopy, checkedHandles)
	blockManager.Unlock()

	if len(blockManager.RequireCopy) > 0 {
		return blockManager.RequireCopy
	} else {
		return nil
	}
}
