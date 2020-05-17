package manager

import (
	"common"
	"common/ipc"
	"fmt"
	"sync"

	logger "github.com/Sirupsen/logrus"
)

type BlockManager struct {
	sync.RWMutex

	Blocks map[common.BlockHandle]*Block
	Files map[common.HotKey]*FileInfo
	KeySet map[common.HotKey]common.ColdKey // map of hot key - cold key

	HandleCount common.BlockHandle
	ExpiredBlocks []common.BlockHandle
	//Status --> unstable / stable (like enum)
}

func NewBlockManager() *BlockManager {
	manager := &BlockManager{
		Blocks:      make(map[common.BlockHandle]*Block),
		Files:       make(map[common.HotKey]*FileInfo),
		HandleCount: 0,
	}
	logger.Info("INIT NEW BLOCK MANAGER")
	return manager
}

func generateKey(prev common.BlockHandle, numBlock int, offset common.Offset) common.HotKey{
	newKey := ""
	start := int(prev)
	for i := 0; i < numBlock; i++ {
		newKey += string(start+i)
		newKey += "_"
	}
	newKey += string(int(offset))
	return common.HotKey(newKey)
}

func (blockManager *BlockManager) CreateBlocks(addrs []common.NodeAddress,
	numBlock int) (common.HotKey, []common.NodeAddress, error) {
	// create file with number of blocks
	blockManager.Lock()
	defer blockManager.Unlock()

	blockHandle := blockManager.HandleCount
	start := int(blockHandle)
	blockManager.HandleCount += common.BlockHandle(numBlock)

	// generate file key
	key := generateKey(blockHandle, numBlock, 0)
	file := new(FileInfo)
	for i := 0; i < numBlock; i++ {
		file.Blocks = append(file.Blocks, common.BlockHandle(start + i))
	}
	blockManager.Files[key] = file

	rpcErrors := map[common.NodeAddress][]error{}
	rpcSucceed := map[common.NodeAddress][]ipc.CreateBlockResponse{}

	for _, addr := range addrs {
		var response ipc.CreateBlockResponse
		// create block in datanode needs
		err := ipc.Single(addr, "DataNode.CreateBlock", ipc.CreateBlockArgs{Handles:file.Blocks}, response)
		if err != nil {
			rpcErrors[addr] = append(rpcErrors[addr], err)
		} else {
			rpcSucceed[addr] = append(rpcSucceed[addr], response)
		}
	}
	return key, addrs, nil
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
