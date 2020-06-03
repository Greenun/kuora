package master

import (
	"common"
	"common/ipc"
	"fmt"
	logger "github.com/Sirupsen/logrus"
	"io"
	"master/manager"
	"math"
	"net"
	"net/rpc"
	"time"
)

type MasterNode struct {
	Address common.NodeAddress
	RootDir string
	listener net.Listener

	blockManager *manager.BlockManager
	dataNodeManager *manager.DataNodeManager
}

func Run(address common.NodeAddress, rootDir string) *MasterNode {
	logger.Info("Run Master Node - - -")
	masterNode := &MasterNode{
		Address: address,
		RootDir: rootDir,
	}
	masterNode.init()

	rpcServer := rpc.NewServer()
	rpcServer.Register(masterNode)
	listener, netErr := net.Listen("tcp", string(address))
	if netErr != nil {
		logger.Error("NET LISTENING ERROR")
	}
	masterNode.listener = listener

	// handle request
	go func(){
		for {
			connection, err := masterNode.listener.Accept()
			if err != nil {
				logger.Errorf("MASTER CONNECTION ERROR")
			} else {
				go func(conn io.ReadWriteCloser){
					rpcServer.ServeConn(conn)
					conn.Close()
				}(connection)
			}
		}
	}()

	// background task
	go func(){
		healthCheckTick := time.Tick(common.HealthCheckInterval)
		expireTick := time.Tick(common.ExpireInterval)
		var err error
		for {
			select {
				case <- healthCheckTick:
					err = masterNode.healthCheck()
				case <- expireTick:
					err = masterNode.ExpireCheck()

			}
			if err != nil {
				logger.Errorf("ERROR OCCURED DURING BACKGROUND TASK - %s", err.Error())
			}
		}
	}()

	return masterNode
}

func (m *MasterNode) init() {
	m.blockManager = manager.NewBlockManager()
	m.dataNodeManager = manager.NewDataNodeManager()
}

func (m *MasterNode) checkKey(key common.HotKey) (common.ColdKey, bool){
	m.blockManager.RLock()
	defer m.blockManager.RUnlock()
	coldKey, exist := m.blockManager.KeySet[key]
	if !exist {
		return "", exist
	} else {
		return coldKey, exist
	}
}

func (m *MasterNode) GetFileMetadata(args ipc.GetMetadataArgs, response *ipc.GetMetadataResponse) error {
	m.blockManager.RLock()
	defer m.blockManager.RUnlock()

	key := args.FileKey
	coldKey, exist := m.blockManager.KeySet[key]
	if exist {
		// handle cold key data
		logger.Infof("Cold Key Found : %s", coldKey)
		return fmt.Errorf("NOT IMPLEMENTED YET") // temp
	} else {
		// handle hot key (in memory)
		logger.Infof("Hot Key : %s", key)
		fileInfo, err := m.blockManager.Files[key]
		if !err {
			return fmt.Errorf("FILE WITH KEY[%s] NOT FOUND", key)
		}
		fileInfo.RLock()
		defer fileInfo.RUnlock()
		response.BlockHandles = fileInfo.Blocks
	}
	return nil
}

func (m *MasterNode) GetBlockInfo(args ipc.GetBlockInfoArgs, response *ipc.GetBlockInfoResponse) error {
	handle := args.Handle

	m.blockManager.RLock()
	blockInfo, exist := m.blockManager.Blocks[handle]
	m.blockManager.RUnlock()
	if !exist {
		return fmt.Errorf("BLOCK %d NOT EXIST", handle)
	}

	blockInfo.RLock()
	defer blockInfo.RUnlock()

	response.Locations = blockInfo.Locations
	response.Primary = blockInfo.Primary
	return nil
}

func (m *MasterNode) HeartbeatResponse(args ipc.HeartBeatArgs, response *ipc.HeartBeatResponse) error {
	m.dataNodeManager.Heartbeat(args.Address, args.DNType, response)
	//if nodeType == common.HOT {
	//
	//} else if nodeType == common.COLD {
	//
	//} else {
	//	return fmt.Errorf("UNVALID NODE TYPE")
	//}

	return nil
}

func (m *MasterNode) CreateFile(args ipc.CreateFileArgs, response *ipc.CreateFileResponse) error {
	logger.Info("Create File Operation")
	length := args.Length
	blockNum := int(math.Ceil(float64(length) / float64(common.BlockSize)))
	//selectedNodes, err := m.dataNodeManager.SelectReplication(common.ReplicaNum)
	selectedNodes, err := m.dataNodeManager.SelectReplication(2)
	if err != nil {
		logger.Errorf("ERROR DURING CREATE FILE: %v", err)
		return err
	}
	key, _, opErr := m.blockManager.CreateBlocks(selectedNodes, blockNum)
	if opErr != nil {
		response.ErrCode = 1
		return opErr
	}
	response.Key = key
	response.ErrCode = 0 // temp
	return nil
}

func (m *MasterNode) DeleteFile(args ipc.DeleteFileArgs, response *ipc.DeleteFileResponse) error {
	coldKey, exist := m.checkKey(args.Key)
	m.blockManager.RLock()
	defer m.blockManager.RUnlock()

	if exist {
		// cold key search
		logger.Infof("Search for %s", coldKey)


	} else {
		fileInfo, fileExist := m.blockManager.Files[args.Key]
		if !fileExist {
			return fmt.Errorf("ERROR DURING SEARCH FILE %s", args.Key)
		}
		for _, handle := range fileInfo.Blocks {
			blockInfo, e := m.blockManager.Blocks[handle]
			if !e {
				logger.Errorf("BLOCK %d DOES NOT EXIST", handle)
			}
			m.dataNodeManager.Lock()
			for _, dn := range blockInfo.Locations {
				// add garbage
				m.dataNodeManager.PushGarbage(handle, dn, common.HOT)
			}
			m.dataNodeManager.Unlock()
			// block delete
			delete(m.blockManager.Blocks, handle)
		}
		// delete key
		delete(m.blockManager.Files, args.Key)
	}
	return nil
}

func (m *MasterNode) healthCheck() error {
	deadNodes := m.dataNodeManager.HealthCheckNodes()
	for _, hotDead := range deadNodes[common.HOT] {
		logger.Infof("Remove Node - %s", hotDead)
		handles, err := m.dataNodeManager.RemoveNode(hotDead, common.HOT)
		if err != nil {
			return fmt.Errorf("ERROR OCCURRED DURING REMOVE NODE - %s", err.Error())
		}
		logger.Infof("Sweep Block Phase")
		m.blockManager.SweepBlocks(handles, hotDead)
	}

	for _, coldDead := range deadNodes[common.COLD] {
		logger.Infof("Remove Node - %s", coldDead)
		handles, err := m.dataNodeManager.RemoveNode(coldDead, common.COLD)
		if err != nil {
			return fmt.Errorf("ERROR OCCURRED DURING REMOVE NODE - %s", err.Error())
		}
		logger.Infof("Sweep Block Phase")
		m.blockManager.SweepBlocks(handles, coldDead)
	}
}

func (m *MasterNode) ListKeys(args ipc.ListKeysArgs, response *ipc.ListKeysResponse) error {
	return nil
}

func (m *MasterNode) ExpireCheck() error {
	return nil
}