package datanode

import (
	"common"
	"common/ipc"
	"fmt"
	"io"
	"net"
	"net/rpc"
	"os"
	"sync"
	"time"

	logger "github.com/Sirupsen/logrus"
)

type DataNode struct {
	sync.RWMutex
	masterNode common.NodeAddress
	address common.NodeAddress
	listener net.Listener
	rootDir string
	nodeType common.NodeType

	blockMap map[common.BlockHandle]*Block
	garbage []common.BlockHandle
}


func Run(rootDir string, addr, masterAddr common.NodeAddress, nodeType common.NodeType) {
	d := &DataNode{
		masterNode: masterAddr,
		address: addr,
		rootDir: rootDir,
		nodeType: nodeType,
		blockMap: make(map[common.BlockHandle]*Block),
		garbage: make([]common.BlockHandle, 0),
	}
	rpcServer := rpc.NewServer()
	rpcServer.Register(d)
	l, err := net.Listen("tcp", string(d.address))
	if err != nil {
		logger.Error("RUNNING DATANODE RPC LISTENER ERROR")
	}
	d.listener = l
	// create root directory
	if _, err := os.Stat(rootDir); os.IsNotExist(err) {
		err := os.Mkdir(rootDir, 0755)
		if err != nil {
			logger.Errorf("ERROR DURING MKDIR %s", rootDir)
		}
	}
	//
	go func(){
		var err error
		var action string
		heartbeatTick := time.Tick(common.HeartBeatInterval)
		garbageTick := time.Tick(common.GarbageCollectionInterval)
		for {
			select {
				case <- heartbeatTick:
					action = "Heartbeat"
					err = d.Heartbeat()
				case <- garbageTick:
					action = "GarbageCollection"
					err = d.GarbageCollection()
			}
			if err != nil {
				logger.Errorf("ERROR OCCURRED DURING TASK %s", action)
			} else {
				logger.Infof(" - TASK: %s", action)
			}
		}
	}()

	go func(){
		for {
			connection, err := d.listener.Accept()
			if err != nil {
				logger.Errorf("ERROR OCCURRED DURING RPC: %v", err)
			} else {
				go func(conn io.ReadWriteCloser){
					rpcServer.ServeConn(conn)
					conn.Close()
				}(connection)
			}
		}
	}()
}

func (d *DataNode) Heartbeat() error {
	args := ipc.HeartBeatArgs{
		Address: d.address,
		DNType: d.nodeType,
	}
	var response ipc.HeartBeatResponse
	err := ipc.Single(d.masterNode, "MasterNode.HeartbeatResponse", args, &response)
	if err != nil {
		logger.Error("ERROR DURING HEARTBEAT")
		return err
	}
	d.garbage = append(d.garbage, response.Garbage...)
	return nil
}

func (d *DataNode) CreateBlock(args ipc.CreateBlockArgs, response *ipc.CreateBlockResponse ) error {
	d.Lock()
	defer d.Unlock()
	// temp
	hstr := ""
	for _, v := range args.Handles {
		hstr += string(v) + " "
	}
	logger.Info("Create Block RPC Call ", hstr)

	//files := make([]string, len(args.Handles))
	for _, handle := range args.Handles {
		//files = append(files, BlockFileFormat(d.rootDir, handle))
		logger.Infof("Format: %s", BlockFileFormat(d.rootDir, handle))
		osErr := createBlockFile(BlockFileFormat(d.rootDir, handle))
		if osErr != nil {
			logger.Errorf("ERROR OCCURRED DURING CREATE FILE: %d\n%v", handle, osErr.Error())
			return osErr
		}
		d.blockMap[handle] = &Block{
			length:0,
			version:0,
			corrupted:false,
		}
	}
	response.ResponseCode = 0 // temp

	return nil
}

func (d *DataNode) ReadBlock(args ipc.ReadBlockArgs, response *ipc.ReadBlockResponse) error {
	handle := args.Handle
	d.RLock()
	defer d.RUnlock()
	blockInfo, exist := d.blockMap[handle]
	if !exist {
		return fmt.Errorf("BLOCK %d DOES NOT EXIST", handle)
	}
	// read file
	filename := BlockFileFormat(d.rootDir, handle)
	data := make([]byte, args.Length)
	blockInfo.RLock()
	n, err := d.readFile(filename, args.Offset, data)
	blockInfo.RUnlock()

	if err != nil {
		if err == io.EOF {
			response.Length = n
			response.ErrCode = common.ReadEOF
			return nil
		}
		return err
	}
	response.Length = n
	response.Data = data

	return nil
}

func (d *DataNode) readFile(filename string, offset common.Offset, data []byte) (int, error) {
	fd, err := os.Open(filename)
	if err != nil {
		logger.Errorf("ERROR DURING READ FILE OPERATION: %v", err)
		return -1, err
	}
	defer fd.Close()
	return fd.ReadAt(data, int64(offset))
}

func (d *DataNode) WriteBlock(args ipc.WriteBlockArgs, response *ipc.WriteBlockResponse) error {
	filename := BlockFileFormat(d.rootDir, args.Handle)
	d.RLock()
	blockInfo, exist := d.blockMap[args.Handle]
	d.RUnlock()
	if !exist {
		return fmt.Errorf("BLOCK DOES NOT EXIST - %d", args.Handle)
	}
	editLength := args.Offset + common.Offset(len(args.Data))
	if editLength > common.BlockSize {
		logger.Errorf("BLOCK %d SIZE EXCEEDED", args.Handle)
		return fmt.Errorf("BLOCK SIZE EXCEEDED")
	}
		if editLength > common.Offset(blockInfo.length) {
		blockInfo.length = uint64(editLength)
	}
	blockInfo.Lock()
	_, err := d.writeFile(filename, args.Offset, args.Data)
	blockInfo.Unlock()

	if err != nil {
		return fmt.Errorf("BLOCK FILE WRITE ERROR")
	}

	// forward block
	if len(args.Secondaries) > 0 {
		// need to separate it from sync
		forwardResult := make(chan error, len(args.Secondaries))
		for _, t := range args.Secondaries {
			go func(target common.NodeAddress){
				forwardResult <- d.forwardBlockData(target, args.Data, args.Handle, args.Offset)
			}(t)

		}
		<- forwardResult // temp
	}
	return nil
}

func  (d *DataNode) writeFile(filename string, offset common.Offset, data []byte) (int, error) {
	file, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE, 0744)
	if err != nil {
		return -1, fmt.Errorf("ERROR OCCURRED - %s", err.Error())
	}
	defer file.Close()
	n, err := file.WriteAt(data, int64(offset)) // uint64 --> int64 ..
	if err != nil {
		return -1, fmt.Errorf("ERROR OCCURRED - %s", err.Error())
	}

	return n, nil
}

func (d *DataNode) forwardBlockData(target common.NodeAddress, data []byte,
	handle common.BlockHandle, offset common.Offset) error {
	writeArgs := ipc.WriteBlockArgs{
		Handle:      handle,
		Data:        data,
		Offset:      offset,
		Secondaries: nil,
	}
	var resp ipc.WriteBlockResponse
	err := ipc.Single(target, "DataNode.WriteBlock", writeArgs, &resp)
	if err != nil {
		return err
	}
	return nil
}

//func (d *DataNode) ForwardBlockData(args ipc.ForwardDataArgs, response *ipc.ForwardDataResponse) error {
//	forwardResult := make(chan bool, len(args.Target))
//	for _, t := range args.Target {
//		go func(data []byte, target common.NodeAddress){
//			writeArgs := ipc.WriteBlockArgs{
//				Handle: args.Handle,
//				Data: args.Data,
//				Secondaries: nil,
//			}
//			var resp ipc.WriteBlockResponse
//			err := ipc.Single(target, "DataNode.WriteBlock", writeArgs, &resp)
//			if err != nil {
//				forwardResult <- false
//			} else {
//				forwardResult <- true
//			}
//		}(args.Data, t)
//	}
//
//	logger.Infof("Forward Result: %s", <- forwardResult) // temp
//	return nil
//}

func (d *DataNode) GarbageCollection() error {
	logger.Infof("Garbage Collection For %d blocks", len(d.garbage))
	var err error
	for _, handle := range d.garbage {
		err = d.RemoveBlock(handle)
		if err != nil {
			logger.Errorf("ERROR DURING REMOVE FILE: %v", err)
			return err
		}
	}
	d.garbage = make([]common.BlockHandle, 0) // flush all
	return nil
}

func (d *DataNode) RemoveBlock(handle common.BlockHandle) error {
	d.Lock()
	//defer d.Unlock()
	delete(d.blockMap, handle)
	d.Unlock()

	blockName := BlockFileFormat(d.rootDir, handle)
	osErr := os.Remove(blockName)
	return osErr
}