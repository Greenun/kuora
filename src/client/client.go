package client

import (
	"common"
	"common/ipc"
	"fmt"
	logger "github.com/Sirupsen/logrus"
	"math/rand"
	"time"
)

type Client struct {
	master common.NodeAddress
}

func NewClient(address common.NodeAddress) *Client{
	c := &Client{master:address}
	return c
}

func (c *Client) Create(length uint64) (common.HotKey, error) {
	var response ipc.CreateFileResponse
	args := ipc.CreateFileArgs{Length:length}
	err := ipc.Single(c.master, "MasterNode.CreateFile", args, &response)
	if err != nil {
		logger.Error("CREATE ERROR")
		return "", err
	}
	fileKey := response.Key
	logger.Infof("File Key: %s -- %d", fileKey, response.ErrCode)

	return fileKey, nil
}

func (c *Client) GetMetadata(key common.HotKey) ([]common.BlockHandle, int64, error){
	var getMetadata ipc.GetMetadataResponse
	err := ipc.Single(c.master, "MasterNode.GetFileMetadata", ipc.GetMetadataArgs{FileKey: key}, &getMetadata)
	if err != nil {
		// handle error
		return nil, 0, err
	}
	return getMetadata.BlockHandles, getMetadata.Length,nil
}

func (c *Client) Read(key common.HotKey, offset common.Offset, buffer []byte) (int64, error) {
	//(int64, []byte, error)
	handles, length, err := c.GetMetadata(key)
	if err != nil {
		return -1, err
	}
	if int(offset / common.BlockSize) > len(handles) {
		return -1, fmt.Errorf("OFFSET EXCEED FILE LENGTH")
	}
	var current int64 = 0
	blockBuffer := make([]byte, len(buffer))
	var bound int64
	if int64(len(buffer)) + int64(offset) > length {
		bound = length - int64(offset)
	} else {
		bound = int64(len(buffer))
	}
	// read for length of buffer
	for current < bound {
		idx := common.BlockIndex(offset / common.BlockSize)
		blockOffset := offset % common.BlockSize
		if int(idx) >= len(handles) {
			return -1, fmt.Errorf("Read after EOF Error")
		}
		handle := handles[idx]
		//logger.Infof("O Offset: %d, BlockSize: %d, key: %s, handle: %d", offset, common.BlockSize, key, handle)
		//logger.Infof("Index: %d, Offset: %d", idx, blockOffset)
		//logger.Infof("Buffer Length: %d, Current: %d", len(buffer), current)
		readNum, err := c.ReadBlock(handle, blockOffset, blockBuffer)
		//logger.Infof("Read Number: %d ", readNum)
		if err != nil {
			logger.Warnf("Error : %v", err)
			return -1, fmt.Errorf("READ ERROR - %v", err.Error())
		}
		copy(buffer[current:], blockBuffer[:readNum])
		current += readNum
		offset += common.Offset(readNum)
	}
	return current, nil
}


func (c *Client) ReadBlock(handle common.BlockHandle, offset common.Offset, buffer []byte) (int64, error) {
	var readNum int64 = 0
	bufferLength := len(buffer)

	if common.BlockSize - offset > common.Offset(bufferLength) {
		readNum = int64(bufferLength)
	} else {
		readNum = int64(common.BlockSize - offset)
	}

	var infoResponse ipc.GetBlockInfoResponse

	err := ipc.Single(c.master, "MasterNode.GetBlockInfo", ipc.GetBlockInfoArgs{Handle:handle}, &infoResponse)
	if err != nil {
		return 0, fmt.Errorf("RPC CALL ERROR - %s", err.Error())
	}
	if len(infoResponse.Locations) == 0 {
		return 0, fmt.Errorf("REPLICAS ARE NOT EXIST")
	}
	locations := infoResponse.Locations
	rand.Seed(int64(time.Now().Nanosecond()))

	var readResponse ipc.ReadBlockResponse
	for _, randomIndex := range rand.Perm(len(locations)) {
		err := ipc.Single(locations[randomIndex],
			"DataNode.ReadBlock",
			ipc.ReadBlockArgs{
				Handle: handle,
				Offset: offset,
				Length: len(buffer),
			},
			&readResponse,
		)
		if err != nil {
			// temp
			logger.Errorf("ERROR OCCURRED DURING READ BLOCK %d from %s - %v", handle, locations[randomIndex], err.Error())
			//fmt.Errorf("ERROR OCCURRED DURING READ BLOCK %d from %s", handle, locations[randomIndex])
		} else {
			//logger.Infof("Read Data From Node %v", locations[randomIndex])
			copy(buffer, readResponse.Data) // dump bytes
			//logger.Infof("DATA: %v, %v, %v, %d", buffer, readResponse.Data, readResponse.ErrCode, readResponse.Length)
			break
		}
	}
	if err != nil {
		return -1, fmt.Errorf("READ BLOCK ERROR FOR ALL LOCATIONS")
	} else if readResponse.ErrCode == common.ReadEOF {
		return int64(readResponse.Length), nil //fmt.Errorf("CODE-%v", common.ReadEOF)
	} else {
		return readNum, nil
	}
}

func (c *Client) Write(key common.HotKey, offset common.Offset, data []byte) error {
	handles, _, err := c.GetMetadata(key)
	if err != nil {
		return err
	}
	if offset / common.BlockSize > common.Offset(len(handles)) {
		return fmt.Errorf("WRITE OFFSET EXCEEDS FILE LENGTH")
	}

	var current int = 0
	for current < len(data) {
		blockIndex := offset / common.BlockSize
		innerOffset := offset % common.BlockSize

		handle := handles[blockIndex]
		blockMargin := int(common.BlockSize - innerOffset)
		var written int
		if current + blockMargin > len(data) {
			written = len(data) - current
		} else {
			written = blockMargin
		}
		// three times
		for i := 0; i < common.MaxRetry; i++ {
			err = c.WriteBlock(handle, innerOffset, data[current:current+written])
			if err != nil {
				logger.Errorf("ERROR DURING WRITE BLOCK - %s", err.Error())
			} else {
				break
			}
		}
		if err != nil {
			return err
		}
		offset += common.Offset(written)
		current += written
	}

	return nil
}

func (c *Client) WriteBlock(handle common.BlockHandle, offset common.Offset, data []byte) error {
	if common.Offset(len(data)) + offset > common.BlockSize {
		return fmt.Errorf("OFFSET + DATA LENGTH EXCEEDS BLOCK SIZE")
	}
	var resp ipc.GetBlockInfoResponse
	err := ipc.Single(c.master, "MasterNode.GetBlockInfo", ipc.GetBlockInfoArgs{Handle:handle}, &resp)
	if err != nil {
		logger.Errorf("ERROR OCCURRED DURING GetBlockInfo RPC")
		return err
	}
	var writeResp ipc.WriteBlockResponse
	secondaries, err := common.RemoveAddress(resp.Locations, resp.Primary)
	if err != nil {
		return fmt.Errorf("PRIMARY DOES NOT EXIST IN LOCATIONS")
	}
	//logger.Infof("Primary Address - %s", resp.Primary)
	writeErr := ipc.Single(resp.Primary, "DataNode.WriteBlock", ipc.WriteBlockArgs{
		Handle:      handle,
		Data:        data,
		Offset:      offset,
		Secondaries: secondaries,
	}, &writeResp)
	if writeErr != nil {
		return fmt.Errorf("WRITE BLOCK OPERATION ERROR - %s", writeErr.Error())
	}
	return nil
}

// debug
func (c *Client) CreateAndWrite(data []byte) (common.HotKey, error) {
	length := uint64(len(data))
	key, createErr := c.Create(length)
	if createErr != nil {
		return "", fmt.Errorf("ERROR OCCURRED DURING CREATE FILE - %s", createErr.Error())
	}
	writeErr := c.Write(key, 0, data)
	if writeErr != nil {
		return "", fmt.Errorf("ERROR OCCURRED DURING WRITE FILE - %s", writeErr.Error())
	}

	return key, nil
}

func (c *Client) Delete(key common.HotKey) error {
	var response ipc.DeleteFileResponse

	err := ipc.Single(c.master, "MasterNode.DeleteFile", ipc.DeleteFileArgs{Key:key}, &response)
	if err != nil {
		return fmt.Errorf("DELETE ERROR - %s", err.Error())
	}
	return nil
}

func (c *Client) ListKeys() ([]common.HotKey, error) {
	var response ipc.ListKeysResponse
	err := ipc.Single(c.master, "MasterNode.ListKeys", ipc.ListKeysArgs{}, &response)
	if err != nil {
		return nil, fmt.Errorf("LIST KEY ERROR - %s", err.Error())
	}
	//logger.Infof("Keys - - -")
	//for i, key := range response.Keys {
	//	logger.Infof("%d: %s", i+1, key)
	//}
	return response.Keys, nil
}

func (c *Client) NodeStatus() (map[common.NodeAddress]*common.Node ,error) {
	var response ipc.NodeStatusResponse
	err := ipc.Single(c.master, "MasterNode.NodeStatus", ipc.NodeStatusArgs{}, &response)
	if err != nil {
		return nil, fmt.Errorf("NODE STATUS ERROR - %s", err.Error())
	}
	//for k, v := range response.Nodes {
	//	logger.Infof("%s : %v", k, v)
	//}

	return response.Nodes, nil
}