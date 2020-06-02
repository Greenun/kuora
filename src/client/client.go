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

func (c *Client) Create(length uint64) error {
	var response ipc.CreateFileResponse
	args := ipc.CreateFileArgs{Length:length}
	err := ipc.Single(c.master, "MasterNode.CreateFile", args, &response)
	if err != nil {
		logger.Error("CREATE ERROR")
		return err
	}
	fileKey := response.Key
	logger.Infof("File Key: %s -- %d", fileKey, response.ErrCode)

	return nil
}

func (c *Client) GetMetadata(key common.HotKey) ([]common.BlockHandle, error){
	var getMetadata ipc.GetMetadataResponse
	err := ipc.Single(c.master, "MasterNode.GetMetadata", ipc.GetMetadataArgs{FileKey: key}, getMetadata)
	if err != nil {
		// handle error
		return nil, err
	}
	return getMetadata.BlockHandles, nil
}

func (c *Client) Read(key common.HotKey, offset common.Offset, buffer []byte) (int64, error) {
	handles, err := c.GetMetadata(key)
	if err != nil {
		return -1, err
	}
	if int(offset / common.BlockSize) > len(handles) {
		return -1, fmt.Errorf("OFFSET EXCEED FILE LENGTH")
	}
	totalData := make([]byte, len(buffer))
	var current int64 = 0
	// read for length of buffer
	for current < int64(len(buffer)) {
		idx := common.BlockIndex(offset / common.BlockSize)
		blockOffset := offset % common.BlockSize

		if int(idx) >= len(handles) {
			return -1, fmt.Errorf("Read after EOF Error")
		}

		handle := handles[idx]
		readNum, err := c.ReadBlock(handle, blockOffset, buffer)

		if err != nil {
			return -1, fmt.Errorf("Read Error")
		}

		current += readNum
		offset += common.Offset(readNum)
		totalData = append(totalData, buffer[:readNum]...)
	}
	logger.Info(totalData) // temp
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
			logger.Errorf("ERROR OCCURRED DURING READ BLOCK %d from %s", handle, locations[randomIndex])
		} else {
			break
		}
	}
	if err != nil {
		return -1, fmt.Errorf("READ BLOCK ERROR FOR ALL LOCATIONS")
	} else if readResponse.ErrCode == common.ReadEOF {
		return int64(readResponse.Length), fmt.Errorf("READ EOF")
	}

	return readNum, nil
}

func (c *Client) Write(key common.HotKey, offset common.Offset, data []byte) error {
	handles, err := c.GetMetadata(key)
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

		for i := 0; i < 3; i++ {
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
	err := ipc.Single(c.master, "Master.GetBlockInfo", ipc.GetBlockInfoArgs{Handle:handle}, &resp)
	if err != nil {
		logger.Errorf("ERROR OCCURRED DURING GetBlockInfo RPC")
		return err
	}
	var writeResp ipc.WriteBlockResponse
	secondaries, err := common.RemoveAddress(resp.Locations, resp.Primary)
	if err != nil {
		return fmt.Errorf("PRIMARY DOES NOT EXIST IN LOCATIONS")
	}

	writeErr := ipc.Single(resp.Primary, "DataNode.WriteBlock", ipc.WriteBlockArgs{
		Handle:      handle,
		Data:        data,
		Offset:      offset,
		Secondaries: secondaries,
	}, &writeResp)
	if writeErr != nil {
		return fmt.Errorf("WRITE BLOCK OPERATION ERROR")
	}
	return nil
}

func (c *Client) Delete(key common.HotKey) error {
	return nil
}

func (c *Client) ListKeys() error {
	return nil
}