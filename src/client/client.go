package client

import (
	"common"
	"common/ipc"
	"fmt"
	logger "github.com/Sirupsen/logrus"
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

func (c *Client) Read(key common.HotKey, offset common.Offset, buffer []byte) (int, error) {
	handles, err := c.GetMetadata(key)
	if err != nil {
		return -1, err
	}
	if int(offset / common.BlockSize) > len(handles) {
		return -1, fmt.Errorf("OFFSET EXCEED FILE LENGTH")
	}
	var current int = 0
	for current < len(buffer) {

	}

	return 0, nil
}


func (c *Client) ReadBlock() error {
	return nil
}

func (c *Client) Write(key common.HotKey, offset common.Offset, data []byte) error {
	return nil
}

func (c *Client) Delete(key common.HotKey) error {
	return nil
}
