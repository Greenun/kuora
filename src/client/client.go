package client

import (
	"common"
	"common/ipc"
)

type Client struct {
	master common.NodeAddress
}

func (c *Client) Create() error {

}

func (c *Client) Read(key common.HotKey, offset common.Offset, buffer []byte) (int, error) {
	var getMetadata ipc.GetMetadataResponse
	err := ipc.Single(c.master, "MasterNode.GetMetadata", ipc.GetMetadataArgs{FileKey: key}, getMetadata)
	if err != nil {
		// handle error
		return -1, err
	}
}

func (c *Client) ReadBlock() error {

}

func (c *Client) Write(key common.HotKey, offset common.Offset, data []byte) error {

}

func (c *Client) Delete(key common.HotKey) error {

}
