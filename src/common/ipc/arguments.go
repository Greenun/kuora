package ipc

import (
	"common"
)

type CreateBlockArgs struct {
	//handle common.BlockHandle
	Handles []common.BlockHandle
}

type CreateBlockResponse struct {
	ResponseCode common.Code
}

type WriteBlockArgs struct {

}

type WriteBlockResponse struct {

}

type ForwardDataArgs struct {

}

type ForwardDataResponse struct {

}

type ReadBlockArgs struct {
	Handle common.BlockHandle
	Offset common.Offset
	Length int
}

type ReadBlockResponse struct {
	Data []byte
	Length int
	ErrCode common.Code
}

// master actions (heartbeat etc..)

type HeartBeatArgs struct {
	Address common.NodeAddress
	DNType common.NodeType
}

type HeartBeatResponse struct {
	Garbage []common.BlockHandle
}

// key-value based

type ListKeysArgs struct {
	Limit int // how many files
}

type ListKeysResponse struct {
	Keys []common.HotKey
}

type GetMetadataArgs struct {
	FileKey common.HotKey
}

type GetMetadataResponse struct {
	// blocks
	BlockHandles []common.BlockHandle
}

type GetBlockInfoArgs struct {
	Handle common.BlockHandle
}

type GetBlockInfoResponse struct {
	// locations
	Locations []common.NodeAddress
	Primary common.NodeAddress
}

type CreateFileArgs struct {
	Length uint64
}

type CreateFileResponse struct {
	Key common.HotKey
	ErrCode common.Code
}

type DeleteFileArgs struct {
	Key common.HotKey
}

type DeleteFileResponse struct {
	ErrCode common.Code
}