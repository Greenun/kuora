package common

import "time"

type NodeAddress string

type Code int

type Offset uint64
type BlockHandle uint64
type Checksum int64
type BlockIndex int32
type BlockVersion int32

type HotKey string
type ColdKey string
type Filename string

type NodeType int

const (
	HOT = iota
	COLD
)

type Error struct {
	errCode Code
	errMsg  string
}

func (e *Error) Error() (Code, string) {
	return e.errCode, e.errMsg
}

// config
const (
	ReplicaNum = 3
	ColdReplicaNum = 1
	ChunkSize = 1 << 19 // 512KB

	HeartBeatInterval = 500 * time.Millisecond
	GarbageCollectionInterval = 30 * time.Second // temp

)