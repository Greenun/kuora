package common

import "time"

type NodeAddress string

type Code int

type Offset int64
type BlockHandle uint64
type Checksum int64
type BlockIndex int32
type BlockVersion int32

type HotKey string
type ColdKey string

type NodeType int

const (
	HOT = iota
	COLD
)

type Error struct {
	errCode Code
	errMsg  string
}

// error code
const (
	ReadOK = 400 + iota
	ReadEOF
)

// Status
const (
	STABLE = 111 + iota
	UNSTABLE
)

func (e *Error) Error() (Code, string) {
	return e.errCode, e.errMsg
}

// config
const (
	ReplicaNum = 2 //3 temp
	ColdReplicaNum = 1
	BlockSize = 1 << 19 // 512KB

	HeartBeatInterval = 500 * time.Millisecond
	GarbageCollectionInterval = 30 * time.Second // temp
	HeartBeatTimeout = 5 * time.Second
	HealthCheckInterval = 4*HeartBeatInterval
	ExpireInterval = 60 * time.Second // temp
	RearrangeInterval = 2*HealthCheckInterval

	ExpireTime = 3 * time.Minute
	MaxRetry = 3
)