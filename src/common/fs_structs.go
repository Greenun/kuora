package common

import "time"

type NodeAddress string

type code int

type Offset uint64
type ChunkHandle uint64
type Checksum int64
type ChunkIndex int32

type HotKey string
type ColdKey string
type Filename string

type Lease struct {
	primary NodeAddress
	secondaries []NodeAddress
	expired time.Time
}

type Error struct {
	errCode code
	errMsg string
}

func (e *Error) Error() (code, string) {
	return e.errCode, e.errMsg
}

// config

const (
	ReplicaNum = 3
	ChunkSize = 1 << 19 // 512KB
	GarbageTag = "__deleted"

	HeartBeatInterval = 500 * time.Millisecond

)