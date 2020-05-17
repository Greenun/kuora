package manager

import (
	"common"
	"time"
)

type Node struct {
	//blocks []common.BlockHandle
	Blocks map[common.BlockHandle]bool
	Garbage []common.BlockHandle

	LastBeat time.Time
}
