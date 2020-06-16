package manager

import (
	"common"
	"time"
)

type Node struct {
	Blocks map[common.BlockHandle]bool
	Garbage []common.BlockHandle

	LastBeat time.Time
}


// for debug
type NodeStatusArgs struct {}

type NodeStatusResponse struct {
	Nodes map[common.NodeAddress]*Node
}