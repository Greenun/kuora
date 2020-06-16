package common

import (
	"time"
)

type Node struct {
	Blocks map[BlockHandle]bool
	Garbage []BlockHandle

	LastBeat time.Time
}
