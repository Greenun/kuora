package datanode

import (
	"common"
	"fmt"
	"os"
	"path"
	"sync"
)

const (
	BlockPermission = 0644
)

type Block struct {
	sync.RWMutex
	length uint64
	version common.BlockVersion
	corrupted bool
}

func BlockFileFormat(rootDir string, handle common.BlockHandle) string {
	return path.Join(rootDir, fmt.Sprint("Block_%v.blk", handle))
}

func CreateBlockFile(filename string) error {
	_, err := os.OpenFile(filename, os.O_CREATE, BlockPermission)
	if err != nil {
		return err
	}
	return nil
}