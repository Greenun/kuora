package manager

import (
	"common"
	"sync"
	"time"
	logrus "github.com/Sirupsen/logrus"
)

var logger *logrus.Entry = common.Logger()

type Block struct {
	sync.RWMutex
	Expired time.Time // hot file would be expired
	Locations []common.NodeAddress
	Primary common.NodeAddress
	//
}

type FileInfo struct {
	sync.RWMutex
	Blocks []common.BlockHandle
}