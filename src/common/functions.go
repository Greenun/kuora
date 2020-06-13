package common

import (
	"bytes"
	"fmt"
	logrus "github.com/Sirupsen/logrus"
	"math/rand"
	"runtime"
	"strconv"
	"time"
)

func RemoveAddress(nodeList []NodeAddress, target NodeAddress) ([]NodeAddress, error){
	idx := -1
	for i, v := range nodeList {
		if v == target {
			idx = i
			break
		}
	}
	if idx == -1 {
		return nil, fmt.Errorf("NO ELEMENT")
	}
	nodeList = append(nodeList[:idx], nodeList[idx+1:]...)
	return nodeList, nil
}

func GenerateRandomData(length int64) []byte {
	data := make([]byte, length)
	rand.Seed(int64(time.Now().Nanosecond()))
	rand.Read(data)
	return data
}

func GetGoroutineID() uint64 {
	b := make([]byte, 1024)
	b = b[:runtime.Stack(b, true)]
	//stackTrace := string(b)
	//if strings.Contains(stackTrace, "datanode") {
	//
	//} else if strings.Contains(stackTrace, "master") {
	//
	//} else if strings.Contains(stackTrace, "block") {
	//
	//}
	b = bytes.TrimPrefix(b, []byte("goroutine "))
	b = b[:bytes.IndexByte(b, ' ')]
	n, _ := strconv.ParseUint(string(b), 10, 64)
	return n
}

func Logger() *logrus.Entry{
	log := logrus.New()
	log.SetLevel(logrus.DebugLevel)
	logger := log.WithField("GoRoutine_ID", GetGoroutineID())
	logger.Infof("Init Logger - - - ")
	return logger
}