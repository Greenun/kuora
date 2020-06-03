package common

import (
	"fmt"
	"math/rand"
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