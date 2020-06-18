package client

import (
	"bytes"
	"common"
	"fmt"
	"math/rand"
	"sync"
	"testing"
)

//var c = NewClient("127.0.0.1:40000")

//var keyList = make([]common.HotKey, 0)
//var keyMap = make(map[common.HotKey][]byte, 0)
var keyMap sync.Map
const (
	MASTER_ADDRESS = "127.0.0.1:40000"
	READ_TIMES = 300
	WRITE_TIMES = 20
	POOL_LENGTH = 30
)

var clientPool = make([]*Client, 0)
var clientMap = make(map[*Client]common.HotKey)

func TestMultiClient(t *testing.T) {
	keyMap = sync.Map{}
	length := int64(1 << 19)
	for i := 0; i < POOL_LENGTH; i++ {
		clientPool = append(clientPool, NewClient(MASTER_ADDRESS))
	}
	wg := sync.WaitGroup{}
	// Write Phase
	for i := 0; i < WRITE_TIMES; i++ {
		randomBytes := common.GenerateRandomData(length + int64(i*10))
		wg.Add(1)
		go func(idx int, rb []byte){
			idx = idx % POOL_LENGTH
			key, err := clientPool[idx].CreateAndWrite(rb)
			if err != nil {
				t.Errorf("ERROR WRITE - %v", err.Error())
			}
			clientMap[clientPool[idx]] = key
			//keyMap[key] = rb
			keyMap.Store(key, rb)
			wg.Done()
		}(i, randomBytes)
	}
	wg.Wait()
	keyList := make([]common.HotKey, 0)
	//for key, _ := range keyMap {
	//	keyList = append(keyList, key)
	//}
	keyMap.Range(func(k, v interface{}) bool{
		key := fmt.Sprintf("%s", k)
		keyList = append(keyList, common.HotKey(key))
		return true
	})
	wg = sync.WaitGroup{}
	// Read Phase
	for _, i := range rand.Perm(READ_TIMES) {
		idx := i % POOL_LENGTH
		wg.Add(1)
		go func(key common.HotKey){
			//answer := keyMap[key]
			temp, _ := keyMap.Load(key)
			answer, _ := temp.([]byte)
			buffer := make([]byte, len(answer))
			_, err := clientPool[idx].Read(key, 0, buffer)
			if err != nil {
				t.Errorf("ERROR READ - %v", err.Error())
			}
			if bytes.Compare(buffer, answer) != 0 {
				t.Errorf("Byte NOT EQUAL for %s", key)
			} else {
				t.Logf("Bytes Equal for %s", key)
			}
			wg.Done()
		}(keyList[idx % WRITE_TIMES])
		if idx % 4 == 0 {
			wg.Add(1)
			randomBytes := common.GenerateRandomData(length + int64(idx*10))
			go func(rb []byte){
				k, err := clientPool[(idx % POOL_LENGTH) + 1].CreateAndWrite(rb)
				if err != nil {
					t.Errorf("ERROR WRITE (WITH READ) - %v", err.Error())
				}
				keyMap.Store(k, rb)
				wg.Done()
			}(randomBytes)
		}
	}
	wg.Wait()
}

func Test_DeleteAll(t *testing.T) {
	// delete all
	var c = NewClient("127.0.0.1:40000")
	keys, err := c.ListKeys()
	if err != nil {
		t.Error("LIST ERROR")
		t.Fail()
	}
	for _, key := range keys {
		err := c.Delete(key)
		if err != nil {
			t.Errorf("ERROR - %s", err.Error())
			t.Fail()
		}
	}
}

//func TestFlow(t *testing.T) {
//	l1 := int64(10)
//
//	randomBytes := common.GenerateRandomData(l1)
//	t.Logf("Bytes: %v", randomBytes)
//	key, err := c.CreateAndWrite(randomBytes)
//	if err != nil {
//		t.Errorf("Create And Write File Error - %s", err.Error())
//	}
//	t.Logf("File Key : %s", key)
//	keyList = append(keyList, key)
//	keyMap[key] = randomBytes
//	buffer := make([]byte, l1)
//	n, _ := c.Read(key, 0, buffer)
//	t.Logf("Bytes: %v", n)
//	result := bytes.Compare(buffer, randomBytes)
//	t.Logf("Compare Result - %d", result) // result need to be 0
//	if result != 0 {
//		t.Fail()
//	}
//}
//
//func TestBigFileFlow(t *testing.T) {
//	length := int64(1 << 20 + 10)
//	randomBytes := common.GenerateRandomData(length)
//	key, err := c.CreateAndWrite(randomBytes)
//	if err != nil {
//		t.Errorf("Create And Write Error - %s}", err.Error())
//	}
//	t.Logf("File Key : %v", key)
//	keyList = append(keyList, key)
//	keyMap[key] = randomBytes
//	buffer := make([]byte, length)
//	n, err := c.Read(key, 0, buffer)
//	if err != nil {
//		t.Errorf("Error - %s", err.Error())
//	}
//	t.Logf("Bytes: %v", n)
//	result := bytes.Compare(buffer, randomBytes)
//	t.Logf("buffer ~ %d | buffer/randomBytes ~ %v/%v", len(buffer), buffer[:10], randomBytes[:10])
//	t.Logf("Compare Result - %d", result) // result need to be 0
//	if result != 0 {
//		t.Fail()
//	}
//}
//
//func TestReadFile(t *testing.T) {
//	for _, key := range keyList {
//		buffer := make([]byte, len(keyMap[key]))
//		n, _ := c.Read(key, 0, buffer)
//		t.Logf("Bytes: %d", n)
//		result := bytes.Compare(buffer, keyMap[key])
//		t.Logf("Compare Result - %d", result) // result need to be 0
//	}
//}
//
//func TestReadFileFromRandomOffset(t *testing.T) {
//	offset := common.Offset(0)
//	rand.Seed(int64(time.Now().Nanosecond()))
//	for key, answer := range keyMap {
//		offset = common.Offset(rand.Intn(len(answer) - 1))
//		randomLength := offset + common.Offset(rand.Intn(len(answer) - int(offset) - 1)) + 1
//		buffer := make([]byte, randomLength - offset)
//		n, err := c.Read(key, offset, buffer)
//		t.Logf("Offset: %d | Length: %d", offset, randomLength)
//		if err != nil && n == -1 {
//			t.Errorf("Error Occurred - %v", err.Error())
//		}
//		t.Logf("Bytes: %d", n)
//		if bytes.Compare(buffer, answer[offset:randomLength]) == 0 {
//			t.Logf("Corret For Key - %v", key)
//		}
//	}
//}
//
//func TestClient_ListKeys(t *testing.T) {
//	_, err := c.ListKeys()
//	if err != nil {
//		t.Fail()
//	}
//	t.Log("List Key Ends - - -")
//}
//
//func TestClient_NodeStatus(t *testing.T) {
//	_, err := c.NodeStatus()
//	if err != nil {
//		t.Fail()
//	}
//	t.Log("List Key Ends - - -")
//}
//
//func TestClient_Delete(t *testing.T) {
//	// delete file for created in this test
//	for k, _ := range keyMap {
//		err := c.Delete(k)
//		if err != nil {
//			t.Errorf(err.Error())
//		}
//	}
//}
//