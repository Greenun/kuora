package client

import (
	"bytes"
	"common"
	"math/rand"
	"testing"
	"time"
)

var c = NewClient("127.0.0.1:40000")

var keyList = make([]common.HotKey, 0)
var keyMap = make(map[common.HotKey][]byte, 0)

func TestFlow(t *testing.T) {
	l1 := int64(10)

	randomBytes := common.GenerateRandomData(l1)
	t.Logf("Bytes: %v", randomBytes)
	key, err := c.CreateAndWrite(randomBytes)
	if err != nil {
		t.Errorf("Create And Write File Error - %s", err.Error())
	}
	t.Logf("File Key : %s", key)
	keyList = append(keyList, key)
	keyMap[key] = randomBytes
	buffer := make([]byte, l1)
	n, _ := c.Read(key, 0, buffer)
	t.Logf("Bytes: %v", n)
	result := bytes.Compare(buffer, randomBytes)
	t.Logf("Compare Result - %d", result) // result need to be 0
	if result != 0 {
		t.Fail()
	}
}

func TestBigFileFlow(t *testing.T) {
	length := int64(1 << 20 + 10)
	randomBytes := common.GenerateRandomData(length)
	key, err := c.CreateAndWrite(randomBytes)
	if err != nil {
		t.Errorf("Create And Write Error - %s}", err.Error())
	}
	t.Logf("File Key : %v", key)
	keyList = append(keyList, key)
	keyMap[key] = randomBytes
	buffer := make([]byte, length)
	n, err := c.Read(key, 0, buffer)
	if err != nil {
		t.Errorf("Error - %s", err.Error())
	}
	t.Logf("Bytes: %v", n)
	result := bytes.Compare(buffer, randomBytes)
	t.Logf("buffer ~ %d | buffer/randomBytes ~ %v/%v", len(buffer), buffer[:10], randomBytes[:10])
	t.Logf("Compare Result - %d", result) // result need to be 0
	if result != 0 {
		t.Fail()
	}
}

func TestReadFile(t *testing.T) {
	for _, key := range keyList {
		buffer := make([]byte, len(keyMap[key]))
		n, _ := c.Read(key, 0, buffer)
		t.Logf("Bytes: %d", n)
		result := bytes.Compare(buffer, keyMap[key])
		t.Logf("Compare Result - %d", result) // result need to be 0
	}
}

func TestReadFileFromRandomOffset(t *testing.T) {
	offset := common.Offset(0)
	rand.Seed(int64(time.Now().Nanosecond()))
	for key, answer := range keyMap {
		offset = common.Offset(rand.Intn(len(answer) - 1))
		randomLength := offset + common.Offset(rand.Intn(len(answer) - int(offset) - 1)) + 1
		buffer := make([]byte, randomLength - offset)
		n, err := c.Read(key, offset, buffer)
		t.Logf("Offset: %d | Length: %d", offset, randomLength)
		if err != nil && n == -1 {
			t.Errorf("Error Occurred - %v", err.Error())
		}
		t.Logf("Bytes: %d", n)
		if bytes.Compare(buffer, answer[offset:randomLength]) == 0 {
			t.Logf("Corret For Key - %v", key)
		}
	}
}

func TestClient_ListKeys(t *testing.T) {
	_, err := c.ListKeys()
	if err != nil {
		t.Fail()
	}
	t.Log("List Key Ends - - -")
}

func TestClient_NodeStatus(t *testing.T) {
	_, err := c.NodeStatus()
	if err != nil {
		t.Fail()
	}
	t.Log("List Key Ends - - -")
}

func TestClient_Delete(t *testing.T) {
	// delete file for created in this test
	for k, _ := range keyMap {
		err := c.Delete(k)
		if err != nil {
			t.Errorf(err.Error())
		}
	}
}

//func Test_DeleteAll(t *testing.T) {
//	// delete all
//	err := c.ListKeys()
//	if err != nil {
//		t.Error("LIST ERROR")
//		t.Fail()
//	}
//}