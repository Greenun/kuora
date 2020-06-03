package client

import (
	"bytes"
	"common"
	"testing"
)

var c = NewClient("127.0.0.1:40000")

func TestClient_Create(t *testing.T) {
	t.Log("Create Operation")
	c.Create(10)
	c.Create(1000)
	c.Create(common.BlockSize + 1)
}

//func TestClient_Read(t *testing.T) {
	//t.Log("Read Operation")
//}


func TestFlow(t *testing.T) {
	l1 := int64(10)
	//l2 := int64(1 << 12)
	//l3 := int64(1 << 20 + 1)

	randomBytes := common.GenerateRandomData(l1)
	t.Logf("Bytes: %v", randomBytes)
	key, err := c.CreateAndWrite(randomBytes)
	if err != nil {
		t.Errorf("Create And Write File Error - %s", err.Error())
	}
	t.Logf("File Key : %s", key)
	buffer := make([]byte, l1)
	data := make([]byte, l1)
	_, data, _ = c.Read(key, 0, buffer)
	t.Logf("Bytes: %v", data)
	result := bytes.Compare(buffer, randomBytes)
	t.Logf("Compare Result - %d", result) // result need to be 0
}