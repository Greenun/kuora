package client

import (
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


