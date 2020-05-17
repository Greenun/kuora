package ipc

import (
	"common"
	"fmt"
	"net/rpc"
)

func Single(addr common.NodeAddress, procedure string, args interface{}, response interface{}) error{
	connection, cerr := rpc.Dial("tcp", string(addr))
	if cerr != nil {
		return cerr
	}

	defer connection.Close()

	rpcError := connection.Call(procedure, args, response)
	return rpcError
}

func Multi(addrs []common.NodeAddress, procedure string, args interface{}) []error {
	errors := make([]string, 0)
	errChan := make(chan error)
	//resultChan := make(chan interface{})

	for _, addr := range addrs {
		go func(address common.NodeAddress){
			errChan <- Single(address, procedure, args, nil)
		}(addr)
	}

	for _ = range addrs {
		if err := <- errChan; err != nil {
			errors = append(errors, err.Error())
		}
	}
	if errors != nil {
		errorList := make([]error, len(errors))
		for _, e := range errors {
			errorList = append(errorList, fmt.Errorf(e))
		}
		return errorList
	} else {
		return nil
	}
}