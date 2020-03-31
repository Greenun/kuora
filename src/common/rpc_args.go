package common

import "time"

// chunk actions

type CreateChunkArgs struct {
	handle ChunkHandle
}

type CreateChunkResponse struct {
	errCode code
}

type WriteChunkArgs struct {

}

type WriteChunkResponse struct {

}

type ReadChunkArgs struct {

}

type ReadChunkResponse struct {

}

// master actions (heartbeat etc..)

type HeartBeatArgs struct {

}

type HeartBeatResponse struct {

}

// key-value based

type ListKeysArgs struct {
	limit int // how many files
}

type ListKeysResponse struct {
	keys []HotKey
}

type GetFilenameArgs struct {
	key HotKey
}

type GetFilenameResponse struct {
	filename Filename
}

//