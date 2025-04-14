package kv

import uuid "github.com/satori/go.uuid"

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrLevelDB     = "ErrLevelDB"
	NoKeyValue     = ""
	RPCGet         = "KVServer.Get"
	RPCPut         = "KVServer.Put"
	RPCDelete      = "KVServer.Delete"
	RPCGetState    = "KVServer.IsLeader"
	OpPut          = "Put"
	OpGet          = "Get"
	OpDelete       = "Delete"
	CommitTimeout  = "CommitTimeout"
)

type Err string

type PutArgs struct {
	Key    string
	Value  string
	Id     uuid.UUID
	Serial uuid.UUID
}

type PutReply struct {
	Err Err
}

type GetArgs struct {
	Key    string
	Id     uuid.UUID
	Serial uuid.UUID
}

type GetReply struct {
	Err   Err
	Value string
}

type DeleteArgs struct {
	Key    string
	Id     uuid.UUID
	Serial uuid.UUID
}

type DeleteReply struct {
	Err Err
}

type StateArgs struct{}
type StateReply struct {
	Term     int
	IsLeader bool
}
