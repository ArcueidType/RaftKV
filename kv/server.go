package kv

import (
	"errors"
	uuid "github.com/satori/go.uuid"
	"github.com/syndtr/goleveldb/leveldb"
	"net/rpc"
	"raftkv/raft"
	"raftkv/utils"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

type Op struct {
	Type   string
	Key    string
	Value  string
	Serial uuid.UUID
}

type CommonReply struct {
	Err    Err
	Key    string
	Value  string
	Serial *uuid.UUID
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32

	leveldb       *leveldb.DB
	commonReplies []*CommonReply
}

func (kv *KVServer) findReply(op *Op, idx int, reply *CommonReply) string {
	t0 := time.Now()
	for time.Since(t0).Seconds() < 2 {
		kv.mu.Lock()
		if len(kv.commonReplies) > idx {
			if op.Serial == *kv.commonReplies[idx].Serial {
				replyTmp := kv.commonReplies[idx]
				reply.Err = replyTmp.Err
				reply.Key = replyTmp.Key
				reply.Value = replyTmp.Value
				kv.mu.Unlock()
				return OK
			} else {
				kv.mu.Unlock()
				return CommitTimeout
			}
		}
		kv.mu.Unlock()
		time.Sleep(20 * time.Millisecond)
	}
	return CommitTimeout
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) error {
	op := &Op{
		Type:   OpGet,
		Key:    args.Key,
		Value:  NoKeyValue,
		Serial: args.Serial,
	}
	reply.Err = ErrWrongLeader
	idx, _, isLeader := kv.rf.Start(*op)
	if !isLeader {
		return nil
	}

	commonReply := &CommonReply{}
	s := kv.findReply(op, idx, commonReply)
	if s == OK {
		reply.Value = commonReply.Value
		reply.Err = commonReply.Err
	}
	return nil
}

func (kv *KVServer) Put(args *PutArgs, reply *PutReply) error {
	op := &Op{
		Type:   OpPut,
		Key:    args.Key,
		Value:  args.Value,
		Serial: args.Serial,
	}
	reply.Err = ErrWrongLeader
	idx, _, isLeader := kv.rf.Start(*op)
	if !isLeader {
		return nil
	}

	commonReply := &CommonReply{}
	s := kv.findReply(op, idx, commonReply)
	if s == OK {
		reply.Err = commonReply.Err
	}
	return nil
}

func (kv *KVServer) Delete(args *DeleteArgs, reply *DeleteReply) error {
	op := &Op{
		Type:   OpDelete,
		Key:    args.Key,
		Value:  NoKeyValue,
		Serial: args.Serial,
	}
	reply.Err = ErrWrongLeader
	idx, _, isLeader := kv.rf.Start(*op)
	if !isLeader {
		return nil
	}

	commonReply := &CommonReply{}
	s := kv.findReply(op, idx, commonReply)
	if s == OK {
		reply.Err = commonReply.Err
	}
	return nil
}

func (kv *KVServer) IsLeader(args *StateArgs, reply *StateReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	term, isLeader := kv.rf.GetState()
	reply.Term = term
	reply.IsLeader = isLeader
	return nil
}

func (kv *KVServer) Kill(args *KillArgs, reply *KillReply) error {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	//if err := kv.leveldb.Close(); err != nil {
	//	panic(err)
	//}
	reply.IsDead = kv.killed()
	return nil
}

func (kv *KVServer) killed() bool {
	dead := atomic.LoadInt32(&kv.dead)
	return dead == 1
}

func (kv *KVServer) Restart(args *KillArgs, reply *KillReply) error {
	atomic.StoreInt32(&kv.dead, 0)
	kv.rf.Restart()
	//db, err := leveldb.OpenFile("./db"+strconv.Itoa(kv.me), nil)
	//if err != nil {
	//	panic(err)
	//}
	//kv.leveldb = db
	//log.Println("server dead", kv.killed())
	reply.IsDead = kv.killed()
	return nil
}

func StartKVServer(servers []*utils.ClientEnd, me int, persister *raft.Persister) *KVServer {
	utils.Register(Op{})

	kv := new(KVServer)
	kv.me = me

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.MakeRaft(servers, me, persister, kv.applyCh)
	db, err := leveldb.OpenFile("./db"+strconv.Itoa(me), nil)
	if err != nil {
		panic(err)
	}
	kv.leveldb = db
	kv.commonReplies = make([]*CommonReply, 1)

	if err := rpc.Register(kv.rf); err != nil {
		panic(err)
	}

	go kv.opHandler()
	return kv
}

func (kv *KVServer) opHandler() {
	for {
		if kv.killed() {
			continue
		}

		applyMsg := <-kv.applyCh
		op := applyMsg.Command.(Op)
		reply := &CommonReply{
			Key:    op.Key,
			Value:  op.Value,
			Serial: &op.Serial,
		}

		if op.Type == OpPut {
			err := kv.leveldb.Put([]byte(op.Key), []byte(op.Value), nil)
			if err != nil {
				reply.Err = ErrLevelDB
			} else {
				reply.Err = OK
			}
		} else if op.Type == OpGet {
			value, err := kv.leveldb.Get([]byte(op.Key), nil)
			if err != nil {
				if errors.Is(err, leveldb.ErrNotFound) {
					reply.Err = ErrNoKey
				} else {
					reply.Err = ErrLevelDB
				}
			} else {
				reply.Value = string(value)
				reply.Err = OK
			}
		} else if op.Type == OpDelete {
			err := kv.leveldb.Delete([]byte(op.Key), nil)
			if err != nil {
				reply.Err = ErrLevelDB
			} else {
				reply.Err = OK
			}
		} else {
			panic("Unknown op type")
		}

		func() {
			kv.mu.Lock()
			defer kv.mu.Unlock()
			kv.commonReplies = append(kv.commonReplies, reply)
		}()
	}
}
