package kv

import (
	"log"
	"raftkv/raft"
	"raftkv/utils"
	"testing"
)

const (
	ADDR1 = "127.0.0.1:10002"
	ADDR2 = "127.0.0.1:10003"
	ADDR3 = "127.0.0.1:10004"
)

func initializeCluster(n int) ([]*utils.ClientEnd, []*KVServer, []*raft.Raft) {
	servers := make([]*KVServer, n)
	clientEnds := make([]*utils.ClientEnd, n)
	rafts := make([]*raft.Raft, n)

	cE1 := &utils.ClientEnd{
		Addr: ADDR1,
	}
	clientEnds[0] = cE1
	cE2 := &utils.ClientEnd{
		Addr: ADDR2,
	}
	clientEnds[1] = cE2
	cE3 := &utils.ClientEnd{
		Addr: ADDR3,
	}
	clientEnds[2] = cE3

	return clientEnds, servers, rafts
}

func TestBasicPutGet(t *testing.T) {
	path := "../config/client.yml"
	clientEnds := GetClientEnds(path)
	client := MakeKVClient(clientEnds)

	key := "test_key"
	value := "test_value"

	client.Put(key, value)
	got := client.Get(key)
	if got != value {
		t.Fatalf("Get() = %v, want %v", got, value)
	}

	client.Delete(key)
	got = client.Get(key)
	if got != NoKeyValue {
		t.Fatalf("After Delete(), Get() = %v, want %v", got, NoKeyValue)
	}
}

//func TestLeaderFailure(t *testing.T) {
//	servers, kvServers, _ := initializeCluster(3)
//	defer func() {
//		for _, kv := range kvServers {
//			kv.Kill()
//		}
//	}()
//
//	// 查找当前领导者
//	var leader *KVServer
//	for _, kv := range kvServers {
//		if _, isLeader := kv.rf.GetState(); isLeader {
//			leader = kv
//			break
//		}
//	}
//	if leader == nil {
//		t.Fatal("No leader elected")
//	}
//
//	// 客户端操作
//	client := MakeKVClient(servers)
//	client.Put("key1", "value1")
//
//	// 杀死领导者
//	leader.Kill()
//	time.Sleep(500 * time.Millisecond) // 等待重新选举
//
//	// 继续操作
//	client.Put("key2", "value2")
//	got := client.Get("key2")
//	if got != "value2" {
//		t.Fatalf("After leader failure, Get() = %v, want %v", got, "value2")
//	}
//}

func FuzzPutGet(f *testing.F) {
	path := "../config/client.yml"
	clientEnds := GetClientEnds(path)
	client := MakeKVClient(clientEnds)
	f.Add("key", "value")
	f.Add("1sd12fsaw", "rnaoufngg2")
	f.Fuzz(func(t *testing.T, key string, value string) {
		client.Put(key, value)
		v := client.Get(key)
		if v != value {
			t.Errorf("Get(%v) = %v, want %v", key, v, value)
		}
	})
}

func TestGetState(t *testing.T) {
	clientEnd := &utils.ClientEnd{
		Addr: ADDR3,
	}
	stateReply := &StateReply{}
	clientEnd.Call(RPCGetState, &StateArgs{}, stateReply)
	log.Println(stateReply.IsLeader, stateReply.Term)
}

func TestKill(t *testing.T) {
	clientEnd := &utils.ClientEnd{
		Addr: ADDR2,
	}
	killReply := &KillReply{}

	clientEnd.Call("KVServer.Kill", &KillArgs{}, killReply)
	log.Println(killReply.IsDead)
}
