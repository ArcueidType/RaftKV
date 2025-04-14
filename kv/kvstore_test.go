package kv

import (
	"log"
	"raftkv/utils"
	"testing"
	"time"
)

const (
	ADDR1 = "127.0.0.1:10002"
	ADDR2 = "127.0.0.1:10003"
	ADDR3 = "127.0.0.1:10004"
)

func FindLeader(clientEnds []*utils.ClientEnd) int {
	for i, end := range clientEnds {
		reply := &StateReply{}
		ok := end.Call(RPCGetState, &StateArgs{}, reply)
		if ok && reply.IsLeader {
			log.Printf("Leader found at index %d, address %s, term %d\n", i, end.Addr, reply.Term)
			return i
		}
	}
	log.Println("No leader found in the current cluster.")
	return -1
}

func initializeCluster(n int) []*utils.ClientEnd {
	clientEnds := make([]*utils.ClientEnd, n)
	//servers := make([]*KVServer, n)
	//rafts := make([]*raft.Raft, n)

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

	return clientEnds
}

func TestBasicPutGet(t *testing.T) {
	path := "../config/client.yml"
	clientEnds := GetClientEnds(path)

	defer func() {
		for _, ce := range clientEnds {
			killReply := &KillReply{}
			ce.Call("KVServer.Kill", &KillArgs{}, killReply)
		}
	}()

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

func TestLeaderFailure(t *testing.T) {
	path := "../config/client.yml"
	clientEnds := GetClientEnds(path)
	defer func() {
		for _, ce := range clientEnds {
			killReply := &KillReply{}
			ce.Call("KVServer.Kill", &KillArgs{}, killReply)
		}
	}()

	leader_idx := FindLeader(clientEnds)
	if leader_idx != -1 {
		t.Fatalf("No leader elected")
	}
	leader := clientEnds[leader_idx]

	client := MakeKVClient(clientEnds)
	client.Put("key1", "value1")

	killReply := &KillReply{}
	leader.Call("KVServer.Kill", &KillArgs{}, killReply)
	log.Println(killReply.IsDead)
	time.Sleep(500 * time.Millisecond)

	client.Put("key2", "value2")
	got := client.Get("key2")
	if got != "value2" {
		t.Fatalf("After leader failure, Get() = %v, want %v", got, "value2")
	}
}

func TestFollowerFailure(t *testing.T) {

}

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
		Addr: ADDR1,
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
