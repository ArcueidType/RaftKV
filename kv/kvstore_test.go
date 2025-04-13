package kv

import (
	"net/rpc"
	"raftkv/raft"
	"raftkv/utils"
	"strconv"
	"testing"
	"time"
)

func initializeCluster(n int) ([]*utils.ClientEnd, []*KVServer, []*raft.Raft) {
	servers := make([]*KVServer, n)
	peers := make([]*utils.ClientEnd, n)
	persisters := make([]*raft.Persister, n)
	rafts := make([]*raft.Raft, n)

	for i := 0; i < n; i++ {
		persister := raft.MakePersister(i)
		persisters[i] = persister

		address := "127.0.0.1:" + strconv.Itoa(10002+i)
		client := utils.TryConnect(address)
		clientEnd := &utils.ClientEnd{
			Addr:   address,
			Client: client,
		}
		peers[i] = clientEnd
	}

	for i := 0; i < n; i++ {
		clientEnds := make([]*utils.ClientEnd, n)
		for j := 0; j < n; j++ {
			if i != j {
				clientEnds[j] = peers[j]
			}
		}

		server := StartKVServer(clientEnds, i, persisters[i])
		servers[i] = server

		if err := rpc.Register(server); err != nil {
			panic(err)
		}
		rafts[i] = server.rf
	}
	return peers, servers, rafts
}

//func makeTestCluster(n int) ([]*utils.ClientEnd, []*KVServer) {
//	servers := make([]*utils.ClientEnd, n)
//	kvServers := make([]*KVServer, n)
//	persisters := make([]*raft.Persister, n)
//
//	for i := 0; i < n; i++ {
//		servers[i] = &utils.ClientEnd{}
//		persisters[i] = raft.MakePersister(n)
//	}
//
//	for i := 0; i < n; i++ {
//		kvServers[i] = StartKVServer(servers, i, persisters[i])
//
//	}
//
//	time.Sleep(1 * time.Second)
//	return servers, kvServers
//}

func TestBasicPutGet(t *testing.T) {
	peers, servers, _ := initializeCluster(3)
	defer func() {
		for _, kv := range servers {
			kv.Kill()
		}
	}()

	client := MakeKVClient(peers)

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
	servers, kvServers, _ := initializeCluster(3)
	defer func() {
		for _, kv := range kvServers {
			kv.Kill()
		}
	}()

	// 查找当前领导者
	var leader *KVServer
	for _, kv := range kvServers {
		if _, isLeader := kv.rf.GetState(); isLeader {
			leader = kv
			break
		}
	}
	if leader == nil {
		t.Fatal("No leader elected")
	}

	// 客户端操作
	client := MakeKVClient(servers)
	client.Put("key1", "value1")

	// 杀死领导者
	leader.Kill()
	time.Sleep(500 * time.Millisecond) // 等待重新选举

	// 继续操作
	client.Put("key2", "value2")
	got := client.Get("key2")
	if got != "value2" {
		t.Fatalf("After leader failure, Get() = %v, want %v", got, "value2")
	}
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
