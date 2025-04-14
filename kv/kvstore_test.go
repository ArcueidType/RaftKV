package kv

import (
	"log"
	"math/rand"
	"raftkv/raft"
	"raftkv/utils"
	"testing"
	"time"
)

const (
	ADDR1 = "127.0.0.1:10002"
	ADDR2 = "127.0.0.1:10003"
	ADDR3 = "127.0.0.1:10004"
	ADDR4 = "127.0.0.1:10005"
	ADDR5 = "127.0.0.1:10006"
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

func TestBasicPutGet(t *testing.T) {
	path := "../config/client.yml"
	clientEnds := GetClientEnds(path)

	//defer func() {
	//	for _, ce := range clientEnds {
	//		killReply := &KillReply{}
	//		ce.Call("KVServer.Kill", &KillArgs{}, killReply)
	//	}
	//}()

	client := MakeKVClient(clientEnds)

	key := "test_key"
	value := "test_value"

	client.Put(key, value)
	time.Sleep(500 * time.Millisecond)
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

func TestReElection(t *testing.T) {
	path := "../config/client.yml"
	clientEnds := GetClientEnds(path)
	//defer func() {
	//	for _, ce := range clientEnds {
	//		killReply := &KillReply{}
	//		ce.Call("KVServer.Kill", &KillArgs{}, killReply)
	//	}
	//}()

	leader_idx := FindLeader(clientEnds)
	if leader_idx == -1 {
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

func TestFollowerFailureRecovery(t *testing.T) {
	path := "../config/client.yml"
	clientEnds := GetClientEnds(path)

	// Step 1: Find the initial leader
	leader_idx := FindLeader(clientEnds)
	if leader_idx == -1 {
		t.Fatalf("No leader elected")
	}

	// Step 2: Create a client and submit a command
	client := MakeKVClient(clientEnds)
	client.Put("key1", "value1")

	// Step 3: Kill one of the followers (simulating failure)
	follower_idx := (leader_idx + 1) % len(clientEnds)
	follower := clientEnds[follower_idx]
	killReply := &KillReply{}
	follower.Call("KVServer.Kill", &KillArgs{}, killReply)
	log.Println("Follower killed:", killReply.IsDead)
	time.Sleep(500 * time.Millisecond) // wait for leader to handle failure

	client.Put("key2", "value2")
	got := client.Get("key2")
	if got != "value2" {
		t.Fatalf("After follower failure, Get() = %v, want %v", got, "value2")
	}

	// Step 6: Reconnect the follower and ensure the system stays consistent
	killReply_ := &KillReply{}
	follower.Call("KVServer.Restart", &KillArgs{}, killReply_)
	log.Println("Follower restarted:", !killReply_.IsDead)
	time.Sleep(500 * time.Millisecond)
	client.Put("key3", "value3")

	// Step 7: Ensure that the follower has the latest value after recovery
	got = client.Get("key3")
	if got != "value3" {
		t.Fatalf("After follower recovery, Get() = %v, want %v", got, "value3")
	}
}

func TestLogConsistency(t *testing.T) {
	path := "../config/client.yml"
	clientEnds := GetClientEnds(path)

	leader_idx := FindLeader(clientEnds)
	if leader_idx == -1 {
		t.Fatalf("No leader elected")
	}

	client := MakeKVClient(clientEnds)
	client.Put("key1", "value1")

	for i := 0; i < len(clientEnds); i++ {
		if clientEnds[i].Client != nil {
			value := client.Get("key1")
			if value != "value1" {
				t.Fatalf("Node %d has inconsistent data: expected 'value1', but got %v", i, value)
			}
		}
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
		Addr: ADDR3,
	}
	killReply := &KillReply{}

	clientEnd.Call("KVServer.Kill", &KillArgs{}, killReply)
	log.Println(killReply.IsDead)
}

func simulateNetworkIssues() bool {
	// 10%概率消息丢失
	if (rand.Int() % 1000) < 100 {
		return false
	} else if (rand.Int() % 1000) < 200 { // 10%概率消息长延迟但不超过electionTimeOut的一半
		ms := rand.Int63() % (int64(raft.ElectionTimeout) / 2)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	} else { // 80%概率延迟0~12ms
		ms := (rand.Int63() % 13)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
	return true
}

func FuzzTest(f *testing.F) {
	path := "../config/client.yml"
	clientEnds := GetClientEnds(path)
	client := MakeKVClient(clientEnds)

	f.Add("key", "value")
	f.Add("1sd12fsaw", "rnaoufngg2")
	f.Add("testkey", "testvalue")
	f.Add("randomkey", "randomvalue")

	rand.Seed(time.Now().UnixNano())

	f.Fuzz(func(t *testing.T, key string, value string) {
		if len(key) == 0 || len(value) == 0 {
			t.Skip("Skipping empty key or value")
		}

		// 执行 Put 操作
		if simulateNetworkIssues() {
			client.Put(key, value)

			got := client.Get(key)
			if got != value {
				t.Errorf("For key %v, expected value %v, got %v", key, value, got)
			}
		} else {
			t.Logf("Message lost: Skipping Put for key %v", key)
		}

		if rand.Intn(10) < 1 { // 10% 概率模拟节点崩溃
			node := rand.Intn(len(clientEnds))
			log.Printf("FuzzTest: Killing and restarting node %d", node)

			killReply := &KillReply{}
			clientEnds[node].Call("KVServer.Kill", &KillArgs{}, killReply)
			time.Sleep(time.Second)

			restartReply := &KillReply{}
			clientEnds[node].Call("KVServer.Restart", &KillArgs{}, restartReply)
			time.Sleep(time.Second)
		} else if rand.Intn(10) < 2 { // 10% 概率模拟 Leader 崩溃
			leaderIdx := FindLeader(clientEnds)
			if leaderIdx != -1 {
				leader := clientEnds[leaderIdx]

				killReply := &KillReply{}
				leader.Call("KVServer.Kill", &KillArgs{}, killReply)
				log.Printf("Leader %d killed: %v", leaderIdx, killReply.IsDead)
				time.Sleep(500 * time.Millisecond) // 等待新的 Leader 选举出来

				// 确保新的 Leader 被选举
				newLeaderIdx := FindLeader(clientEnds)
				if newLeaderIdx == -1 {
					t.Fatalf("No new leader elected after leader failure")
				}
				time.Sleep(1000 * time.Millisecond)

				restartReply := &KillReply{}
				leader.Call("KVServer.Restart", &KillArgs{}, restartReply)
				time.Sleep(time.Second)
			}
		}

		time.Sleep(time.Millisecond * time.Duration(rand.Intn(100)))
	})

}
