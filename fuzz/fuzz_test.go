package fuzz

import (
	"log"
	"math/rand"
	"raftkv/kv"
	"raftkv/raft"
	"raftkv/utils"
	"testing"
	"time"
)

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
	servers := 5
	clientEnds := kv.GetClientEnds("./config/client.yml")
	client := kv.MakeKVClient(clientEnds)

	rand.Seed(time.Now().UnixNano())

	f.Add("key", "value")
	f.Add("1sd12fsaw", "rnaoufngg2")
	f.Add("testkey", "testvalue")
	f.Add("randomkey", "randomvalue")

	// 设置随机种子，确保每次运行时会有不同的结果
	rand.Seed(time.Now().UnixNano())

	// 通过 f.Fuzz() 生成随机的 key 和 value，进行 Put 和 Get 操作
	f.Fuzz(func(t *testing.T, key string, value string) {
		// 跳过空的 key 或 value
		if len(key) == 0 || len(value) == 0 {
			t.Skip("Skipping empty key or value")
		}

		// 执行 Put 操作
		if simulateNetworkIssues() {
			client.Put(key, value)

			// 获取返回的值，并验证是否一致
			got := client.Get(key)
			if got != value {
				t.Errorf("For key %v, expected value %v, got %v", key, value, got)
			}
		} else {
			// 模拟消息丢失，跳过本次操作
			t.Logf("Message lost: Skipping Put for key %v", key)
		}

		// 模拟崩溃和重启
		if rand.Intn(10) < 2 { // 20% 概率模拟节点崩溃
			node := rand.Intn(len(clientEnds)) // 随机选择一个节点
			log.Printf("FuzzTest: Killing and restarting node %d", node)

			// 模拟崩溃
			killReply := &KillReply{}
			clientEnds[node].Client.Close()
			time.Sleep(time.Second)

			// 模拟重启
			clientEnds[node].Client = utils.TryConnect(clientEnds[node].Addr)
			time.Sleep(time.Second)
		}

		// 模拟Leader崩溃和重新选举
		if rand.Intn(10) < 1 { // 10% 概率模拟 Leader 崩溃
			leaderIdx := FindLeader(clientEnds)
			if leaderIdx != -1 {
				leader := clientEnds[leaderIdx]

				// 模拟 Leader 崩溃
				killReply := &KillReply{}
				leader.Call("KVServer.Kill", &KillArgs{}, killReply)
				log.Printf("Leader %d killed: %v", leaderIdx, killReply.IsDead)
				time.Sleep(500 * time.Millisecond) // 等待新的 Leader 选举出来

				// 确保新的 Leader 被选举
				newLeaderIdx := FindLeader(clientEnds)
				if newLeaderIdx == -1 {
					t.Fatalf("No new leader elected after leader failure")
				}
			}
		}

		// 模拟长时间运行时的操作延迟
		time.Sleep(time.Millisecond * time.Duration(rand.Intn(100)))
	})

}
