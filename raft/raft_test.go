package raft

import (
	"raftkv/utils"
	"strconv"
	"sync"
	"testing"
	"time"
)

func makeRaftCluster(n int, applyChs []chan ApplyMsg) []*Raft {
	rafts := make([]*Raft, n)
	peers := make([]*utils.ClientEnd, n)
	persisters := make([]*Persister, n)

	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			peers[i] = &utils.ClientEnd{}
		}(i)
	}
	wg.Wait()

	for i := 0; i < n; i++ {
		persister := MakePersister(i)
		persisters[i] = persister

		address := "127.0.0.1:" + strconv.Itoa(10002+i)
		clientEnd := &utils.ClientEnd{
			Addr:   address,
			Client: nil,
		}
		peers[i] = clientEnd
	}

	for i := 0; i < n; i++ {
		rafts[i] = MakeRaft(peers, i, persisters[i], applyChs[i])
	}
	return rafts
}

func TestInitialElection(t *testing.T) {
	const n = 3
	applyChs := make([]chan ApplyMsg, n)
	for i := range applyChs {
		applyChs[i] = make(chan ApplyMsg, 100)
	}
	rafts := makeRaftCluster(n, applyChs)

	time.Sleep(ElectionSuccess * time.Second)

	leaderCount := 0
	for i := 0; i < n; i++ {
		_, isLeader := rafts[i].GetState()
		if isLeader {
			leaderCount++
		}
	}
	if leaderCount != 1 {
		t.Fatalf("expected 1 leader, got %d", leaderCount)
	}

	term := -1
	for i := 0; i < n; i++ {
		currentTerm, _ := rafts[i].GetState()
		if term == -1 {
			term = currentTerm
		} else if term != currentTerm {
			t.Fatalf("terms not consistent")
		}
	}
}

func TestLogReplication(t *testing.T) {
	const n = 3
	applyChs := make([]chan ApplyMsg, n)
	for i := range applyChs {
		applyChs[i] = make(chan ApplyMsg, 100)
	}
	rafts := makeRaftCluster(n, applyChs)

	time.Sleep(1 * time.Second)

	var leader *Raft
	for i := 0; i < n; i++ {
		if _, isLeader := rafts[i].GetState(); isLeader {
			leader = rafts[i]
			break
		}
	}
	if leader == nil {
		t.Fatalf("no leader elected")
	}

	value := "test_command"
	leader.Start(value)

	time.Sleep(1 * time.Second)

	success := 0
	for i := 0; i < n; i++ {
		rafts[i].mu.Lock()
		logLen := len(rafts[i].logs)
		rafts[i].mu.Unlock()
		if logLen > 1 {
			success++
		}
	}
	if success <= n/2 {
		t.Fatalf("log not replicated to majority")
	}
}

func TestPersist(t *testing.T) {
	const n = 3
	applyChs := make([]chan ApplyMsg, n)
	for i := range applyChs {
		applyChs[i] = make(chan ApplyMsg, 100)
	}
	rafts := makeRaftCluster(n, applyChs)

	time.Sleep(1 * time.Second)

	var leader *Raft
	for i := 0; i < n; i++ {
		if _, isLeader := rafts[i].GetState(); isLeader {
			leader = rafts[i]
			break
		}
	}
	if leader == nil {
		t.Fatalf("no leader elected")
	}

	for i := 0; i < 5; i++ {
		leader.Start(i)
	}
	time.Sleep(1 * time.Second)

	index := (leader.me + 1) % n
	oldRaft := rafts[index]
	data := oldRaft.persister.ReadRaftState()

	newRaft := MakeRaft(oldRaft.peers, oldRaft.me, oldRaft.persister, applyChs[index])
	newRaft.readPersist(data)

	newRaft.mu.Lock()
	logLen := len(newRaft.logs)
	newRaft.mu.Unlock()
	if logLen <= 1 {
		t.Fatalf("logs not persisted")
	}
}

func TestLeaderFailure(t *testing.T) {
	const n = 3
	applyChs := make([]chan ApplyMsg, n)
	for i := range applyChs {
		applyChs[i] = make(chan ApplyMsg, 100)
	}
	rafts := makeRaftCluster(n, applyChs)

	time.Sleep(1 * time.Second)

	var leaderIndex int
	for i := 0; i < n; i++ {
		if _, isLeader := rafts[i].GetState(); isLeader {
			leaderIndex = i
			break
		}
	}
	rafts[leaderIndex].Kill()

	time.Sleep(2 * time.Second)

	newLeaderCount := 0
	for i := 0; i < n; i++ {
		if i == leaderIndex {
			continue
		}
		_, isLeader := rafts[i].GetState()
		if isLeader {
			newLeaderCount++
		}
	}
	if newLeaderCount != 1 {
		t.Fatalf("expected 1 new leader, got %d", newLeaderCount)
	}
}

func findLeader(rafts []*Raft) *Raft {
	for i := 0; i < len(rafts); i++ {
		if _, isLeader := rafts[i].GetState(); isLeader {
			return rafts[i]
		}
	}
	return nil
}

func checkLogConsistency(t *testing.T, rafts []*Raft) {
	leader := findLeader(rafts)
	if leader == nil {
		t.Fatal("no leader found")
	}

	leaderLog := leader.logs[1:]
	for i, r := range rafts {
		if r == leader {
			continue
		}
		followerLog := r.logs[1:]
		for j := range leaderLog {
			if j >= len(followerLog) {
				t.Fatalf("follower %d log too short", i)
			}
			if leaderLog[j].Term != followerLog[j].Term ||
				leaderLog[j].Command != followerLog[j].Command {
				t.Fatalf("log mismatch at index %d", j)
			}
		}
	}
}
