package raft

import (
	"bytes"
	"log"
	"math/rand"
	"raftkv/utils"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type LogEntry struct {
	Command interface{}
	Term    int
	Index   int
}

type CommandState struct {
	Term  int
	Index int
}

const (
	FOLLOWER = iota
	CANDIDATE
	LEADER
	CopyEntries
	HeartBeat
)

type Raft struct {
	mu        sync.Mutex
	peers     []*utils.ClientEnd
	persister *Persister
	me        int
	dead      int32

	currentTerm int
	votedFor    int
	logs        []LogEntry

	commitIndex int
	lastApplied int

	nextIndex []int

	identity     int
	serversNum   int
	heartBeatCnt int

	applyCh      chan ApplyMsg
	doAppendCh   chan int
	applyCmdLogs map[interface{}]*CommandState
}

func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool

	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	isleader = rf.identity == LEADER
	return term, isleader
}

func (rf *Raft) persist() {
	writer := new(bytes.Buffer)
	encoder := utils.NewEncoder(writer)

	func() {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		encoder.Encode(rf.votedFor)
		encoder.Encode(rf.currentTerm)
		encoder.Encode(rf.logs)
	}()

	data := writer.Bytes()
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}

	reader := bytes.NewBuffer(data)
	decoder := utils.NewDecoder(reader)
	var votedFor, currentTerm int
	var logs []LogEntry

	if err := decoder.Decode(&votedFor); err != nil {
		panic(err)
	}
	if err := decoder.Decode(&currentTerm); err != nil {
		panic(err)
	}
	if err := decoder.Decode(&logs); err != nil {
		panic(err)
	}

	rf.votedFor = votedFor
	rf.currentTerm = currentTerm
	rf.logs = logs
	for i := range rf.logs {
		logEntry := rf.logs[i]
		rf.applyCmdLogs[logEntry.Command] = &CommandState{
			Term:  logEntry.Term,
			Index: logEntry.Index,
		}
	}
}

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return nil
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.identity = FOLLOWER
	}

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		lastLog := rf.logs[len(rf.logs)-1]
		if args.LastLogTerm > lastLog.Term {
			rf.identity = FOLLOWER
			rf.votedFor = args.CandidateId
			rf.heartBeatCnt++
			reply.VoteGranted = true
		} else if args.LastLogTerm == lastLog.Term && args.LastLogIndex >= lastLog.Index {
			rf.identity = FOLLOWER
			rf.votedFor = args.CandidateId
			rf.heartBeatCnt++
			reply.VoteGranted = true
		}
	}

	go rf.persist()
	return nil
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term         int
	Success      bool
	PrevLogIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.PrevLogIndex = args.PrevLogIndex

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = args.LeaderId
		rf.identity = FOLLOWER
	}

	lenLogsCur := len(rf.logs)
	if args.Term < rf.currentTerm {
		return nil
	}

	if args.PrevLogIndex >= lenLogsCur || rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		if args.PrevLogIndex >= lenLogsCur {
			reply.PrevLogIndex = lenLogsCur
			return nil
		}

		index := args.PrevLogIndex
		term := rf.logs[index].Term
		for index--; index >= 0 && rf.logs[index].Term == term; index-- {
		}
		reply.PrevLogIndex = index + 1
		return nil
	}

	var deleteLogEntries []LogEntry
	idx1 := args.PrevLogIndex + 1
	idx2 := len(args.Entries) - 1
	for idx1 < lenLogsCur && idx2 >= 0 {
		log1 := &rf.logs[idx1]
		log2 := args.Entries[idx2]
		if log1.Term != log2.Term || log1.Index != log2.Index {
			deleteLogEntries = rf.logs[idx1:]
			rf.logs = rf.logs[:idx1]
			break
		}
		idx1++
		idx2--
	}
	for i := 0; i < len(deleteLogEntries); i++ {
		delete(rf.applyCmdLogs, deleteLogEntries[i].Command)
	}

	for idx2 >= 0 {
		logEntry := args.Entries[idx2]
		rf.logs = append(rf.logs, logEntry)
		rf.applyCmdLogs[logEntry.Command] = &CommandState{
			Term:  logEntry.Term,
			Index: logEntry.Index,
		}
		idx2--
	}

	if rf.commitIndex < args.LeaderCommit {
		idx := len(rf.logs) - 1
		if args.LeaderCommit <= idx {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = idx
		}

		go rf.apply()
	}

	rf.heartBeatCnt++
	reply.Success = true
	go rf.persist()
	return nil
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	if SimulateNetworkIssues() {
		ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
		return ok
	} else {
		return false
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	if SimulateNetworkIssues() {
		ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
		return ok
	} else {
		return false
	}
}

func SimulateNetworkIssues() bool {
	// 10%概率消息丢失
	if (rand.Int() % 1000) < 100 {
		log.Println("drop this message")
		return false
	} else if (rand.Int() % 1000) < 200 { // 10%概率消息长延迟但不超过electionTimeOut的一半
		ms := rand.Int63() % (int64(ElectionTimeout) / 2)
		log.Println("super delay")
		time.Sleep(time.Duration(ms) * time.Second)
	} else { // 80%概率延迟0~12ms
		ms := (rand.Int63() % 13)
		log.Println("normal delay")
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
	return true
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := -1
	isLeader := !rf.killed() && rf.identity == LEADER
	term := rf.currentTerm
	if isLeader {
		if commandState, has := rf.applyCmdLogs[command]; has {
			return commandState.Index, commandState.Term, isLeader
		}

		index = len(rf.logs)
		newLogEntry := LogEntry{
			Command: command,
			Term:    term,
			Index:   index,
		}
		rf.logs = append(rf.logs, newLogEntry)
		rf.applyCmdLogs[command] = &CommandState{
			Term:  term,
			Index: index,
		}
		go rf.persist()
		go func() {
			rf.doAppendCh <- CopyEntries
		}()
	}
	return index, term, isLeader
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

func (rf *Raft) killed() bool {
	dead := atomic.LoadInt32(&rf.dead)
	return dead == 1
}

func (rf *Raft) Restart() {
	atomic.StoreInt32(&rf.dead, 0)
	go func() {
		for {
			if rf.killed() {
				return
			}
			rf.mu.Lock()
			switch rf.identity {

			case FOLLOWER:
				oldHeartBeatCnt := rf.heartBeatCnt
				rf.mu.Unlock()
				timeout := time.Duration(randomTimeout(700, 1000)) * time.Millisecond
				time.Sleep(timeout)
				func() {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					if rf.heartBeatCnt == oldHeartBeatCnt {
						rf.identity = CANDIDATE
						rf.currentTerm++
						rf.votedFor = rf.me
						go rf.persist()
					}
				}()
			case CANDIDATE:
				rf.mu.Unlock()

				wonCh := make(chan int, 2)
				wg := sync.WaitGroup{}
				wg.Add(1)
				go func() {
					wg.Wait()
					close(wonCh)
				}()

				wg.Add(1)
				go rf.leaderElection(wonCh, &wg)

				timeout := time.Duration(randomTimeout(1000, 1400)) * time.Millisecond
				wg.Add(1)
				go func() {
					time.Sleep(timeout)
					wonCh <- ElectionTimeout
					wg.Done()
				}()

				result := <-wonCh
				if result == ElectionTimeout {
					rf.mu.Lock()
					rf.votedFor = rf.me
					rf.currentTerm++
					rf.mu.Unlock()
					go rf.persist()
				}
				wg.Done()
			default:
				rf.mu.Unlock()
				rf.doAppendCh <- HeartBeat
				time.Sleep(time.Millisecond * 100)
			}
		}
	}()

	go func() {
		for {
			if rf.killed() {
				return
			}
			rf.sendLogEntry(<-rf.doAppendCh)
		}
	}()
}

const (
	CopySuccess = -1
	CopyFailed  = -2
)

func (rf *Raft) sendLogEntry(signal int) {
	rf.mu.Lock()

	if rf.identity != LEADER {
		rf.mu.Unlock()
		return
	}

	argsBase := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		LeaderCommit: rf.commitIndex,
	}

	logLength := len(rf.logs)
	rf.mu.Unlock()

	resultCh := make(chan int, rf.serversNum)
	wg := sync.WaitGroup{}
	wg.Add(1)
	defer wg.Done()
	go func() {
		wg.Wait()
		close(resultCh)
	}()

	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			args := argsBase
			prevLogIndex := func() int {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				curIndex := logLength - 1
				for ; curIndex >= rf.nextIndex[i]; curIndex-- {
					args.Entries = append(args.Entries, rf.logs[curIndex])
				}
				return curIndex
			}()

			for prevLogIndex >= 0 {
				rf.mu.Lock()
				if rf.identity != LEADER || prevLogIndex >= len(rf.logs) {
					resultCh <- CopyFailed
					break
				}
				args.PrevLogIndex = prevLogIndex
				args.PrevLogTerm = rf.logs[prevLogIndex].Term
				rf.mu.Unlock()

				reply := AppendEntriesReply{}
				ok := rf.sendAppendEntries(i, &args, &reply)
				if !ok {
					resultCh <- CopyFailed
					break
				}
				if reply.Term > args.Term {
					resultCh <- reply.Term
					break
				}

				if reply.Success {
					rf.mu.Lock()
					rf.nextIndex[i] = logLength
					rf.mu.Unlock()
					resultCh <- CopySuccess
					break
				} else {
					rf.mu.Lock()
					if prevLogIndex >= len(rf.logs) {
						rf.mu.Unlock()
						break
					}

					rf.nextIndex[i] = reply.PrevLogIndex
					for ; prevLogIndex >= reply.PrevLogIndex; prevLogIndex-- {
						args.Entries = append(args.Entries, rf.logs[prevLogIndex])
					}
					rf.mu.Unlock()
				}
			}
		}(i)
	}

	copyCount := 1
	unCopyCount := 0
	target := rf.serversNum / 2

	for i := 1; i < rf.serversNum; i++ {
		result := <-resultCh
		rf.mu.Lock()
		if rf.identity != LEADER {
			rf.mu.Unlock()
			break
		}
		if rf.currentTerm != argsBase.Term {
			rf.mu.Unlock()
			break
		}
		rf.mu.Unlock()

		if result == CopySuccess {
			copyCount++
			if copyCount > target {
				func() {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					commit := logLength - 1
					if rf.identity == LEADER && commit < len(rf.logs) && commit > rf.commitIndex {
						rf.commitIndex = commit
						go rf.apply()
					}
				}()
				break
			}
		} else if result == CopyFailed {
			unCopyCount++
			if unCopyCount > target {
				func() {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					rf.identity = FOLLOWER
				}()
				break
			}
		} else if result > argsBase.Term {
			func() {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if rf.currentTerm < result {
					rf.currentTerm = result
					rf.votedFor = -1
					rf.identity = FOLLOWER
				}
			}()
			break
		} else {
			panic("Unexpected result: " + strconv.Itoa(result))
		}
	}
}

func (rf *Raft) setToFollower() {
	rf.identity = FOLLOWER
}

func (rf *Raft) apply() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.commitIndex < rf.lastApplied {
		panic("Error occur during raft commit, applied before commited")
	}
	if rf.commitIndex == rf.lastApplied {
		return
	}

	for rf.commitIndex > rf.lastApplied {
		rf.lastApplied++
		logEntry := rf.logs[rf.lastApplied]
		rf.applyCh <- ApplyMsg{
			CommandValid: true,
			Command:      logEntry.Command,
			CommandIndex: logEntry.Index,
		}
	}
}

func randomTimeout(min, max int) int {
	return rand.Intn(max-min) + min
}

const (
	ElectionSuccess = 1
	ElectionTimeout = 2
)

func MakeRaft(peers []*utils.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.votedFor = -1
	rf.serversNum = len(peers)
	rf.logs = append(rf.logs, LogEntry{
		Command: "start",
		Term:    0,
		Index:   0,
	})
	rf.setToFollower()
	rf.commitIndex = 0
	rf.nextIndex = make([]int, rf.serversNum)
	rf.applyCh = applyCh
	rf.doAppendCh = make(chan int, 256)
	rf.applyCmdLogs = make(map[interface{}]*CommandState)
	rand.Seed(time.Now().UnixNano())

	go func() {
		for {
			if rf.killed() {
				return
			}
			rf.mu.Lock()
			switch rf.identity {

			case FOLLOWER:
				oldHeartBeatCnt := rf.heartBeatCnt
				rf.mu.Unlock()
				timeout := time.Duration(randomTimeout(700, 1000)) * time.Millisecond
				time.Sleep(timeout)
				func() {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					if rf.heartBeatCnt == oldHeartBeatCnt {
						rf.identity = CANDIDATE
						rf.currentTerm++
						rf.votedFor = rf.me
						go rf.persist()
					}
				}()
			case CANDIDATE:
				rf.mu.Unlock()

				wonCh := make(chan int, 2)
				wg := sync.WaitGroup{}
				wg.Add(1)
				go func() {
					wg.Wait()
					close(wonCh)
				}()

				wg.Add(1)
				go rf.leaderElection(wonCh, &wg)

				timeout := time.Duration(randomTimeout(1000, 1400)) * time.Millisecond
				wg.Add(1)
				go func() {
					time.Sleep(timeout)
					wonCh <- ElectionTimeout
					wg.Done()
				}()

				result := <-wonCh
				if result == ElectionTimeout {
					rf.mu.Lock()
					rf.votedFor = rf.me
					rf.currentTerm++
					rf.mu.Unlock()
					go rf.persist()
				}
				wg.Done()
			default:
				rf.mu.Unlock()
				rf.doAppendCh <- HeartBeat
				time.Sleep(time.Millisecond * 100)
			}
		}
	}()

	go func() {
		for {
			if rf.killed() {
				return
			}
			rf.sendLogEntry(<-rf.doAppendCh)
		}
	}()

	rf.readPersist(persister.ReadRaftState())
	return rf
}

func (rf *Raft) leaderElection(wonCh chan int, wgp *sync.WaitGroup) {
	defer wgp.Done()

	args := &RequestVoteArgs{}
	args.CandidateId = rf.me

	rf.mu.Lock()
	curTerm := rf.currentTerm
	args.Term = rf.currentTerm
	args.LastLogIndex = len(rf.logs) - 1
	args.LastLogTerm = rf.logs[args.LastLogIndex].Term
	rf.mu.Unlock()

	ch := make(chan *RequestVoteReply, rf.serversNum)
	wg := sync.WaitGroup{}
	wg.Add(1)
	defer wg.Done()
	go func() {
		wg.Wait()
		close(ch)
	}()

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			reply := RequestVoteReply{}
			if ok := rf.sendRequestVote(i, args, &reply); !ok {
				return
			}
			ch <- &reply
		}(i)
	}

	grantedCount := 1
	unGrantedCount := 0
	target := rf.serversNum / 2
	for i := 1; i < rf.serversNum; i++ {
		reply := <-ch

		rf.mu.Lock()
		if rf.currentTerm != curTerm {
			rf.mu.Unlock()
			break
		}
		if rf.identity != CANDIDATE {
			rf.mu.Unlock()
			break
		}
		rf.mu.Unlock()

		if reply == nil {
			unGrantedCount++
			continue
		}
		if reply.VoteGranted {
			grantedCount++
			if grantedCount > target {
				func() {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					if rf.identity != LEADER {
						rf.identity = LEADER
						nextIndex := len(rf.logs)
						for i := range rf.nextIndex {
							rf.nextIndex[i] = nextIndex
						}
						rf.doAppendCh <- HeartBeat
					}
				}()
				wonCh <- ElectionSuccess
				break
			}
		} else {
			if args.Term < reply.Term {
				func() {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					if rf.currentTerm < reply.Term {
						rf.currentTerm = reply.Term
						rf.votedFor = -1
						rf.identity = FOLLOWER
						wonCh <- ElectionSuccess
					}
				}()
				break
			}

			unGrantedCount++
			if unGrantedCount > target {
				break
			}
		}
	}
}
