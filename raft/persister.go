package raft

import (
	"os"
	"strconv"
	"sync"
)

type Persister struct {
	me        int
	mu        sync.Mutex
	raftstate []byte
}

func MakePersister(me int) *Persister {
	persister := &Persister{}
	persister.me = me
	return persister
}

func (ps *Persister) SaveRaftState(state []byte) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	file := strconv.Itoa(ps.me) + ".log"
	ps.raftstate = state
	err := os.WriteFile(file, ps.raftstate, 0644)
	if err != nil {
		panic(err)
	}
}

func (ps *Persister) ReadRaftState() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	file := strconv.Itoa(ps.me) + ".log"
	if _, err := os.Stat(file); err == nil {
		raftState, err := os.ReadFile(file)
		if err != nil {
			panic(err)
		}
		ps.raftstate = raftState
		return ps.raftstate
	} else {
		return nil
	}
}
