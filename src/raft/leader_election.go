package raft

import (
	"math/rand"
	"sync"
	"time"
)

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.state == Leader
	return term, isleader
}

// resetElectionTimer 重置选举的超时时间点
func (rf *Raft) resetElectionTimer() {
	t := time.Now()
	electionTimeout := time.Duration(150+rand.Intn(150)) * time.Millisecond
	rf.electionTime = t.Add(electionTimeout)
}

func (rf *Raft) leaderElection() {
	DPrintf("%d服务器开始成为候选者\n", rf.me)
	rf.state = Candidate
	rf.currentTerm += 1 //新的term
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.log) - 1,
		LastLogTerm:  rf.log[len(rf.log)-1].LogTerm,
	}

	//先给自己投一票
	okSum := 1
	rf.votedFor = rf.me
	rf.persist() //投票信息要持久化
	rf.resetElectionTimer()
	var becomeLeader sync.Once //用sync.Once类型的变量保证切换为leader的方法只执行一次
	// 给其他的服务器发送投票RPC
	for i := 0; i < len(rf.nextIndex); i++ {
		if i == rf.me {
			continue
		}
		go rf.candidateRequestVote(i, &args, &okSum, &becomeLeader)
	}
}

//在选举过程更新raft实例的的currentTerm
func (rf *Raft) setNewTerm(term int) {
	if term > rf.currentTerm || rf.currentTerm == 0 {
		rf.state = Follower
		rf.currentTerm = term
		rf.votedFor = -1
		DPrintf("[%d]: set term %v\n", rf.me, rf.currentTerm)
		rf.persist()
	}

}
