package raft

import "sync"

func (rf *Raft) candidateRequestVote(serverId int, args *RequestVoteArgs, okSum *int, becomeLeader *sync.Once) {
	reply := RequestVoteReply{}
	ok := rf.sendRequestVote(serverId, args, &reply)
	if !ok {
		return
	}
	rf.mu.Lock() //接下来要比较返回的term和当前实例的term所以要加锁
	defer rf.mu.Unlock()
	//如果返回的term比实例本身的term还要大，说明当前leader已经过时
	if reply.Term > args.Term {
		DPrintf("[%d]: %d 在新的term，更新term，结束\n", rf.me, serverId)
		rf.setNewTerm(reply.Term) //这里面要把Leader状态改为Follower
		return
	}

	if reply.Term < args.Term {
		DPrintf("[%d]: %d 的term %d 已经失效，结束\n", rf.me, serverId, reply.Term)
		return
	}
	if !reply.VoteGranted {
		DPrintf("[%d]: %d 没有投给me，结束\n", rf.me, serverId)
		return
	}
	DPrintf("[%d]: from %d term一致，且投给%d\n", rf.me, serverId, rf.me)

	*okSum++
	if *okSum > len(rf.nextIndex)/2 && rf.currentTerm == args.Term && rf.state == Candidate {
		DPrintf(" [%d]: 当选为leader, term为%d\n", rf.me, args.Term)
		becomeLeader.Do(func() {
			rf.state = Leader
			//初始化两个数组
			lastLogIndex := args.LastLogIndex
			for i, _ := range rf.peers {
				rf.nextIndex[i] = lastLogIndex + 1
				rf.matchIndex[i] = 0
			}
			DPrintf("[%d]: leader - nextIndex %#v", rf.me, rf.nextIndex)
			//发送心跳
			rf.appendEntry(false)
		})
	}
}
