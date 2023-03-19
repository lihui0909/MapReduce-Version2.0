package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Index   int
	LogTerm int
	Command interface{}
}

type RequestAppendEntryArgs struct {
	Term           int        // leader的任期
	LeaderId       int        // 为了让follower能够将客户端请求转发给leader
	PrevLogIndex   int        // 紧跟在新日志条目之前的日志条目索引
	PrevLogTerm    int        // PrevLogIndex日志条目的任期
	Entries        []LogEntry // 要保存的日志条目（如果是心跳信息为空，为了效率可能一次会发送多条）
	LeaderCommit   int        // leader的commitIndex
	HasLogsRequest bool       // 如果为true表示包括日志，为false没有日志表示心跳信息。此处不用Entries是否为空判断是因为传输过程可能会丢失entries
}

type RequestAppendEntryReply struct {
	Term     int  // 当前term，为了让leader更新它自己的
	Success  bool // 如果follower包括符合PrevLogIndex和PrevLogTerm的日志返回true
	Conflict bool // 表示是否日志冲突
}

func (rf *Raft) RequestAppendEntry(args *RequestAppendEntryArgs, reply *RequestAppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[%d]： 在 (term %d) follower 收到 leader[%v] RequestAppendEntry请求： %v, prevIndex是 %v, prevTerm是 %v", rf.me, rf.currentTerm, args.LeaderId, args.Entries, args.PrevLogIndex, args.PrevLogTerm)
	reply.Success = false
	reply.Conflict = false
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	rf.resetElectionTimer()
	//if rf.state == Candidate {
	rf.state = Follower
	//}
	reply.Term = rf.currentTerm
	if !args.HasLogsRequest { //说明收到的是心跳信息,直接返回即可
		reply.Success = true
		return
	} else { // 收到的是日志同步信息
		// Figure 2: Reply false if log doesn’t contain an entry at prevLogIndex
		//whose term matches prevLogTerm (§5.3)
		// 1.判断本地日志是否有PrevLogIndex下标的这一条日志
		if len(rf.log) <= args.PrevLogIndex {
			reply.Success = false
			return
		}
		// 若PrevLogIndex为-1，说明当前index为0起始位置，不用再判断前面index,直接写当前index的entry即可
		if args.PrevLogIndex == -1 {
			newEntryIdx := args.PrevLogIndex + 1
			rf.log = append(rf.log, args.Entries[newEntryIdx])
			rf.commitIndex = min(args.LeaderCommit, newEntryIdx)
			reply.Success = true
			return
		}

		// 2.如果有，判断若不相等，需要删除本地地址与参数中冲突的entry
		if rf.log[args.PrevLogIndex].LogTerm != args.PrevLogTerm {
			reply.Success = false
			reply.Conflict = true
			// 3.删除冲突下标本地的entry和之后的entry
			copy(rf.log, rf.log[:args.PrevLogIndex-1])
			return
		}

		// Figure 2 :Append any new entries not already in the log
		// 4.如果前一个目录项term相等，则将参数中的新目录项加到本地
		newEntryIdx := args.PrevLogIndex + 1
		rf.log = append(rf.log, args.Entries[newEntryIdx])
		//rf.log[newEntryIdx] = args.Entries[newEntryIdx]

		// Figure 2 : If leaderCommit > commitIndex, set commitIndex =
		//min(leaderCommit, index of last new entry)
		// 5.维护本地commitIndex
		rf.commitIndex = min(args.LeaderCommit, newEntryIdx)

	}
}

func (rf *Raft) sendRequestAppendEntry(serverId int, args *RequestAppendEntryArgs, reply *RequestAppendEntryReply) bool { // server是发送投票请求的目的实例id
	ok := rf.peers[serverId].Call("Raft.RequestAppendEntry", args, reply)
	return ok
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm       int           //当前任期
	heartBeatInterval time.Duration //心跳时间间隔
	state             RaftState     // 0表示为跟随者 1为候选者 2为leader
	votedFor          int           // 为哪个实例投票
	log               []LogEntry    // 同步的日志
	commitIndex       int           // 提交日志的index
	lastApplied       int           // 已应用日志的index
	nextIndex         []int         // 所有实例下一个需要同步的日志
	matchIndex        []int         // 所有实例匹配的日志
	//lastResponse      time.Time     // 上一次收到leader响应的时间（用于管理选举超时）
	electionTime time.Time //选举超时时间，是一个未来的时间点
}

type RaftState int

const (
	Follower = iota
	Candidate
	Leader
)

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct { //申请投票的RPC请求
	// Your data here (2A, 2B).
	Term         int //候选者任期
	CandidateId  int
	LastLogIndex int // 候选者的最后一条日志索引
	LastLogTerm  int //  候选者的最后一条日志任期
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // 处理请求节点的任期号，用于候选者更新自己的任期
	VoteGranted bool // 候选者获得选票为true，否则为false
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	voteTerm := args.Term
	candidateId := args.CandidateId
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	if voteTerm < rf.currentTerm {
		return
	}
	// 收到的请求比当前实例的任期大，更新自己的currentTerm,并设为非领导。但此时并不代表就会投票给候选者，还需要判断日志
	if voteTerm > rf.currentTerm {
		rf.currentTerm = voteTerm
		rf.votedFor = -1
	}
	// 在任期没问题的情况下，进一步判断日志来决定是否投票给候选者
	if rf.votedFor == -1 || rf.votedFor == candidateId {
		lastIndex := len(rf.log) - 1 //当前实例log日志最后一条的下标
		// 比较当前实例rf日志最后一条log entry和候选者的，如果rf的任期大或任期相同但rf的index大，则返回，不投票给候选者
		if rf.log[lastIndex].LogTerm > args.LastLogTerm || (rf.log[lastIndex].LogTerm == args.LastLogTerm && rf.log[lastIndex].Index > args.LastLogIndex) {
			return
		}

		rf.votedFor = candidateId
		rf.state = Follower
		reply.VoteGranted = true
		reply.Term = rf.currentTerm
		rf.resetElectionTimer()
		rf.persist()
		DPrintf("当前server:%d 投票给%d，任期为：%d\n", rf.me, candidateId, voteTerm)
	}
	return
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool { // server是发送投票请求的目的实例id
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
// Start 添加一条日志项到本地，并通过AppendEntries RPCs发送新的日志项
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 判断当前实例是否为leader
	if rf.state != Leader {
		return -1, rf.currentTerm, false
	}
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	index = len(rf.log)
	term = rf.currentTerm
	newEntry := LogEntry{
		index,
		term,
		command,
	}
	rf.log = append(rf.log, newEntry)
	rf.persist()
	DPrintf("[%v]: term %v 调用Start方法，添加了一个日志entry： %v", rf.me, term, newEntry)
	//rf.appendEntry(true) 修改start中不再维护日志一致性

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
//在该协程中周期性的检查lastResponse,如果超时，触发选举
func (rf *Raft) ticker() {
	for rf.killed() == false {
		//间隔心跳周期检查状态
		time.Sleep(rf.heartBeatInterval)
		rf.mu.Lock()

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		if rf.state == Leader { //如果当前实例是Leader,发送心跳信息 修改-周期性发送心跳并且日志同步
			rf.appendEntry(false)
		}

		//timeNoResponse := time.Now().Sub(rf.lastResponse)
		//// 如果当前实例间隔electionTimeout ms以内没有收到心跳，则开始选举流程(这里每个实例都选择的不同值，防止同时发起选举）
		//var electionTimeout int64 = (int64)(rand.Intn(100) + 300)
		if time.Now().After(rf.electionTime) {
			rf.leaderElection()
		}
		rf.mu.Unlock()
	}
}

//appendEntry Leader用该方法发送日志或心跳信息，hasLogs为true表示是日志
// appendEntry
func (rf *Raft) appendEntry(hasLogs bool) {
	lastLog := rf.log[len(rf.log)-1]
	for peer, _ := range rf.peers {
		if peer == rf.me {
			rf.resetElectionTimer()
			continue
		}

		//发送请求的情况：leader最后一个日志的index不小于nextIndex数组中该实例的index，或者为心跳信息
		if lastLog.Index >= rf.nextIndex[peer] || !hasLogs {
			curNextIdx := rf.nextIndex[peer]
			if curNextIdx <= 0 {
				curNextIdx = 1
			}
			if lastLog.Index+1 < curNextIdx {
				curNextIdx = lastLog.Index
			}
			curPreLog := rf.log[curNextIdx-1]
			args := RequestAppendEntryArgs{
				Term:           rf.currentTerm,
				LeaderId:       rf.me,
				PrevLogIndex:   curPreLog.Index,
				PrevLogTerm:    curPreLog.LogTerm,
				Entries:        make([]LogEntry, len(rf.log)),
				HasLogsRequest: hasLogs,
			}
			copy(args.Entries, rf.log)
			reply := RequestAppendEntryReply{}
			go rf.leaderSendEntries(peer, &args, &reply)
		}

	}
}

//leader发送心跳，按照lab说明，使用一个单独长时间运行的协程（心跳信息不会大于每秒10次）
//leader 给follower同步日志，用的同一个方法
func (rf *Raft) leaderSendEntries(serverId int, args *RequestAppendEntryArgs, reply *RequestAppendEntryReply) {
	ok := rf.sendRequestAppendEntry(serverId, args, reply)
	if !ok {
		DPrintf("[leaderSendEntries]sendRequestAppendEntry failed, leader id is : %d, term is : %d, target server id is : %d\n", rf.me, rf.currentTerm, serverId)
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 返回的term大于当前leader到的term
	if reply.Term > rf.currentTerm {
		//rf.currentTerm = reply.Term
		rf.setNewTerm(reply.Term)
		return
	}

	//说明是个过期的请求
	if args.Term != rf.currentTerm {
		return
	}

	// 日志entry添加成功,更新nextIndex和matchIndex数组记录
	if reply.Success {
		match := len(args.Entries)
		next := match + 1
		rf.nextIndex[serverId] = max(rf.nextIndex[serverId], next)
		rf.matchIndex[serverId] = max(rf.matchIndex[serverId], match)
		DPrintf("[%v]: follower实例 %v添加日志项成功 next： %v match： %v\n", rf.me, serverId, rf.nextIndex[serverId], rf.matchIndex[serverId])
	}
	curLogIdx := args.PrevLogIndex + 1

	// 返回的值为false并且存在冲突的情况，说明follower的日志比leader少，至少之前发的前一条日志entry没有或不相符
	for reply.Success == false && reply.Conflict {
		DPrintf("[%d]: 发送AppendEntry请求给 %d,返回值为false，index 为 %d的entry不一致 ", rf.me, serverId, args.PrevLogIndex)
		//curLogIdx := args.PrevLogIndex
		var preLogIdx int = -1
		var preLogTerm int = 0
		if args.PrevLogIndex-1 >= 0 {
			preLogIdx = args.PrevLogIndex - 1
			preLogTerm = rf.log[preLogIdx].LogTerm
		}

		args = &RequestAppendEntryArgs{
			Term:           rf.currentTerm,
			LeaderId:       rf.me,
			HasLogsRequest: true,
			PrevLogIndex:   preLogIdx,
			PrevLogTerm:    preLogTerm,
			Entries:        args.Entries,
			LeaderCommit:   rf.commitIndex,
		}
		reply = &RequestAppendEntryReply{}
		ok := rf.sendRequestAppendEntry(serverId, args, reply)
		if !ok {
			DPrintf("[leaderSendEntries]sendRequestAppendEntry failed(check阶段), leader id is : %d, term is : %d, target server id is : %d, entry index为: %d\n", rf.me, rf.currentTerm, serverId, args.PrevLogIndex+1)
			return
		}
	}

	if reply.Success {
		match := len(args.Entries)
		next := match + 1
		rf.nextIndex[serverId] = max(rf.nextIndex[serverId], next)
		rf.matchIndex[serverId] = max(rf.matchIndex[serverId], match)
		DPrintf("[%v]:  follower实例: %v添加日志项成功 next： %v match： %v\n", rf.me, serverId, rf.nextIndex[serverId], rf.matchIndex[serverId])
	}

	for idx := args.PrevLogIndex + 1; idx <= curLogIdx; idx++ {
		var preLogIdx int = -1
		var preLogTerm int = 0
		if args.PrevLogIndex-1 >= 0 {
			preLogIdx = args.PrevLogIndex - 1
			preLogTerm = rf.log[preLogIdx].LogTerm
		}
		//preLogIdx := idx - 1
		//preLogTerm := rf.log[preLogIdx].LogTerm
		args = &RequestAppendEntryArgs{
			Term:           rf.currentTerm,
			LeaderId:       rf.me,
			HasLogsRequest: true,
			PrevLogIndex:   preLogIdx,
			PrevLogTerm:    preLogTerm,
			Entries:        args.Entries,
			LeaderCommit:   rf.commitIndex,
		}
		reply = &RequestAppendEntryReply{}
		ok := rf.sendRequestAppendEntry(serverId, args, reply)
		if !ok {
			DPrintf("[leaderSendEntries]sendRequestAppendEntry failed(添加日志阶段), leader id is : %d, term is : %d, target server id is : %d, entry index为: %d\n", rf.me, rf.currentTerm, serverId, args.PrevLogIndex+1)
			return
		}

		if reply.Success {
			match := len(args.Entries)
			next := match + 1
			rf.nextIndex[serverId] = max(rf.nextIndex[serverId], next)
			rf.matchIndex[serverId] = max(rf.matchIndex[serverId], match)
			DPrintf("[%v]: follower实例: %v添加日志项成功 next： %v match： %v\n", rf.me, serverId, rf.nextIndex[serverId], rf.matchIndex[serverId])
		}
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.state = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.resetElectionTimer()
	rf.heartBeatInterval = 50 * time.Millisecond

	n := len(peers)
	rf.nextIndex = make([]int, n)
	rf.log = make([]LogEntry, 0)
	rf.log = append(rf.log, LogEntry{0, 0, -1}) //增加第一条日志，此处是为了防止发送请求投票的RPC请求时，传入的最后一个日志下标不合法
	rf.matchIndex = make([]int, n)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
