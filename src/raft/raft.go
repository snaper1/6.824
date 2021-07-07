/*
 * @Description:
 * @User: Snaper <532990528@qq.com>
 * @Date: 2021-06-16 12:25:21
 * @LastEditTime: 2021-07-07 14:57:51
 */

package raft

import (
	//	"bytes"

	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
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

type Log struct {
	Term    int
	Index   int
	Command interface{}
}

//raft struct
type Raft struct {
	me             int   // this peer's index into peers[]
	dead           int32 // set by Kill()
	voteCount      int32 //投票数量
	heartBeatTime  int64
	heartBeatState int32
	peerCount      int
	mu             sync.Mutex          // Lock to protect shared access to this peer's state
	peers          []*labrpc.ClientEnd // RPC end points of all peers
	persister      *Persister          // Object to hold this peer's persisted state
	applyCh        chan ApplyMsg       //用于通知test（server）新的commited log，不需要主动调用

	//need persist
	state       int32 //节点状态
	currentTerm int32
	voteFor     int32 //投票给谁
	logs        []Log //该节点存储的log（command）

	commitIndex int   //该节点，即将要的最后一个索引
	lastApplied int   //该节点，已提交的最后一个索引
	nextIndex   []int //各个节点即将发送的索引
	matchIndex  []int //各个节点以及被复制的最后一条日志的索引

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
}

/**
 * @name: IsState
 * @desc: 判断rf是否状态为state，使用atomic 并发安全
 * @param {int32} state
 * @return {*}
 */
func (rf *Raft) IsState(state int32) bool {
	return atomic.LoadInt32(&rf.state) == state
}

func (rf *Raft) ChangeState(state int32) {
	atomic.StoreInt32(&rf.state, state)
}

func (rf *Raft) IsHeartBeatDead() bool {
	return atomic.LoadInt32(&rf.heartBeatState) == HEARTBEAT_DEAD
}

func (rf *Raft) GetState() (int, bool) {
	return int(atomic.LoadInt32(&rf.currentTerm)), rf.IsState(LEADER)
}

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
// RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	Term         int32
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	Term        int32
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int32
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Log
	LeaderCommit int
}
type AppendEntriesRply struct {
	Term    int32
	Success bool
}

//
// RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	voteFor := atomic.LoadInt32(&rf.voteFor)
	state := atomic.LoadInt32(&rf.state)
	curTerm := atomic.LoadInt32(&rf.currentTerm)
	reply.Term = curTerm
	reply.VoteGranted = false
	switch state {
	case LEADER:
		return
	case CANDIDATE:

		if args.Term > curTerm {
			rf.resetPeer()
			atomic.StoreInt32(&rf.currentTerm, args.Term)
			atomic.StoreInt32(&rf.voteFor, int32(args.CandidateId))
			reply.Term = curTerm
			reply.VoteGranted = true

		}
		return
	case FOLLOWER:
		if voteFor == -1 && args.Term > curTerm {
			atomic.StoreInt32(&rf.currentTerm, args.Term)
			atomic.StoreInt32(&rf.voteFor, int32(args.CandidateId))
			reply.Term = curTerm
			reply.VoteGranted = true
		}
		return
	}

}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

/**
 * @name:
 * @desc:将新的日志 提交到server中
 * @param {*}
 * @return {*}
 */
func (rf *Raft) applyToServer() {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	for rf.lastApplied < rf.commitIndex {
		rf.lastApplied++
		msg := ApplyMsg{
			Command:      rf.logs[rf.lastApplied].Command,
			CommandValid: true,
			CommandIndex: rf.lastApplied,
		}
		rf.applyCh <- msg
	}

}

/**
 * @name:
 * @desc:
 * @param {*AppendEntriesArgs} args
 * @param {*AppendEntriesRply} reply
 * @return {*}
 */
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesRply) {
	curTerm := atomic.LoadInt32(&rf.currentTerm)
	reply.Success = false
	reply.Term = curTerm
	if args.Term < curTerm {
		return
	} else if args.Term > curTerm {
		atomic.StoreInt32(&rf.currentTerm, args.Term)
		rf.resetPeer()
		return
	}
	atomic.StoreInt64(&rf.heartBeatTime, time.Now().UnixNano()/1e6)
	atomic.StoreInt32(&rf.currentTerm, args.Term)
	rf.mu.Lock()

	if args.PrevLogIndex >= len(rf.logs) || args.PrevLogTerm != rf.logs[args.PrevLogIndex].Term {
		reply.Term = args.Term
		return
	}
	appendLen := len(args.Entries)
	if args.PrevLogIndex+appendLen > len(rf.logs)-1 { //如果复制的日志本地没有，直接追加存储
		rf.logs = append(rf.logs[0:args.PrevLogIndex+1], args.Entries...)
	} else {
		for i := args.PrevLogIndex + 1; i < args.PrevLogIndex+1+appendLen; i++ { //检查日志任期
			j := i - args.PrevLogIndex - 1
			if args.Entries[j].Term != rf.logs[i].Term { //如果已存在的日志和prevlogTerm不相同，则删除之后的日志
				rf.logs = append(rf.logs[0:i], args.Entries[j:]...)
				break
			}
		}
	}

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = args.LeaderCommit
		rf.applyToServer()
	}
	rf.mu.Unlock()

	reply.Success = true

}
func (rf *Raft) SendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesRply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

/**
 * @name: Start
 * @desc: server use this func to append command to the leader log
 * @param {*}
 * @return {*}
 */

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := false
	if rf.IsState(LEADER) {
		term = int(atomic.LoadInt32(&rf.currentTerm))
		rf.mu.Lock()
		index = len(rf.logs) - 1
		log := Log{
			Term:    term,
			Command: command,
			Index:   index,
		}
		rf.logs = append(rf.logs, log)
		rf.mu.Unlock()

	}

	return index, term, isLeader
}

/**
 * @name:
 * @desc:
 * @param {*}
 * @return {*}
 */
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

/**
 * @name: Leading
 * @desc:给节点发送appendEntries，并处理回报
 * @param {int} server
 * @return {*}
 */
func (rf *Raft) Leading(server int) {
	curTerm := atomic.LoadInt32(&rf.currentTerm)
	if !rf.IsState(LEADER) || rf.killed() {
		return
	}
	rf.mu.Lock()
	args := AppendEntriesArgs{
		Term:         curTerm,
		LeaderId:     rf.me,
		Entries:      rf.logs[rf.nextIndex[server]:],
		PrevLogIndex: rf.logs[rf.nextIndex[server]-1].Index,
		PrevLogTerm:  rf.logs[rf.nextIndex[server]-1].Term,
		LeaderCommit: rf.commitIndex,
	}
	rf.mu.Unlock()
	rep := AppendEntriesRply{}
	rf.SendAppendEntries(server, &args, &rep)
	rf.mu.Lock()
	if rep.Success == false && rep.Term > curTerm {
		rf.resetPeer()
		return
	} else if rep.Success == false && rep.Term <= curTerm { //如果当前prevlog和server不匹配
		if rf.nextIndex[server] > 1 {
			rf.nextIndex[server]--
		}
	} else { //添加成功
		rf.nextIndex[server] += len(args.Entries)
		//rf.matchIndex[server]=

	}
	rf.mu.Unlock()

}

/**
 * @name: voting
 * @desc:
 * @param {*}
 * @return {*}
 */
func (rf *Raft) voting() {

	if rf.IsState(FOLLOWER) {
		return
	}
	atomic.StoreInt32(&rf.voteFor, int32(rf.me))
	atomic.AddInt32(&rf.currentTerm, 1)
	atomic.AddInt32(&rf.voteCount, 1)
	for server := 0; server < rf.peerCount; server++ {
		if !rf.IsState(CANDIDATE) {
			break
		}
		if server == rf.me {
			continue
		}
		go func(server int) {
			curTerm := atomic.LoadInt32(&rf.currentTerm)
			args := RequestVoteArgs{
				Term:        curTerm,
				CandidateId: rf.me,
			}
			rep := RequestVoteReply{
				VoteGranted: false,
			}
			if rf.sendRequestVote(server, &args, &rep) && rep.VoteGranted {

				atomic.AddInt32(&rf.voteCount, 1)

			} else if rep.VoteGranted == false && rep.Term > curTerm {
				rf.resetPeer()
				atomic.StoreInt32(&rf.currentTerm, rep.Term)
			}
		}(server)

	}

}
func (rf *Raft) resetPeer() {
	atomic.StoreInt32(&rf.voteCount, 0)
	atomic.StoreInt32(&rf.voteFor, -1)
	rf.ChangeState(FOLLOWER)
}

/**
 * @name:
 * @desc:
 * @param {*}
 * @return {*}
 */
func (rf *Raft) broadcastAppendEntries() {
	for server := 0; server < rf.peerCount; server++ {
		if server == rf.me {
			continue
		}
		if !rf.IsState(LEADER) || rf.killed() {
			rf.resetPeer()
			return
		}
		go rf.Leading(server)

	}
}

/**
 * @name:
 * @desc:
 * @param {*}
 * @return {*}
 */
func (rf *Raft) ticker() {

	for rf.killed() == false {

		if rf.IsState(LEADER) {
			rf.broadcastAppendEntries()
			time.Sleep(time.Millisecond * 100)
			continue
		}
		for {
			if rf.killed() {
				return
			}
			time.Sleep(time.Millisecond * 100)
			curTime := time.Now().UnixNano() / 1e6
			preTime := atomic.LoadInt64(&rf.heartBeatTime)
			if curTime-preTime > HEARTBEAT_TIME_OUT {
				rf.resetPeer()
				break
			}

		}
		randTime := rand.Intn(150) + 150 //150ms-300ms的范围
		if rf.IsState(FOLLOWER) {
			rf.ChangeState(CANDIDATE)
		}
		time.Sleep(time.Millisecond * time.Duration(randTime))
		if !rf.IsState(CANDIDATE) {
			continue
		}
		rf.voting()
		time.Sleep(time.Millisecond * 100)
		votes := atomic.LoadInt32(&rf.voteCount)
		if votes > int32(rf.peerCount)/2 {
			rf.ChangeState(LEADER)
			rf.mu.Lock()
			for index, _ := range rf.nextIndex {
				rf.nextIndex[index] = len(rf.logs)
			}
			rf.mu.Unlock()
		}

	}
}

/**
 * @name: Make
 * @desc: make raft peer
 * @param {*}
 * @return {*}
 */
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.peerCount = len(peers)
	rf.state = FOLLOWER
	rf.dead = 0
	rf.voteCount = 0
	rf.heartBeatTime = time.Now().UnixNano() / 1e6
	rf.currentTerm = 0
	rf.voteFor = -1
	rf.logs = make([]Log, 0)
	rf.logs = append(rf.logs, Log{Term: 0, Index: 0}) //	初始化 log从1开始存
	rf.matchIndex = make([]int, rf.peerCount)
	rf.nextIndex = make([]int, rf.peerCount)
	rf.applyCh = applyCh
	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.ticker()

	return rf
}
