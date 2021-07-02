/*
 * @Description:
 * @User: Snaper <532990528@qq.com>
 * @Date: 2021-06-16 12:25:21
 * @LastEditTime: 2021-07-02 20:57:31
 */

package raft

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
	"math/rand"
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

type Log struct {
	term    int
	Command interface{}
}

//raft struct
type Raft struct {
	mu             sync.Mutex          // Lock to protect shared access to this peer's state
	peers          []*labrpc.ClientEnd // RPC end points of all peers
	persister      *Persister          // Object to hold this peer's persisted state
	me             int                 // this peer's index into peers[]
	dead           int32               // set by Kill()
	voteCount      int32
	heartBeatTime  int64
	heartBeatState int32
	peerCount      int

	//need persist
	state       int32
	currentTerm int32
	voteFor     int32
	logs        []Log

	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int

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

/**
 * @name:
 * @desc: 判断rf的状态，使用atomic 并发安全
 * @param {int32} state
 * @return {*}
 */
func (rf *Raft) ChangeState(state int32) {
	atomic.StoreInt32(&rf.state, state)
}

func (rf *Raft) IsHeartBeatDead() bool {
	return atomic.LoadInt32(&rf.heartBeatState) == HEARTBEAT_DEAD
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	return term, isleader
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
	success bool
}

//
// RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term <= rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false

	} else {
		rf.currentTerm = args.Term
		rf.voteFor = int32(args.CandidateId)

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesRply) {
	curTerm := atomic.LoadInt32(&rf.currentTerm)
	if args.Term < curTerm {
		reply.success = false
		return
	}
	atomic.StoreInt32(&rf.currentTerm, args.Term)
	reply.success = true

}
func (rf *Raft) SendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesRply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

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

func (rf *Raft) Leading() {
	atomic.StoreInt32(&rf.state, LEADER)
	atomic.AddInt32(&rf.currentTerm,1)
	go func() {
		for {
			for server := 0; server < rf.peerCount; server++ {
				if server == rf.me {
					continue
				}
				args := AppendEntriesArgs{}
				rep := AppendEntriesRply{}
				ok := rf.SendAppendEntries(server, &args, &rep)
				if !ok {
					continue
				}
				if rep.success == false {
					rf.ChangeState(FOLLOWER)
					return
				}

			}
			time.Sleep(time.Microsecond * 100)
		}
	}()

}

/**
 * @name: voting
 * @desc:
 * @param {*}
 * @return {*}
 */
func (rf *Raft) voting() {

	atomic.StoreInt32(&rf.voteFor, int32(rf.me))
	for server := 0; server < rf.peerCount; server++ {
		if server == rf.me {
			continue
		}
		go func(server int) {
			curTerm := atomic.LoadInt32(&rf.currentTerm)
			args := RequestVoteArgs{
				Term:        curTerm,
				CandidateId: rf.me,
			}
			rep := RequestVoteReply{}
			if rf.sendRequestVote(server, &args, &rep) && rep.VoteGranted {
				atomic.AddInt32(&rf.voteCount, 1)

			}
		}(server)

	}

}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently
func (rf *Raft) ticker() {

	for rf.killed() == false {
		if rf.IsState(LEADER) {
			time.Sleep(time.Microsecond * 100)
			continue
		}
		for {
			curTime := time.Now().UnixNano() / 1e6
			preTime := atomic.LoadInt64(&rf.heartBeatTime)
			if curTime-preTime > HEARTBEAT_TIME_OUT {
				break
			}
			time.Sleep(time.Microsecond * 100)
		}
		randTime := rand.Intn(150) + 150 //150ms-300ms的范围
		if rf.IsState(FOLLOWER) {
			rf.ChangeState(CANDIDATE)
		}
		time.Sleep(time.Microsecond * time.Duration(randTime))
		if !rf.IsState(CANDIDATE) {
			continue
		}
		rf.voting()
		votes := atomic.LoadInt32(&rf.voteCount)
		if votes >= rf.voteCount/2 {

			rf.Leading()
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
	rf.heartBeatTime = time.Now().UnixNano() / 1e6
	rf.currentTerm = 0
	rf.voteFor = -1
	rf.logs = make([]Log, 0)
	rf.matchIndex = make([]int, 0)
	rf.nextIndex = make([]int, 0)
	rf.peerCount = len(peers)
	rf.state = FOLLOWER
	rf.currentTerm = 0

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.ticker()

	return rf
}
