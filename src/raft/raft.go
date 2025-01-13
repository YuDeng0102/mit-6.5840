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

	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type logEntry struct {
	Command interface{}
	Term    int
}

type raftState string

const (
	follower  raftState = "follower"
	candidate           = "candidate"
	leader              = "leader"
)

const HeartbeartInterval = 102 * time.Millisecond

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//persistent states
	currentTerm int
	votedFor    int
	log         []logEntry

	//Volatile state on all servers
	commitIndex int
	lastApplied int

	//Volatile state on leaders
	nextIndex  []int
	matchIndex []int
	applychan  chan ApplyMsg
	condApply  *sync.Cond

	state         raftState
	lastHeartbeat time.Time
	timeout       time.Duration
}

func (rf *Raft) GetLastIndex() int {
	return len(rf.log) - 1
}

func (rf *Raft) GetLastLogTerm() int {
	return rf.log[rf.GetLastIndex()].Term
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.currentTerm
	isLeader := rf.state == leader
	// Your code here (3A).
	return term, isLeader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
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

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

// 判断args的log 是否比当前节点新
func (rf *Raft) compareUpdateLog(args *RequestVoteArgs) bool {
	if rf.GetLastLogTerm() != args.LastLogTerm {
		return rf.GetLastLogTerm() < args.LastLogTerm
	}
	return rf.GetLastIndex() <= args.LastLogIndex
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.votedFor = -1
		rf.currentTerm = args.Term
		rf.state = follower
	}

	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		DPrintf("server %d has vote for other", rf.me)
		reply.VoteGranted = false
		reply.Term = args.Term
		return
	}

	if rf.compareUpdateLog(args) {
		rf.currentTerm = args.Term
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.state = follower
		rf.lastHeartbeat = time.Now()
	} else {
		reply.VoteGranted = false
	}
	reply.Term = rf.currentTerm

}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []logEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()

	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		return
	}
	if len(args.Entries) == 0 {
		DPrintf("server %d receive heartbeat", rf.me)
	} else {
		DPrintf("server %d receive appendEntries with total %d entries from server %v", rf.me, len(args.Entries), args.LeaderId)
		DPrintf("sender's term : %v ,currentTerm:%v", args.Term, rf.currentTerm)
	}

	rf.lastHeartbeat = time.Now()
	if args.Term > rf.currentTerm {
		// 新leader的第一个消息
		rf.currentTerm = args.Term // 更新iterm
		rf.votedFor = -1           // 易错点: 更新投票记录为未投票
		rf.state = follower
	}

	if args.PrevLogIndex > len(rf.log)-1 || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		return
	}

	if len(args.Entries) != 0 && len(rf.log) > args.PrevLogIndex+1 && rf.log[args.PrevLogIndex+1].Term != args.Entries[0].Term {
		rf.log = rf.log[:args.PrevLogIndex+1]
	}
	if len(args.Entries) != 0 {
		rf.log = append(rf.log, args.Entries...)
		DPrintf("server %v append entries in it's log from %d", rf.me, args.LeaderId)
	}
	reply.Success = true
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = int(math.Min(float64(len(rf.log)-1), float64(args.LeaderCommit)))
		rf.mu.Unlock()
		rf.condApply.Signal()
		DPrintf("server %d commitIndex update to %d", rf.me, rf.commitIndex)
		return
	}

	rf.mu.Unlock()

}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) handleAppendEntries(server int, args AppendEntriesArgs) {
	reply := AppendEntriesReply{}
	ok := rf.sendAppendEntries(server, args, &reply)
	if !ok {
		return
	}
	rf.mu.Lock()
	if args.Term != rf.currentTerm || rf.state != leader {
		rf.mu.Unlock()
		return
	}
	if reply.Success {
		rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
		//DPrintf("server %d matchIndex[%d] update to %d", rf.me, server, rf.matchIndex[server])
		rf.nextIndex[server] = rf.matchIndex[server] + 1

		if len(args.Entries) > 0 {
			DPrintf("server %v successfully send appendentries to server %v", rf.me, server)
		} else {
			DPrintf("server %v successfully send heartbeat to server %v", rf.me, server)
		}

		N := len(rf.log) - 1
		for N > rf.commitIndex {
			count := 1
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					continue
				}
				if rf.matchIndex[i] >= N && rf.log[N].Term == rf.currentTerm {
					count++
				}
			}
			if count > len(rf.peers)/2 {
				rf.commitIndex = N
				DPrintf("server %v commitIndex update to %d", rf.me, rf.commitIndex)
				rf.mu.Unlock()
				rf.condApply.Signal()
				return
			}
			N--
		}
		rf.mu.Unlock()
		return
	}

	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.state = follower
		rf.votedFor = -1
		rf.lastHeartbeat = time.Now()
		rf.resetElectionTimeout()
	}

	if reply.Term == rf.currentTerm && rf.state == leader {
		rf.nextIndex[server]--
	}
	rf.mu.Unlock()

}

func (rf *Raft) broadcastHeartbeat() {
	for i, _ := range rf.peers {
		if i != rf.me {
			go func(server int) {
				// reply := AppendEntriesReply{}
				rf.mu.Lock()
				if rf.state != leader {
					rf.mu.Unlock()
					return
				}
				//DPrintf("server %v nextIndex for server %d is %d", rf.me, server, rf.nextIndex[server])
				args := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: rf.nextIndex[server] - 1,
					PrevLogTerm:  rf.log[rf.nextIndex[server]-1].Term,
					LeaderCommit: rf.commitIndex,
				}

				if rf.nextIndex[server] <= rf.GetLastIndex() {
					args.Entries = rf.log[rf.nextIndex[server]:]
				} else {
					args.Entries = make([]logEntry, 0)
				}
				rf.mu.Unlock()
				// if rf.sendAppendEntries(server, &args, &reply) {
				// 	rf.mu.Lock()
				// 	if reply.Term > rf.currentTerm {
				// 		rf.currentTerm = reply.Term
				// 		rf.state = follower
				// 		rf.votedFor = -1
				// 		DPrintf("server %v find other leader,become a follower", rf.me)
				// 		rf.mu.Unlock()
				// 		return
				// 	}
				// 	rf.mu.Unlock()
				// }
				go rf.handleAppendEntries(server, args)
			}(i)
		}
	}

	// DPrintf("server %v 完成broadcastHeartbeat", rf.me)
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := -1
	term := rf.currentTerm
	if rf.state != leader {
		return index, term, false
	}

	rf.log = append(rf.log, logEntry{
		Command: command,
		Term:    rf.currentTerm,
	})
	DPrintf("server %v append a command %v in it's log,current term %v", rf.me, command, rf.currentTerm)
	return len(rf.log) - 1, rf.currentTerm, true
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) resetElectionTimeout() {
	rf.timeout = time.Duration(450+rand.Intn(450)) * time.Millisecond
}

// func (rf *Raft) initializeLeader() {
// 	rf.mu.Lock()
// 	for i := 0; i < len(rf.matchIndex); i++ {
// 		rf.matchIndex[i] = 0
// 		rf.nextIndex[i] = len(rf.log)
// 	}
// 	rf.votedFor = -1
// 	rf.mu.Unlock()
// 	rf.broadcastHeartbeat()
// }

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.state = candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.resetElectionTimeout()
	rf.lastHeartbeat = time.Now()
	DPrintf("server %v start a election for itself", rf.me)
	rf.mu.Unlock()
	rf.broadcastRequestVote()
}

func (rf *Raft) broadcastRequestVote() {
	rf.mu.Lock()
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.log) - 1,
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}
	rf.mu.Unlock()

	voteCount := 1
	// var wg sync.WaitGroup
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			// wg.Add(1)
			go func(server int) {
				// defer wg.Done()
				reply := RequestVoteReply{}
				if rf.sendRequestVote(server, &args, &reply) {
					rf.handleVoteReply(reply, &voteCount)

					if !reply.VoteGranted {
						DPrintf("server %d did't get server %d's vote", rf.me, server)
					} else {
						DPrintf("server %d successfully get server %d's vote", rf.me, server)
					}
				}

			}(i)
		}
	}
	// wg.Wait()
	// rf.mu.Lock()
	// rf.votedFor = -1
	// if voteCount <= len(rf.peers)/2 {
	// 	rf.state = follower
	// }
	// DPrintf("server %v 's vote finished", rf.me)
	// rf.mu.Unlock()

}

func (rf *Raft) handleVoteReply(reply RequestVoteReply, voteCount *int) {
	rf.mu.Lock()

	if reply.VoteGranted {
		*voteCount++
		if *voteCount > len(rf.peers)/2 && rf.state == candidate {
			rf.state = leader
			for i := 0; i < len(rf.peers); i++ {
				rf.matchIndex[i] = 0
				rf.nextIndex[i] = len(rf.log)
			}

			DPrintf("server %d become leader! term is %d", rf.me, rf.currentTerm)
			rf.mu.Unlock()
			// rf.initializeLeader()
			rf.broadcastHeartbeat()
			return
		}
	}

	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.state = follower
	}

	rf.mu.Unlock()
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		// Your code here (3A)
		// Check if a leader election should be started.

		// pause for a random amount of time between 50 and 350
		// milliseconds.

		rf.mu.Lock()
		ms := 50 + (rand.Int63() % 300)
		// DPrintf("server %d ticking", rf.me)
		// DPrintf("server %v ticker,state:%v", rf.me, rf.state)
		// DPrintf("server %v lastHeartbeat:%v,timeout:%v,pass %v ", rf.me, rf.lastHeartbeat, rf.timeout, time.Since(rf.lastHeartbeat))
		if rf.state != leader {
			if time.Since(rf.lastHeartbeat) > rf.timeout {
				rf.mu.Unlock()
				rf.startElection()
			} else {
				rf.mu.Unlock()
			}
		} else if rf.state == leader {
			rf.mu.Unlock()
			// DPrintf("server %d broadcast heartbeat or appendEntyies", rf.me)
			rf.broadcastHeartbeat()
			ms = HeartbeartInterval.Milliseconds()
		}

		time.Sleep(time.Duration(ms) * time.Millisecond)

	}

}

func (rf *Raft) checkApply() {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.commitIndex <= rf.lastApplied {
			rf.condApply.Wait()
		}

		for rf.commitIndex > rf.lastApplied {
			rf.lastApplied++
			msg := &ApplyMsg{
				CommandValid: true,
				CommandIndex: rf.lastApplied,
				Command:      rf.log[rf.lastApplied].Command,
			}
			DPrintf("server %v 准备将命令 %v(索引为 %v ) 应用到状态机\n", rf.me, msg.Command, msg.CommandIndex)
			rf.applychan <- *msg
		}

		rf.mu.Unlock()

	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.state = follower
	rf.log = make([]logEntry, 1)
	rf.log[0] = logEntry{
		Command: nil,
		Term:    0,
	}
	rf.votedFor = -1
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := 0; i < len(peers); i++ {
		rf.nextIndex[i] = 1
	}
	rf.applychan = applyCh
	rf.condApply = sync.NewCond(&rf.mu)
	// Your initialization code here (3A, 3B, 3C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.lastHeartbeat = time.Now()
	rf.resetElectionTimeout()

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.checkApply()
	return rf
}
