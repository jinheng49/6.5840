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

type Statetype int

const (
	Follower Statetype = iota
	Candidate
	Leader
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	applyCh        chan ApplyMsg // channel to send ApplyMsg to service (or tester)
	applyCond      *sync.Cond    // condition variable to wait for applyCh to be ready
	replicatorCond []*sync.Cond  // condition variable to wait for replicator to be ready
	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	currentTerm int        // current term
	votedFor    int        // candidateId that received vote in current term
	logs        []logEntry // log entries; each entry contains command for state machine, and term when entry was received by leader
	state       Statetype  // current state

	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int

	electionTimer  *time.Timer
	heartbeatTimer *time.Timer
}

type logEntry struct {
	Term    int
	Command interface{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	term = rf.currentTerm
	isleader = (rf.state == Leader)
	return term, isleader
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
	Me          int
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// DPrintf("[term %d]: Raft[%d] state[%d] receive requestVote from Raft[%d]", rf.currentTerm, rf.me, rf.state, args.CandidateId)
	if args.Term < rf.currentTerm || (args.Term == rf.currentTerm && rf.votedFor != -1 && rf.votedFor != args.CandidateId) {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	} else if args.Term > rf.currentTerm {
		rf.ChangeState(Follower)
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}
	if !rf.isLogUpToDate(args.LastLogTerm, args.LastLogIndex) {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	rf.votedFor = args.CandidateId
	rf.electionTimer.Reset(RandomizedElectionTimeout())
	reply.VoteGranted = true
	reply.Term = rf.currentTerm
	reply.Me = rf.me
}

type AppendEntriesArgs struct {
	Term         int
	LeadId       int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []logEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// DPrintf("[term %d]: Raft[%d] [state %d] receive AppendEntries from Raft[%d]", rf.currentTerm, rf.me, rf.state, args.LeadId)
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = Follower
	}
	rf.electionTimer.Reset(RandomizedElectionTimeout())

	if args.PrevLogIndex >= len(rf.logs) || rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		return
	}

	for i := 0; i < len(args.Entries); i++ {
		logIndex := args.PrevLogIndex + i + 1
		if logIndex < len(rf.logs) {
			if rf.logs[logIndex].Term != args.Entries[i].Term {
				rf.logs = rf.logs[:logIndex]
				break
			}
		}
	}

	rf.logs = append(rf.logs, args.Entries...)

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.logs)-1)
	}
	reply.Success = true
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader {
		return -1, -1, false
	}
	DPrintf("[term %d]: Raft[%d] start consensus", rf.currentTerm, rf.me)
	new_log := logEntry{
		Term:    rf.currentTerm,
		Command: command,
	}
	rf.logs = append(rf.logs, new_log)
	index = len(rf.logs)
	term = rf.currentTerm
	// args := &AppendEntriesArgs{
	// 	Term:   rf.currentTerm,
	// 	LeadId: rf.me,
	// }
	// reply := &AppendEntriesReply{}
	// go rf.SendAppendEntries(args, reply)

	return index, term, isLeader
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

func (rf *Raft) ticker() {
	for rf.killed() == false {
		select {
		case <-rf.electionTimer.C:
			// DPrintf("[term %d]:Raft[%d] find election timeout", rf.currentTerm, rf.me)
			rf.mu.Lock()
			rf.ChangeState(Candidate)
			rf.currentTerm += 1
			rf.StartElection()
			rf.electionTimer.Reset(RandomizedElectionTimeout())
			rf.mu.Unlock()
		case <-rf.heartbeatTimer.C:
			rf.mu.Lock()
			if rf.state == Leader {
				rf.SendHeartBeat()
				// DPrintf("[term %d] Raft[%d] send Hearbeats", rf.currentTerm, rf.me)
				rf.heartbeatTimer.Reset(stableheatbeatTime())
			}
			rf.mu.Unlock()
		}

		// Your code here (3A)
		// Check if a leader election should be started.

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
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
	rf := &Raft{
		peers:          peers,
		persister:      persister,
		me:             me,
		dead:           0,
		applyCh:        applyCh,
		replicatorCond: make([]*sync.Cond, len(peers)),
		currentTerm:    0,
		votedFor:       -1,
		logs:           make([]logEntry, 1),
		state:          Follower,
		commitIndex:    0,
		lastApplied:    0,
		nextIndex:      make([]int, len(peers)),
		matchIndex:     make([]int, len(peers)),
		electionTimer:  time.NewTimer(RandomizedElectionTimeout()),
		heartbeatTimer: time.NewTimer(stableheatbeatTime()),
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

func stableheatbeatTime() time.Duration {
	return 100 * time.Millisecond
}

func RandomizedElectionTimeout() time.Duration {
	return time.Duration(150+rand.Intn(150)) * time.Millisecond
}

func (rf *Raft) ChangeState(state Statetype) {
	rf.state = state
}

func (rf *Raft) StartElection() {
	DPrintf("[term %d]: Raft[%d] start election", rf.currentTerm, rf.me)
	// rf.ChangeState(Candidate)
	// rf.currentTerm += 1
	rf.votedFor = rf.me
	// rf.electionTimer.Reset(RandomizedElectionTimeout())
	voteChan := make(chan RequestVoteReply, len(rf.peers))
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.logs) - 1,
		LastLogTerm:  rf.logs[len(rf.logs)-1].Term,
	}
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go func(server int) {
				var reply RequestVoteReply
				ok := rf.sendRequestVote(server, args, &reply)
				if ok {
					voteChan <- reply
				}
			}(i)
		}
	}
	votesReceived := 1
	majority := len(rf.peers)/2 + 1
	electionFinished := false
	for i := 0; i < len(rf.peers); i++ {
		select {
		case reply := <-voteChan:
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.state = Follower
				rf.votedFor = -1
				// rf.persist()
				return
			}
			if reply.VoteGranted && !electionFinished {
				DPrintf("[term %d]: Raft[%d] receive vote from Raft[%d]", rf.currentTerm, rf.me, reply.Me)
				votesReceived++
				if votesReceived >= majority {
					DPrintf("[term %d]: Raft[%d] become leader", rf.currentTerm, rf.me)
					electionFinished = true
					rf.state = Leader
					rf.SendHeartBeat()
					// rf.persist()
					return
				}
			}
		case <-time.After(300 * time.Millisecond):
			if rf.state == Candidate {
				DPrintf("[term %d]: Raft[%d] new election", rf.currentTerm, rf.me)
				go rf.StartElection()

			}
			return
		}
	}
}

func (rf *Raft) SendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) LeaderSendLog() {
	DPrintf("[term %d]: Raft[%d] send log to all peers", rf.currentTerm, rf.me)
	if rf.state != Leader {
		return
	}
	for i := range rf.peers {
		go func(server int) {
			rf.mu.Lock()
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeadId:       rf.me,
				PrevLogIndex: rf.nextIndex[server] - 1,
				PrevLogTerm:  rf.logs[rf.nextIndex[server]-1].Term,
				Entries:      rf.logs[rf.nextIndex[server]:],
				LeaderCommit: rf.commitIndex,
			}
			rf.mu.Unlock()
			var reply AppendEntriesReply
			if rf.SendAppendEntries(server, &args, &reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.state = Follower
					rf.votedFor = -1
					rf.electionTimer.Reset(RandomizedElectionTimeout())
					return
				}
				if reply.Success {
					rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
					rf.matchIndex[server] = rf.nextIndex[server] - 1
				} else {
					rf.nextIndex[server]--
				}
			}
		}(i)
		time.Sleep(10 * time.Millisecond)
	}
}

func (rf *Raft) SendHeartBeat() {
	DPrintf("[term %d]: Raft[%d] send Hearbeats to all peers", rf.currentTerm, rf.me)
	if rf.state != Leader {
		return
	}
	for i := range rf.peers {
		go func(server int) {
			rf.mu.Lock()
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeadId:       rf.me,
				PrevLogIndex: len(rf.logs) - 1,
				PrevLogTerm:  rf.logs[len(rf.logs)-1].Term,
				Entries:      []logEntry{},
				LeaderCommit: rf.commitIndex,
			}
			rf.mu.Unlock()

			var reply AppendEntriesReply

			if rf.SendAppendEntries(server, &args, &reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.state = Follower
					rf.votedFor = -1
					rf.electionTimer.Reset(RandomizedElectionTimeout())
					return
				}
				if reply.Success {
					rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
					rf.matchIndex[server] = rf.nextIndex[server] - 1
				} else {
					rf.nextIndex[server]--
				}
			}
		}(i)
		// time.Sleep(10 * time.Millisecond)是不是太长了
	}
	rf.heartbeatTimer.Reset(stableheatbeatTime())
}

func (rf *Raft) isLogUpToDate(lastLogTerm, lastLogIndex int) bool {
	localLastLogTerm := rf.logs[len(rf.logs)-1].Term
	localLastLogIndex := len(rf.logs) - 1
	if lastLogTerm > localLastLogTerm {
		return true
	} else if lastLogTerm == localLastLogTerm && lastLogIndex >= localLastLogIndex {
		return true
	}
	return false
}
