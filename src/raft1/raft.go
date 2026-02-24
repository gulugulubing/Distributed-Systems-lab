package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	"6.5840/tester1"
)

type stateType int

const (
	follower stateType = iota
	candidate
	leader
)

type LogEntry struct {
	Term    int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// need to persist
	currentTerm int
	votedFor    int
	log         []LogEntry

	// use for election
	lastHeartbeatTime time.Time
	electionTimeout   time.Duration
	rdSeed            *rand.Rand
	Role              stateType

	// volatile on all servers
	commitIndex int
	lastApplied int

	applyCh chan raftapi.ApplyMsg

	// volatile on leaders, reinitialized after election
	nextIndex  []int
	matchIndex []int

	// for snapshot, need persistence
	logStartIndex    int
	lastIncludedTerm int

	// for serialize apply
	applyCond *sync.Cond

	// for pending snapshot
	pendingSnapshot      []byte
	pendingSnapshotIndex int
	pendingSnapshotTerm  int

	sendingAE []bool // Make follower not be flooded
}

func (rf *Raft) initRand() {
	seed := time.Now().UnixNano() + int64(rf.me)
	rf.rdSeed = rand.New(rand.NewSource(seed))
}

func (rf *Raft) resetElectionTimer() {
	randomRange := 600 // 600ms 的随机范围
	baseTimeout := 500 // 基础超时 500ms
	// 这里要比论文的大，因为heartbeat时间在这里测试100ms一次
	timeoutMs := baseTimeout + rf.rdSeed.Intn(randomRange)
	rf.electionTimeout = time.Duration(timeoutMs) * time.Millisecond
	rf.lastHeartbeatTime = time.Now()

	// fmt.Printf("⏰ Server %d election timeout: %vms\n", rf.me, timeoutMs)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isLeader bool
	// Your code here (3A).
	term = rf.currentTerm
	isLeader = rf.Role == leader
	return term, isLeader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist(snapshot []byte) {
	// Your code here (3C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if err := e.Encode(rf.currentTerm); err != nil {
		fmt.Println("encoding fails")
	}
	if err := e.Encode(rf.votedFor); err != nil {
		fmt.Println("encoding fails")
	}
	if err := e.Encode(rf.log); err != nil {
		fmt.Println("encoding fails")
	}
	if err := e.Encode(rf.logStartIndex); err != nil {
		panic("raft encoding fails")
	}
	if err := e.Encode(rf.lastIncludedTerm); err != nil {
		panic("raft encoding lastIncludedTerm fails")
	}
	raftstate := w.Bytes()
	if snapshot == nil {
		snapshot = rf.persister.ReadSnapshot()
	}
	rf.persister.Save(raftstate, snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var log []LogEntry
	var currentTerm int
	var votedFor int
	var logStartIndex int
	var lastIncludedTerm int
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&log) != nil || d.Decode(&logStartIndex) != nil || d.Decode(&lastIncludedTerm) != nil {
		panic("raft readPersist fails")
	} else {
		rf.votedFor = votedFor
		rf.currentTerm = currentTerm
		rf.log = log
		rf.logStartIndex = logStartIndex
		rf.lastIncludedTerm = lastIncludedTerm
		if rf.logStartIndex > 0 {
			rf.lastApplied = logStartIndex - 1
			rf.commitIndex = logStartIndex - 1
		} else {
			rf.lastApplied = 0
			rf.commitIndex = 0
		}
		// fmt.Printf("read server %d votedFor %d currentTerm %d\n", rf.me, votedFor, rf.currentTerm)
	}
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

type InstallSnapshotArgs struct {
	Term              int // leader's term
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int // for leader to update itself
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if index < rf.logStartIndex {
		return
	}

	if index >= len(rf.log)+rf.logStartIndex {
		return
	}

	if index <= rf.commitIndex {
		rf.lastIncludedTerm = rf.log[index-rf.logStartIndex].Term
		rf.log = rf.log[index+1-rf.logStartIndex:]
		rf.logStartIndex = index + 1
		rf.persist(snapshot)

	}
}

func (rf *Raft) sendInstallSnapshot(peer int) {
	rf.mu.Lock()
	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.logStartIndex - 1,
		LastIncludedTerm:  rf.lastIncludedTerm,
		Data:              rf.persister.ReadSnapshot(),
	}

	reply := InstallSnapshotReply{}
	if rf.Role != leader || rf.nextIndex[peer] >= rf.logStartIndex {
		rf.mu.Unlock()
		return
	}
	// fmt.Printf("S%d try send snapshot to nextIndex[%d]=%d lastIncludeIndex %d %v\n", rf.me, peer, rf.nextIndex[peer], args.LastIncludedIndex, time.Now().Format("15:04:05.000"))
	rf.mu.Unlock()
	ok := rf.peers[peer].Call("Raft.InstallSnapshot", &args, &reply)

	if !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > args.Term {
		rf.Role = follower
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.persist(nil)
		rf.resetElectionTimer()
		return
	}

	if args.LastIncludedIndex > rf.matchIndex[peer] {
		rf.nextIndex[peer] = args.LastIncludedIndex + 1
		rf.matchIndex[peer] = args.LastIncludedIndex
	} else {
		// fmt.Printf("old installSnapshot reply. Now %d's next is %d while args'lastIncludeIndex is %d %v\n", peer, rf.nextIndex[peer], args.LastIncludedIndex, time.Now().Format("15:04:05.000"))
	}
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm { // discover a new term
		if rf.Role != follower {
			rf.Role = follower
			// fmt.Printf("S%d degradted to follower due recieve a new term %d to old term %d at %v\n", rf.Role, args.Term, rf.currentTerm, time.Now().Format("15:04:05.000"))
			// tester.Annotate(fmt.Sprintf("Server %d", rf.me),
			// "degraded to follower",
			// fmt.Sprintf("reiecve snapshot with new term: %d", args.Term))
		}
		rf.votedFor = -1
		rf.currentTerm = args.Term
		rf.persist(nil)
	}

	if args.LastIncludedIndex <= rf.lastApplied || args.LastIncludedIndex < rf.logStartIndex {
		// no need to send reply
		return
	}

	if args.LastIncludedIndex >= rf.logStartIndex+len(rf.log)-1 {
		rf.log = make([]LogEntry, 0)
	} else {
		if args.LastIncludedTerm == rf.log[args.LastIncludedIndex-rf.logStartIndex].Term {
			rf.log = rf.log[args.LastIncludedIndex+1-rf.logStartIndex:]
		} else {
			rf.log = make([]LogEntry, 0)
		}
	}

	rf.logStartIndex = args.LastIncludedIndex + 1
	rf.lastIncludedTerm = args.LastIncludedTerm
	rf.persist(args.Data)
	if rf.commitIndex < args.LastIncludedIndex {
		rf.commitIndex = args.LastIncludedIndex
	}

	rf.pendingSnapshot = args.Data
	rf.pendingSnapshotIndex = args.LastIncludedIndex
	rf.pendingSnapshotTerm = args.LastIncludedTerm
	rf.applyCond.Broadcast()
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

type voteInfo struct {
	term        int
	voteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		// follow has seen more up-to-date term
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	if args.Term == rf.currentTerm && rf.votedFor != -1 {
		// already vote for someone at this term
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	// candid's term is at least up-to-date same as this follower, maybe more up-to-date
	if args.Term > rf.currentTerm {
		// see a new term
		rf.votedFor = -1
		rf.Role = follower
	}
	rf.currentTerm = args.Term
	rf.persist(nil)
	reply.Term = rf.currentTerm

	lastLogTerm := rf.lastIncludedTerm
	if len(rf.log) > 0 {
		lastLogTerm = rf.log[len(rf.log)-1].Term
	}

	if args.LastLogTerm > lastLogTerm && (rf.votedFor == -1 || rf.votedFor == args.CandidateId) {
		// candid's log is more up-to-date than follower
		rf.votedFor = args.CandidateId
		rf.persist(nil)
		reply.VoteGranted = true
		// fmt.Printf("S%d vote granted %d %v\n", rf.me, args.CandidateId, time.Now().Format("15:04:05.000"))
		//tester.Annotate(fmt.Sprintf("Server %d", rf.me),
		//	fmt.Sprintf("S%d, VoteFor: %d, term: %d", rf.me, rf.votedFor, rf.currentTerm),
		//	fmt.Sprintf("because cand's lastLog term is %d while my term is %d", args.LastLogTerm, lastLogTerm))

		rf.Role = follower
		rf.resetElectionTimer()
		return
	}

	if args.LastLogTerm == lastLogTerm && args.LastLogIndex >= rf.logStartIndex+len(rf.log)-1 && (rf.votedFor == -1 || rf.votedFor == args.CandidateId) {
		// candid's log is also more up-date-to follower
		rf.votedFor = args.CandidateId
		rf.persist(nil)
		reply.VoteGranted = true
		// fmt.Printf("S%d vote granted %d %v\n", rf.me, args.CandidateId, time.Now().Format("15:04:05.000"))
		//tester.Annotate(fmt.Sprintf("Server %d", rf.me),
		//	fmt.Sprintf("S%d, VoteFor: %d, term: %d", rf.me, rf.votedFor, rf.currentTerm),
		//	fmt.Sprintf("because cand's lastLog term is %d while my term is %d", args.LastLogTerm, lastLogTerm))

		rf.Role = follower
		rf.resetElectionTimer()
		return
	}
	// candid's log is too old to win vote
	// shouldn't reset electionTime here, because rejecting the vote
	reply.VoteGranted = false
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
	Term             int
	LeaderId         int // looks no use in partA
	PrevLogIndex     int
	PrevLogTerm      int
	Entries          []LogEntry
	LeaderCommit     int
	LeaderCommitTerm int
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	// for rolling back quickly
	Xterm  int
	Xindex int
	Xlen   int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	if args.Entries == nil { // This a heartBeat
		if args.Term < rf.currentTerm { // tell the sender it is not leader anymore
			reply.Term = rf.currentTerm
			reply.Success = false
			rf.mu.Unlock()
			return
		}
		if args.Term > rf.currentTerm { // discover a new term
			if rf.Role != follower {
				rf.Role = follower
			}
			rf.votedFor = -1
			rf.currentTerm = args.Term
			rf.persist(nil)
		}
		reply.Success = true
		rf.resetElectionTimer()

		if args.LeaderCommit < rf.logStartIndex+len(rf.log) && args.LeaderCommit-rf.logStartIndex >= 0 && args.LeaderCommitTerm == rf.log[args.LeaderCommit-rf.logStartIndex].Term {
			if args.LeaderCommit > rf.commitIndex {
				rf.commitIndex = args.LeaderCommit
				rf.applyCond.Broadcast()
			}
		}
		rf.mu.Unlock()
	} else {
		if args.Term < rf.currentTerm { // tell the sender: your term is too old
			reply.Term = rf.currentTerm
			reply.Success = false
			rf.mu.Unlock()
			return
		}

		if args.Term > rf.currentTerm { // discover a new term
			if rf.Role != follower {
				rf.Role = follower
				// fmt.Printf("S%d degradted to follower due recieve a new term %d to old term %d at %v\n", rf.Role, args.Term, rf.currentTerm, time.Now().Format("15:04:05.000"))
				//tester.Annotate(fmt.Sprintf("Server %d", rf.me),
				//	"degraded to follower",
				//	fmt.Sprintf("reiecve APE with new term: %d", args.Term))
			}
			rf.votedFor = -1
			rf.currentTerm = args.Term
			rf.persist(nil)
		}
		rf.resetElectionTimer()
		// then handle log

		if args.PrevLogIndex >= len(rf.log)+rf.logStartIndex {
			// My (follower's) log too short
			// fmt.Printf("APE find S%d logLen+startIdx %d too short to PrveIndex %d %v\n", rf.me, len(rf.log)+rf.logStartIndex, args.PrevLogIndex, time.Now().Format("15:04:05"))
			reply.Xlen = len(rf.log) + rf.logStartIndex
			reply.Success = false
			rf.mu.Unlock()
			return
		}

		// idx < logStartIndex are all commited
		if args.PrevLogIndex >= rf.logStartIndex && rf.log[args.PrevLogIndex-rf.logStartIndex].Term != args.PrevLogTerm {
			// fmt.Printf("APE find S%d pre Log term unmatch%v\n", rf.me, time.Now().Format("15:04:05"))
			reply.Xlen = -1 // indicate not the above problem
			reply.Xterm = rf.log[args.PrevLogIndex-rf.logStartIndex].Term
			left := 0
			if rf.logStartIndex == 0 {
				left = 1
			} else {
				left = rf.logStartIndex
			}
			right := rf.logStartIndex + len(rf.log) - 1

			for left <= right {
				mid := left + (right-left)/2
				if rf.log[mid-rf.logStartIndex].Term >= reply.Xterm {
					right = mid - 1
				} else {
					left = mid + 1
				}
			}
			reply.Xindex = left
			reply.Success = false
			rf.mu.Unlock()
			return
		}

		// preLog match
		if args.PrevLogIndex+len(args.Entries) < rf.logStartIndex+len(rf.log) {
			isConflict := false
			for i := args.PrevLogIndex + 1; i <= args.PrevLogIndex+len(args.Entries); i++ {
				if i-rf.logStartIndex >= 0 && rf.log[i-rf.logStartIndex].Term != args.Entries[i-args.PrevLogIndex-1].Term {
					rf.log[i-rf.logStartIndex] = args.Entries[i-args.PrevLogIndex-1]
					isConflict = true
				}
			}
			if isConflict {
				rf.log = rf.log[:args.PrevLogIndex+len(args.Entries)+1-rf.logStartIndex]
			}
		} else {
			if args.PrevLogIndex+1-rf.logStartIndex >= 0 {
				rf.log = append(rf.log[:args.PrevLogIndex+1-rf.logStartIndex], args.Entries...)
			} else {
				rf.log = args.Entries[rf.logStartIndex-(args.PrevLogIndex+1):]
			}
		}
		rf.persist(nil)
		//tester.Annotate(fmt.Sprintf("Server %d", rf.me),
		//	"receive new APE log ",
		//	fmt.Sprintf("S%d recieve a new log at term %d   PrevlogIdx %d apeLogLen %d lastComm %d at %v\n", rf.me, args.Term, args.PrevLogIndex, len(args.Entries), args.Entries[len(args.Entries)-1].Command, time.Now().Format("15:04:05.000")))
		// fmt.Printf("S%d rev APE PrevlogIdx %d apeLogLen %d lastComm %d %v at logStartIdx %d, now len:%d, argTerm: %d, myTerm %d\n", rf.me, args.PrevLogIndex, len(args.Entries), args.Entries[len(args.Entries)-1].Command, time.Now().Format("15:04:05.000"), rf.logStartIndex, len(rf.log), args.Term, rf.currentTerm)
		reply.Success = true
		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = min(rf.logStartIndex+len(rf.log)-1, args.LeaderCommit)
			rf.applyCond.Broadcast()
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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
	isLeader := false

	// Your code here (3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.Role != leader {
		return index, term, isLeader
	}

	index = len(rf.log) + rf.logStartIndex
	term = rf.currentTerm
	isLeader = true
	// fmt.Printf("S%d start idx%d command %d at Term %d, %v\n", rf.me, len(rf.log)+rf.logStartIndex, command, rf.currentTerm, time.Now().Format("15:04:05.000"))
	rf.log = append(rf.log, LogEntry{rf.currentTerm, command})
	rf.persist(nil)
	rf.sendAPE()
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
	// notify applier to close applyCh and return
	rf.applyCond.Broadcast()

}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (3A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		if rf.Role != leader {
			if elapsed := time.Since(rf.lastHeartbeatTime); elapsed > rf.electionTimeout {
				rf.Role = candidate

				rf.currentTerm = rf.currentTerm + 1
				rf.votedFor = rf.me
				rf.persist(nil)
				rf.resetElectionTimer()

				ch := make(chan voteInfo, len(rf.peers)-1)
				// fmt.Printf("S%d become candid at term %d %v\n", rf.me, rf.currentTerm, time.Now().Format("15:04:05.000"))
				/*tester.Annotate(fmt.Sprintf("Server %d", rf.me),
				fmt.Sprintf("candi‘s term: %d", rf.currentTerm),
				fmt.Sprintf("Server %d at %v", rf.me, time.Now().Format("15:04:05.000")))
				*/
				rf.startElection(ch)
				go rf.processElectionResult(ch)
			}
		} else {
			if elapsed := time.Since(rf.lastHeartbeatTime); elapsed > 100*time.Millisecond {
				rf.lastHeartbeatTime = time.Now()
				rf.sendAPE()

				for peer := range rf.peers {
					if peer == rf.me {
						continue
					}
					if rf.nextIndex[peer] < rf.logStartIndex {
						go rf.sendInstallSnapshot(peer)
					}
				}
			}
		}

		rf.mu.Unlock()
		// pause for a random amount of time between 10 and 20
		// milliseconds.
		ms := 10 + (rand.Int63() % 10)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) applyTicker() {
	defer close(rf.applyCh)
	for rf.killed() == false {
		rf.mu.Lock()
		for rf.lastApplied >= rf.commitIndex && rf.pendingSnapshot == nil && !rf.killed() {
			rf.applyCond.Wait()
		}
		if rf.killed() {
			rf.mu.Unlock()
			return
		}

		if rf.pendingSnapshot != nil {
			snapshot := rf.pendingSnapshot
			rf.pendingSnapshot = nil
			pendingSnapshotTerm := rf.pendingSnapshotTerm
			pendingSnapshotIndex := rf.pendingSnapshotIndex
			rf.mu.Unlock()

			rf.applyCh <- raftapi.ApplyMsg{
				SnapshotValid: true,
				Snapshot:      snapshot,
				SnapshotTerm:  pendingSnapshotTerm,
				SnapshotIndex: pendingSnapshotIndex,
			}
			rf.mu.Lock()
			rf.lastApplied = max(rf.lastApplied, pendingSnapshotIndex)
			rf.mu.Unlock()
			continue
		}

		var msgs []raftapi.ApplyMsg
		for commitIdx := rf.lastApplied + 1; commitIdx <= rf.commitIndex; commitIdx++ {
			applyMsg := raftapi.ApplyMsg{}
			applyMsg.CommandValid = true
			applyMsg.Command = rf.log[commitIdx-rf.logStartIndex].Command
			applyMsg.CommandIndex = commitIdx
			// fmt.Printf("S%d commit index: %d comm: %d %v\n", rf.me, commitIdx, applyMsg.Command, time.Now().Format("15:04:05"))
			msgs = append(msgs, applyMsg)
		}
		rf.mu.Unlock()

		for _, msg := range msgs {
			//fmt.Printf("S%d,send apply msg in ProAPE idx:%d, com: %d\n", rf.me, msg.CommandIndex, msg.Command)
			rf.applyCh <- msg
			// fmt.Printf("S%d,after sending apply msg in ProAPE idx:%d, com: %d\n", rf.me, msg.CommandIndex, msg.Command)
		}
		rf.mu.Lock()
		rf.lastApplied = max(rf.lastApplied, msgs[len(msgs)-1].CommandIndex)
		rf.mu.Unlock()
	}
}
func (rf *Raft) startElection(ch chan voteInfo) {
	requestVoteArgs := &RequestVoteArgs{}
	requestVoteArgs.Term = rf.currentTerm
	requestVoteArgs.CandidateId = rf.me
	requestVoteArgs.LastLogIndex = len(rf.log) + rf.logStartIndex - 1
	if len(rf.log) == 0 {
		requestVoteArgs.LastLogTerm = rf.lastIncludedTerm
	} else {
		requestVoteArgs.LastLogTerm = rf.log[len(rf.log)-1].Term
	}
	for i := range rf.peers {
		if i == rf.me {
			// no need to vote myself, already vote when become candid
			continue
		}
		go func(i int, ch chan voteInfo) {
			requestVoteReply := &RequestVoteReply{}
			if ok := rf.sendRequestVote(i, requestVoteArgs, requestVoteReply); ok {
				ch <- voteInfo{requestVoteReply.Term, requestVoteReply.VoteGranted}
			}
		}(i, ch)
	}
}

func (rf *Raft) processElectionResult(ch chan voteInfo) {
	voteCount := 1
	timeout := time.After(300 * time.Second)
	for rf.killed() == false {
		select {
		case v := <-ch:
			rf.mu.Lock()
			// fmt.Println("acquire lock in line 588")
			if rf.Role == candidate {
				if v.term > rf.currentTerm {
					// degrade to follower
					// fmt.Printf("S%d degradted to follower due recieve a new term %d to old term %d at %v\n", rf.Role, v.term, rf.currentTerm, time.Now().Format("15:04:05.000"))
					//tester.Annotate(fmt.Sprintf("Server %d", rf.me),
					//	"degraded to follower process ElectionResult",
					//	fmt.Sprintf("s%d is leader, term %d, but reply term: %d", rf.me, rf.currentTerm, v.term))
					rf.currentTerm = v.term
					rf.Role = follower
					rf.votedFor = -1
					rf.persist(nil)
					rf.resetElectionTimer()

					rf.mu.Unlock()
					break
				}
				if v.term < rf.currentTerm {
					rf.mu.Unlock()
					break
				}
				if v.voteGranted {
					// fmt.Printf("Election for me %v\n", rf.me)
					voteCount++
					if voteCount > len(rf.peers)/2 && v.term == rf.currentTerm && rf.Role != leader {
						// maybe old term vote come, so need to check term here
						rf.Role = leader
						rf.nextIndex = make([]int, len(rf.peers))
						rf.matchIndex = make([]int, len(rf.peers))
						// fmt.Printf("S%d win leader at term %d %v\n", rf.me, rf.currentTerm, time.Now().Format("15:04:05.000"))
						// tester.Annotate(fmt.Sprintf("Server %d", rf.me), "I win Leader", fmt.Sprintf("Server %d term %d at %v", rf.me, rf.currentTerm, time.Now().Format("15:04:05.000")))
						for i := 0; i < len(rf.peers); i++ {
							if i != rf.me {
								rf.nextIndex[i] = len(rf.log) + rf.logStartIndex
								rf.matchIndex[i] = 0
							}
						}
						rf.sendAPE()
					}
				}
			}
			rf.mu.Unlock()

		case <-timeout:
			// No need to close ch here, go's gc will handle it
			break
		}
	}
}

func (rf *Raft) sendAPE() {
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}

		// If we are already sending to this peer, skip spawning a new goroutine!
		// The active goroutine will naturally batch the new entries.
		if rf.sendingAE[peer] {
			continue
		}
		rf.sendingAE[peer] = true

		go func(p int) {
			// MUST release the flag when this goroutine exits
			defer func() {
				rf.mu.Lock()
				rf.sendingAE[p] = false
				rf.mu.Unlock()
			}()

			args := &AppendEntriesArgs{}
			rf.mu.Lock()
			if rf.Role != leader {
				rf.mu.Unlock()
				return
			}
			if rf.nextIndex[p] < rf.logStartIndex {
				// should send snap not APE
				// fmt.Printf("S%d too slow,preLogIdx%d : logStartIdx %d %v\n", peer, args.PrevLogIndex, rf.logStartIndex, time.Now().Format("15:04:05.000"))
				rf.mu.Unlock()
				rf.sendInstallSnapshot(p)
				return
			}
			args.Term = rf.currentTerm
			args.LeaderId = rf.me
			args.PrevLogIndex = rf.nextIndex[p] - 1
			if args.PrevLogIndex == rf.logStartIndex-1 {
				args.PrevLogTerm = rf.lastIncludedTerm
			} else {
				args.PrevLogTerm = rf.log[args.PrevLogIndex-rf.logStartIndex].Term
			}
			if rf.logStartIndex+len(rf.log) > rf.nextIndex[p] {
				entriesToCopy := rf.log[rf.nextIndex[p]-rf.logStartIndex:]
				args.Entries = make([]LogEntry, len(entriesToCopy))
				copy(args.Entries, entriesToCopy) // 关键：复制数据到新切片
				//fmt.Printf("S%d send APE %d at Term %d\n", rf.me, args.Entries[len(args.Entries)-1].Command, rf.currentTerm)
			} else {
				args.Entries = nil
			}
			args.LeaderCommit = rf.commitIndex
			if rf.commitIndex-rf.logStartIndex < 0 {
				args.LeaderCommitTerm = rf.lastIncludedTerm
			} else {
				args.LeaderCommitTerm = rf.log[rf.commitIndex-rf.logStartIndex].Term
			}
			rf.mu.Unlock()

			reply := &AppendEntriesReply{}
			//tester.Annotate(fmt.Sprintf("Server %d", rf.me),
			//fmt.Sprintf("leader %d send APE to %d\n", rf.me, peer),
			//fmt.Sprintf("Am I leader %d, Am I dead %d, term %d", rf.Role, rf.dead, rf.currentTerm))
			if ok := rf.sendAppendEntries(p, args, reply); !ok {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()
			if reply.Success {
				lastApeIndex := args.PrevLogIndex + len(args.Entries)
				if lastApeIndex > rf.logStartIndex+len(rf.log)-1 {
					// fmt.Println("this leader became follower and its log has been truncated")
					return
				}

				if lastApeIndex >= rf.matchIndex[p] {
					rf.nextIndex[p] = lastApeIndex + 1
					rf.matchIndex[p] = lastApeIndex
					//tester.Annotate(fmt.Sprintf("Server %d", rf.me),
					//	fmt.Sprintf("follower %d append last log %d logStart at: %d", a.peer, a.lastAPEIdx, rf.logStartIndex),
					//	fmt.Sprintf("Server %d at %v", rf.me, time.Now().Format("15:04:05.000")))
				} else {
					// fmt.Println("ape done by follower but info is old")
				}
				if lastApeIndex > rf.commitIndex {
					// a potential commit log copied by the majority
					matchCnt := 1
					for peerId := range rf.matchIndex {
						if peerId == rf.me {
							continue
						}
						if rf.matchIndex[peerId] >= lastApeIndex {
							matchCnt++
						}
					}
					if matchCnt > len(rf.peers)/2 && rf.log[lastApeIndex-rf.logStartIndex].Term == rf.currentTerm {
						rf.commitIndex = lastApeIndex
						rf.applyCond.Broadcast()
						// fmt.Printf("could commit-idx%d--from follower-S%d,%d\n", lastApeIndex, peer, rf.log[lastApeIndex-rf.logStartIndex].Command)
					}
				}
			} else {
				if reply.Term > args.Term {
					// should use args.Term, not rf.currentTerm, because when rpc reply back, currentTerm may change
					// which leads the program to unexpect branch
					if reply.Term > rf.currentTerm {
						// fmt.Printf("S%d My role is %d, my term is %d, but I reiecve term from : %d at preLog %d, len(log) %d\n", rf.me, rf.Role, rf.currentTerm, reply.Term, args.PrevLogIndex, len(args.Entries))
						rf.Role = follower
						rf.currentTerm = reply.Term
						rf.votedFor = -1
						rf.persist(nil)

						rf.resetElectionTimer()
					}
				} else {
					// handling unMatch problem
					if reply.Xlen != -1 {
						// XLen's problem
						rf.nextIndex[p] = reply.Xlen
						// fmt.Printf("S%d handling unMatch len problem rf.nextIndex[%d]=%d,am I crash:%v %v\n", rf.me, peer, rf.nextIndex[peer], rf.killed(), time.Now().Format("15:04:05.000"))
					} else {
						left := 0
						if rf.logStartIndex == 0 {
							left = 1
						} else {
							left = rf.logStartIndex
						}
						right := rf.logStartIndex + len(rf.log) - 1
						for left <= right {
							mid := left + (right-left)/2
							if rf.log[mid-rf.logStartIndex].Term > reply.Xterm {
								right = mid - 1
							} else {
								left = mid + 1
							}
						}

						lastLogTerm := rf.lastIncludedTerm
						if left-1-rf.logStartIndex >= 0 {
							lastLogTerm = rf.log[left-1-rf.logStartIndex].Term
						}
						if lastLogTerm != reply.Xterm {
							// follower's conflict term not found in leader
							rf.nextIndex[p] = reply.Xindex
						} else {
							rf.nextIndex[p] = left
						}
						// fmt.Printf("S%d handling unMatch Term problem rf.nextIndex[%d]=%d %v\n", rf.me, peer, rf.nextIndex[peer], time.Now().Format("15:04:05.000"))
					}
				}
			}
		}(peer)
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
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.Role = follower
	rf.initRand()
	rf.resetElectionTimer()
	rf.log = []LogEntry{}
	rf.log = append(rf.log, LogEntry{0, nil})
	rf.applyCh = applyCh
	rf.logStartIndex = 0
	rf.applyCond = sync.NewCond(&rf.mu)

	rf.sendingAE = make([]bool, len(peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.applyTicker()

	return rf
}
