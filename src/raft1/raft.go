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
	logStartIndex int

	lastIncludedTerm int

	// for serialize apply
	applyCond      *sync.Cond
	applyingLogCnt int // tell installed snapshot apply wait
}

// 在初始化每个 Raft 节点时调用
func (rf *Raft) initRand() {
	// 使用当前时间的纳秒级时间戳 + 节点ID作为种子
	// 这样可以确保不同节点、不同启动时刻的种子都不同
	seed := time.Now().UnixNano() + int64(rf.me)
	rf.rdSeed = rand.New(rand.NewSource(seed))
}

// 然后在需要生成随机超时的地方使用这个实例
func (rf *Raft) resetElectionTimer() {
	randomRange := 800 // 600ms 的随机范围
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
		fmt.Println("encoding fails")
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
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&log) != nil || d.Decode(&logStartIndex) != nil {
		fmt.Println("readPersist fails")
	} else {
		rf.votedFor = votedFor
		rf.currentTerm = currentTerm
		rf.log = log
		rf.logStartIndex = logStartIndex
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

		//tester.Annotate(fmt.Sprintf("Server %d", rf.me),
		//	"doing snapshot",
		//	fmt.Sprintf("new logStartIndex %d %v", index+1, time.Now().Format("15:04:05")))
		// fmt.Printf("S%d doing snapshot new logStartIndex %d %v\n", rf.me, index+1, time.Now().Format("15:04:05.000"))
		//if rf.Role == leader {
		//	rf.sendInstallSnapshot()
		//}
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
	// startWaitReply := time.Now()
	ok := rf.peers[peer].Call("Raft.InstallSnapshot", &args, &reply)
	/*
		for !ok && !rf.killed() {
			ok = rf.peers[peer].Call("Raft.InstallSnapshot", &args, &reply)
			time.Sleep(10 * time.Millisecond)
			if elapsed := time.Since(startWaitReply); elapsed > 300*time.Second {
				// if the leader can Not get the reply from follower, it should not modify anything below
				return
			}
		}
	*/

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
		// fmt.Printf("slow s%d catch match index %d\n", peer, rf.nextIndex[peer])
		// tester.Annotate(fmt.Sprintf("Server %d", rf.me),
		//"catch match index",
		//fmt.Sprintf("slow s%d catch match index %d %v", peer, rf.nextIndex[peer], time.Now().Format("15:04:05.000")))
	} else {
		// fmt.Printf("old installSnapshot reply. Now %d's next is %d while args'lastIncludeIndex is %d %v\n", peer, rf.nextIndex[peer], args.LastIncludedIndex, time.Now().Format("15:04:05.000"))
	}
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
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

	if args.LastIncludedIndex <= rf.lastApplied || args.LastIncludedIndex+1 < rf.logStartIndex {
		// 不需要你的snapshot，我自己就行
		rf.mu.Unlock()
		return
	}

	for rf.applyingLogCnt > 0 {
		// fmt.Printf("S%d waiting install snap lastIncludedIndex: %d %v\n", rf.me, args.LastIncludedIndex, time.Now().Format("15:04:05.000"))
		//tester.Annotate(fmt.Sprintf("Server %d", rf.me),
		//"waiting install snapshot",
		//fmt.Sprintf("lastIncludedIndex: %d, lastIncludedTerm: %d", args.LastIncludedIndex, args.LastIncludedTerm))
		rf.applyCond.Wait()
	}
	if args.LastIncludedIndex <= rf.lastApplied || args.LastIncludedIndex+1 < rf.logStartIndex {
		rf.mu.Unlock()
		return
	}

	if args.LastIncludedIndex >= rf.logStartIndex+len(rf.log)-1 {
		// oldLogLen := len(rf.log)
		rf.log = make([]LogEntry, 0)
		// fmt.Printf("S%d Installing snapshot remove all previous (oldStartIndex: %d, : newStartIndex%d),(oldLogLen %d : new LogLe %d) %v\n", rf.me, rf.logStartIndex, args.LastIncludedIndex+1, oldLogLen, len(rf.log), time.Now().Format("15:04:05.000"))
	} else {
		// oldLogLen := len(rf.log)
		// rf.log = rf.log[args.LastIncludedIndex+1-rf.logStartIndex:]
		// fmt.Printf("S%d Installing snapshot truncate previous log (oldStartIndex: %d, : newStartIndex%d),(oldLogLen %d : new LogLe %d) %v\n", rf.me, rf.logStartIndex, args.LastIncludedIndex+1, oldLogLen, len(rf.log), time.Now().Format("15:04:05.000"))
		rf.mu.Unlock()
		return
	}

	rf.logStartIndex = args.LastIncludedIndex + 1
	rf.lastIncludedTerm = args.LastIncludedTerm
	rf.persist(args.Data)
	if rf.commitIndex < args.LastIncludedIndex {
		rf.commitIndex = args.LastIncludedIndex
		rf.lastApplied = args.LastIncludedIndex
	} else {
		rf.mu.Unlock()
		return
	}

	rf.applyCond.L.Unlock()

	msg := raftapi.ApplyMsg{}
	msg.CommandValid = false
	msg.SnapshotValid = true
	msg.Snapshot = args.Data
	msg.SnapshotTerm = args.LastIncludedTerm
	msg.SnapshotIndex = args.LastIncludedIndex
	rf.applyCh <- msg
	// fmt.Printf("S%d finish install snap lastIncludedIndex: %d %v", rf.me, args.LastIncludedIndex, time.Now().Format("15:04:05.000"))
	//tester.Annotate(fmt.Sprintf("Server %d", rf.me),
	//	"finish install snapshot",
	//	fmt.Sprintf("lastIncludedIndex: %d, lastIncludedTerm: %d", args.LastIncludedIndex, args.LastIncludedTerm))
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

type appendRelyInfo struct {
	term    int
	success bool
	// used for APE, tell the leader which follower has appended its logs
	peer       int
	lastAPEIdx int
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
				// fmt.Printf("S%d degradted to follower due recieve a new term %d to old term %d at %v\n", rf.Role, args.Term, rf.currentTerm, time.Now().Format("15:04:05.000"))
				// tester.Annotate(fmt.Sprintf("Server %d", rf.me),
				//	"degraded to follower",
				//	fmt.Sprintf("reiecve HB with new term: %d", args.Term))
			}
			rf.votedFor = -1
			rf.currentTerm = args.Term
			rf.persist(nil)
		}
		reply.Success = true
		rf.resetElectionTimer()

		// heartbeat时看到新的
		if args.LeaderCommit < rf.logStartIndex+len(rf.log) && args.LeaderCommit-rf.logStartIndex >= 0 && args.LeaderCommitTerm == rf.log[args.LeaderCommit-rf.logStartIndex].Term {
			preCommitIdx := rf.commitIndex
			var msgs []raftapi.ApplyMsg
			if args.LeaderCommit > rf.commitIndex {
				rf.commitIndex = args.LeaderCommit
				for commitIdx := preCommitIdx + 1; commitIdx <= rf.commitIndex; commitIdx++ {
					applyMsg := raftapi.ApplyMsg{}
					applyMsg.CommandValid = true
					applyMsg.Command = rf.log[commitIdx-rf.logStartIndex].Command
					applyMsg.CommandIndex = commitIdx
					// fmt.Printf("S%d inHB commit index: %d startIndex: %d len: %d comm: %d %v\n", rf.me, commitIdx, rf.logStartIndex, len(rf.log), applyMsg.Command, time.Now().Format("15:04:05.000"))
					msgs = append(msgs, applyMsg)
				}
				// fmt.Println("=====================")
				rf.applyingLogCnt++
				rf.mu.Unlock()

				if len(msgs) == 0 {
					return
				}

				rf.applyCond.L.Lock()
				for msgs[0].CommandIndex != rf.lastApplied+1 {
					rf.applyCond.Wait()
				}
				rf.applyCond.L.Unlock()

				for _, msg := range msgs {
					// fmt.Printf("S%d,send apply msg in HB idx:%d, com: %d\n", rf.me, msg.CommandIndex, msg.Command)
					rf.applyCh <- msg
					/*
						rf.mu.Lock()
						rf.lastApplied = msg.CommandIndex
						rf.mu.Unlock()
					*/
					// fmt.Printf("S%d,after sending apply msg in HB idx:%d, com: %d\n", rf.me, msg.CommandIndex, msg.Command)
				}
				rf.mu.Lock()
				rf.lastApplied = msgs[len(msgs)-1].CommandIndex
				rf.applyingLogCnt--
				rf.mu.Unlock()
				rf.applyCond.Broadcast()
			} else {
				rf.mu.Unlock()
			}
		} else {
			rf.mu.Unlock()
		}
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

		// 如果小于的话，一定是吻合的，logStartIndex前面都是commit的
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
		// fmt.Printf("S%d rev APE PrevlogIdx %d apeLogLen %d lastComm %d %v at logStartIdx %d, now len:%d\n", rf.me, args.PrevLogIndex, len(args.Entries), args.Entries[len(args.Entries)-1].Command, time.Now().Format("15:04:05.000"), rf.logStartIndex, len(rf.log))
		reply.Success = true

		preCommitIdx := rf.commitIndex
		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = min(rf.logStartIndex+len(rf.log)-1, args.LeaderCommit)
			var msgs []raftapi.ApplyMsg
			for commitIdx := preCommitIdx + 1; commitIdx <= rf.commitIndex; commitIdx++ {
				applyMsg := raftapi.ApplyMsg{}
				applyMsg.CommandValid = true
				applyMsg.Command = rf.log[commitIdx-rf.logStartIndex].Command
				applyMsg.CommandIndex = commitIdx
				// fmt.Printf("S%d inAPE commit index: %d startIndex: %d len: %d comm: %d %v\n", rf.me, commitIdx, rf.logStartIndex, len(rf.log), applyMsg.Command, time.Now().Format("15:04:05.000"))
				msgs = append(msgs, applyMsg)
			}
			// fmt.Println("=====================")
			rf.applyingLogCnt++
			rf.mu.Unlock()

			if len(msgs) == 0 {
				return
			}
			rf.applyCond.L.Lock()
			for msgs[0].CommandIndex != rf.lastApplied+1 {
				rf.applyCond.Wait()
			}
			rf.applyCond.L.Unlock()

			for _, msg := range msgs {
				// fmt.Printf("S%d,send apply msg in APE idx:%d, com: %d\n", rf.me, msg.CommandIndex, msg.Command)
				rf.applyCh <- msg
				/*
					rf.mu.Lock()
					rf.lastApplied = msg.CommandIndex
					rf.mu.Unlock()
				*/
				// fmt.Printf("S%d,after sending apply msg in APE idx:%d, com: %d\n", rf.me, msg.CommandIndex, msg.Command)
			}
			rf.mu.Lock()
			rf.lastApplied = msgs[len(msgs)-1].CommandIndex
			rf.applyingLogCnt--
			rf.mu.Unlock()
			rf.applyCond.Broadcast()
		} else {
			rf.mu.Unlock()
		}
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
	// fmt.Printf("S%d start idx%d command %d %v\n", rf.me, len(rf.log)+rf.logStartIndex, command, time.Now().Format("15:04:05.000"))
	rf.log = append(rf.log, LogEntry{rf.currentTerm, command})
	rf.persist(nil)

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
				// tester.Annotate(fmt.Sprintf("Server %d", rf.me),
				//fmt.Sprintf("candi‘s term: %d", rf.currentTerm),
				//fmt.Sprintf("Server %d at %v", rf.me, time.Now().Format("15:04:05.000")))
				rf.startElection(ch)
				go rf.processElectionResult(ch)
			}
		} else {
			if elapsed := time.Since(rf.lastHeartbeatTime); elapsed > 100*time.Millisecond {
				// lab的hint里说测试会要求heartbeat是100ms，但我没想明白为什么测试可以控制hb，不得是我的代码里控制的么
				rf.sendHeartBeat()
				rf.lastHeartbeatTime = time.Now()

				apeCh := make(chan appendRelyInfo, len(rf.peers)-1)
				rf.sendAPE(apeCh)
				go rf.processAPE(apeCh)

				for peer := range rf.peers {
					if peer == rf.me {
						continue
					}
					if rf.nextIndex[peer] < rf.logStartIndex {
						go rf.sendInstallSnapshot(peer)
					} else {
						if rf.logStartIndex+len(rf.log)-1 < rf.nextIndex[peer] {
							// last index < nextIndex, no need to send ape, send heartbeat

						} else {
							// send APE
						}
					}
				}
			}
		}

		rf.mu.Unlock()
		// pause for a random amount of time between 10 and 50
		// milliseconds.
		ms := 5 + (rand.Int63() % 5)
		time.Sleep(time.Duration(ms) * time.Millisecond)
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
					if voteCount > len(rf.peers)/2 && v.term == rf.currentTerm {
						// maybe old term vote come, so need to check term here
						rf.Role = leader
						rf.nextIndex = make([]int, len(rf.peers))
						rf.matchIndex = make([]int, len(rf.peers))
						rf.sendHeartBeat()
						// fmt.Printf("S%d win leader at term %d %v\n", rf.me, rf.currentTerm, time.Now().Format("15:04:05.000"))
						// tester.Annotate(fmt.Sprintf("Server %d", rf.me), "I win Leader", fmt.Sprintf("Server %d term %d at %v", rf.me, rf.currentTerm, time.Now().Format("15:04:05.000")))
						for i := 0; i < len(rf.peers); i++ {
							if i != rf.me {
								rf.nextIndex[i] = len(rf.log) + rf.logStartIndex
								rf.matchIndex[i] = 0
							}
						}
					}
				}
			}
			rf.mu.Unlock()

		case <-timeout:
			close(ch)
			break
		}
	}
}

func (rf *Raft) sendHeartBeat() {
	args := &AppendEntriesArgs{}
	args.Term = rf.currentTerm
	args.LeaderId = rf.me
	args.Entries = nil // default is nil, just explicitly
	args.LeaderCommit = rf.commitIndex
	if rf.commitIndex-rf.logStartIndex >= 0 && rf.commitIndex-rf.logStartIndex < len(rf.log) {
		args.LeaderCommitTerm = rf.log[rf.commitIndex-rf.logStartIndex].Term
	}

	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		if rf.logStartIndex+len(rf.log)-1 >= rf.nextIndex[peer] {
			// will send an APE soon, no need to heartbeat
			continue
		}
		go func(i int) {
			reply := &AppendEntriesReply{}
			if ok := rf.sendAppendEntries(i, args, reply); ok {
				rf.mu.Lock()
				if !reply.Success && reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.Role = follower
					rf.votedFor = -1
					rf.persist(nil)
					rf.resetElectionTimer()
				}
				rf.mu.Unlock()
			}
		}(peer)
	}
}

func (rf *Raft) sendAPE(ch chan appendRelyInfo) {
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		if rf.logStartIndex+len(rf.log)-1 < rf.nextIndex[peer] {
			// last index < nextIndex, no need to send ape
			continue
		}
		args := &AppendEntriesArgs{}
		args.Term = rf.currentTerm
		args.LeaderId = rf.me
		args.PrevLogIndex = rf.nextIndex[peer] - 1
		if args.PrevLogIndex < rf.logStartIndex-1 {
			// follower太落后了，应该给他InstallSnapshot
			// fmt.Printf("S%d too slow,preLogIdx%d : logStartIdx %d %v\n", peer, args.PrevLogIndex, rf.logStartIndex, time.Now().Format("15:04:05.000"))
			continue
		}
		if args.PrevLogIndex == rf.logStartIndex-1 {
			args.PrevLogTerm = rf.lastIncludedTerm
		} else {
			args.PrevLogTerm = rf.log[args.PrevLogIndex-rf.logStartIndex].Term
		}
		if rf.logStartIndex+len(rf.log) > rf.nextIndex[peer] {
			entriesToCopy := rf.log[rf.nextIndex[peer]-rf.logStartIndex:]
			args.Entries = make([]LogEntry, len(entriesToCopy))
			copy(args.Entries, entriesToCopy) // 关键：复制数据到新切片
		}
		// fmt.Printf("S%d send %d\n", rf.me, args.Entries[len(args.Entries)-1].Command)
		args.LeaderCommit = rf.commitIndex
		if rf.commitIndex-rf.logStartIndex < 0 {
			args.LeaderCommitTerm = rf.lastIncludedTerm
		} else {
			args.LeaderCommitTerm = rf.log[rf.commitIndex-rf.logStartIndex].Term
		}
		go func(i int, ch chan appendRelyInfo) {
			for rf.killed() == false {
				reply := &AppendEntriesReply{}
				//tester.Annotate(fmt.Sprintf("Server %d", rf.me),
				//fmt.Sprintf("leader %d send APE to %d\n", rf.me, peer),
				//fmt.Sprintf("Am I leader %d, Am I dead %d, term %d", rf.Role, rf.dead, rf.currentTerm))
				ok := rf.sendAppendEntries(peer, args, reply)
				//tester.Annotate(fmt.Sprintf("Server %d", rf.me),
				//fmt.Sprintf("leader %d recive APERply from %d replyOk %v sucess %v\n", rf.me, peer, ok, reply.Success),
				//fmt.Sprintf("Am I leader %d, Am I dead %d, term %d", rf.Role, rf.dead, rf.currentTerm))
				startWaitReply := time.Now()
				for !ok && rf.killed() == false {
					// 3C中可能server是挂了，再重启，所以要持续发送
					ok = rf.sendAppendEntries(peer, args, reply)
					time.Sleep(10 * time.Millisecond)
					if elapsed := time.Since(startWaitReply); elapsed > 300*time.Second {
						// if the leader can Not get the reply from follower, it should not modify anything below
						return
					}
				}
				if rf.killed() {
					return
				}

				if reply.Success {
					ch <- appendRelyInfo{reply.Term, reply.Success, i, args.PrevLogIndex + len(args.Entries)}
					break
				} else {
					rf.mu.Lock()
					if reply.Term > args.Term {
						// fmt.Println("term problem")
						// should use args.Term, not rf.currentTerm, because when rpc reply back, currentTerm may change
						// which leads the program to unexpect branch
						// fmt.Println(args.Term, rf.currentTerm)
						ch <- appendRelyInfo{reply.Term, reply.Success, i, -1}
						rf.mu.Unlock()
						break
					} else {
						// 处理不匹配的问题
						// 重新给rpcArgs赋值，重新发送rpc
						// fmt.Println("handling unMatch problem")
						if reply.Xlen != -1 {
							// XLen's problem
							rf.nextIndex[peer] = reply.Xlen
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
								rf.nextIndex[peer] = reply.Xindex
							} else {
								rf.nextIndex[peer] = left
							}
							// fmt.Printf("S%d handling unMatch Term problem rf.nextIndex[%d]=%d %v\n", rf.me, peer, rf.nextIndex[peer], time.Now().Format("15:04:05.000"))
						}
						args.PrevLogIndex = rf.nextIndex[peer] - 1
						if args.PrevLogIndex >= len(rf.log)+rf.logStartIndex || args.PrevLogIndex < rf.logStartIndex {
							rf.mu.Unlock()
							return
						}
						args.PrevLogTerm = rf.log[args.PrevLogIndex-rf.logStartIndex].Term
						args.Entries = rf.log[rf.nextIndex[peer]-rf.logStartIndex:]
						args.LeaderCommit = rf.commitIndex
					}
					rf.mu.Unlock()
				}
				time.Sleep(10 * time.Millisecond)
			}
		}(peer, ch)
	}
}

func (rf *Raft) processAPE(ch chan appendRelyInfo) {
	timeout := time.After(300 * time.Second)
	for rf.killed() == false {
		select {
		case a := <-ch:
			rf.mu.Lock()
			if !a.success {
				// become follower
				//tester.Annotate(fmt.Sprintf("Server %d", rf.me),
				//	"degraded to follower",
				//	fmt.Sprintf("server %d is leader %v, term %d, but heartbeart reply term: %d", rf.me, rf.Role, rf.currentTerm, a.term))
				if a.term > rf.currentTerm {
					// fmt.Printf("S%d degradted to follower due recieve a new term %d to old term %d at %v\n", rf.Role, a.term, rf.currentTerm, time.Now().Format("15:04:05.000"))
					//tester.Annotate(fmt.Sprintf("Server %d", rf.me),
					//	"degraded to follower When processAPE",
					//	fmt.Sprintf("My role is %d, my term is %d, but I reiecve term from : %d", rf.Role, rf.currentTerm, a.term))
					rf.Role = follower
					rf.currentTerm = a.term
					rf.votedFor = -1
					rf.persist(nil)

					rf.resetElectionTimer()
				}
				rf.mu.Unlock()
				break
			} else {
				// fmt.Printf("reply ape by %d last idx %d\n", a.peer, a.lastAPEIdx)
				if a.lastAPEIdx > rf.logStartIndex+len(rf.log)-1 {
					fmt.Println("this leader became follower and its log has been truncated")
					rf.mu.Unlock()
					break
				}

				if a.lastAPEIdx >= rf.matchIndex[a.peer] {
					rf.nextIndex[a.peer] = a.lastAPEIdx + 1
					rf.matchIndex[a.peer] = a.lastAPEIdx
					//tester.Annotate(fmt.Sprintf("Server %d", rf.me),
					//	fmt.Sprintf("follower %d append last log %d logStart at: %d", a.peer, a.lastAPEIdx, rf.logStartIndex),
					//	fmt.Sprintf("Server %d at %v", rf.me, time.Now().Format("15:04:05.000")))
				} else {
					// fmt.Println("ape done by follower but info is old")
				}
				// 可能存在a.lastAPEIdx < rf.logStartIndex的情况
				// 因为这个回复lastAPEIdx，是之前已经commit掉的，可能已经snapshot后抛弃了，但这个follower还没来得及installsnapshot
				// 这样的话下面的代码就直接跳过了，至少annotate会报错
				//tester.Annotate(fmt.Sprintf("Server %d", rf.me),
				//	fmt.Sprintf("Leader knew follower %d append last log %d com: %v", a.peer, a.lastAPEIdx, rf.log[a.lastAPEIdx-rf.logStartIndex].Command),
				//	fmt.Sprintf("Server %d at %v", rf.me, time.Now().Format("15:04:05.000")))
				if a.lastAPEIdx > rf.commitIndex {
					// 出现了一个新的可能已经在大部分follower得到复制的log
					matchCnt := 1
					for peer := range rf.matchIndex {
						if peer == rf.me {
							continue
						}
						if rf.matchIndex[peer] >= a.lastAPEIdx {
							matchCnt++
						}
					}
					if matchCnt > len(rf.peers)/2 && rf.log[a.lastAPEIdx-rf.logStartIndex].Term == rf.currentTerm {
						preCommitIdx := rf.commitIndex
						rf.commitIndex = a.lastAPEIdx
						// fmt.Printf("could commit----S%d,%d\n", a.peer, rf.log[a.lastAPEIdx-rf.logStartIndex].Command)
						var msgs []raftapi.ApplyMsg
						for commitIdx := preCommitIdx + 1; commitIdx <= rf.commitIndex; commitIdx++ {
							applyMsg := raftapi.ApplyMsg{}
							applyMsg.CommandValid = true
							applyMsg.Command = rf.log[commitIdx-rf.logStartIndex].Command
							applyMsg.CommandIndex = commitIdx
							// fmt.Printf("S%d inleader commit index: %d comm: %d %v\n", rf.me, commitIdx, applyMsg.Command, time.Now().Format("15:04:05"))
							msgs = append(msgs, applyMsg)
						}
						rf.applyingLogCnt++
						rf.mu.Unlock()

						rf.applyCond.L.Lock()
						for msgs[0].CommandIndex != rf.lastApplied+1 {
							rf.applyCond.Wait()
						}
						rf.applyCond.L.Unlock()

						for _, msg := range msgs {
							//fmt.Printf("S%d,send apply msg in ProAPE idx:%d, com: %d\n", rf.me, msg.CommandIndex, msg.Command)
							rf.applyCh <- msg
							/*
								rf.mu.Lock()
								rf.lastApplied = msg.CommandIndex
								rf.mu.Unlock()
							*/
							// fmt.Printf("S%d,after sending apply msg in ProAPE idx:%d, com: %d\n", rf.me, msg.CommandIndex, msg.Command)
						}
						rf.mu.Lock()
						rf.lastApplied = msgs[len(msgs)-1].CommandIndex
						rf.applyingLogCnt--
						rf.mu.Unlock()
						rf.applyCond.Broadcast()
					} else {
						rf.mu.Unlock()
					}
				} else {
					rf.mu.Unlock()
				}
			}
		case <-timeout:
			close(ch)
			break
		}
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

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
