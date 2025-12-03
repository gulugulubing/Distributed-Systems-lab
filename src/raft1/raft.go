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
func (rf *Raft) persist() {
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
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)
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
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&log) != nil {
		fmt.Println("readPersist fails")
	} else {
		rf.votedFor = votedFor
		rf.currentTerm = currentTerm
		rf.log = log
		// fmt.Printf("read server %d votedFor %d currentTerm %d\n", rf.me, votedFor, rf.currentTerm)
	}
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
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
	rf.persist()
	reply.Term = rf.currentTerm

	if args.LastLogTerm > rf.log[len(rf.log)-1].Term && (rf.votedFor == -1 || rf.votedFor == args.CandidateId) {
		// candid's log is more up-to-date than follower
		rf.votedFor = args.CandidateId
		rf.persist()
		reply.VoteGranted = true
		// fmt.Printf("term %d candidate %d, vote granted\n", rf.currentTerm, rf.votedFor)
		tester.Annotate(fmt.Sprintf("Server %d", rf.me),
			fmt.Sprintf("S%d, VoteFor: %d, term: %d", rf.me, rf.votedFor, rf.currentTerm),
			fmt.Sprintf("because cand's lastLog term is %d while my term is %d", args.LastLogTerm, rf.log[len(rf.log)-1].Term))

		rf.Role = follower
		rf.resetElectionTimer()
		return
	}

	if args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIndex >= len(rf.log)-1 && (rf.votedFor == -1 || rf.votedFor == args.CandidateId) {
		// candid's log is also more up-date-to follower
		rf.votedFor = args.CandidateId
		rf.persist()
		reply.VoteGranted = true
		// fmt.Printf("term %d candidate %d, vote granted\n", rf.currentTerm, rf.votedFor)
		tester.Annotate(fmt.Sprintf("Server %d", rf.me),
			fmt.Sprintf("S%d, VoteFor: %d, term: %d", rf.me, rf.votedFor, rf.currentTerm),
			fmt.Sprintf("because cand's lastLog term is %d while my term is %d", args.LastLogTerm, rf.log[len(rf.log)-1].Term))

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
	defer rf.mu.Unlock()
	if args.Entries == nil { // This a heartBeat
		if args.Term < rf.currentTerm { // tell the sender it is not leader anymore
			reply.Term = rf.currentTerm
			reply.Success = false
			return
		}
		if args.Term > rf.currentTerm { // discover a new term
			if rf.Role != follower {
				rf.Role = follower
				tester.Annotate(fmt.Sprintf("Server %d", rf.me),
					"degraded to follower",
					fmt.Sprintf("reiecve HB with new term: %d", args.Term))
			}
			rf.votedFor = -1
			rf.currentTerm = args.Term
			rf.persist()
		}
		reply.Success = true
		rf.resetElectionTimer()

		// heartbeat时看到新的
		if args.LeaderCommit < len(rf.log) && args.LeaderCommitTerm == rf.log[args.LeaderCommit].Term {
			preCommitIdx := rf.commitIndex
			if args.LeaderCommit > rf.commitIndex {
				rf.commitIndex = args.LeaderCommit
				for commitIdx := preCommitIdx + 1; commitIdx <= rf.commitIndex; commitIdx++ {
					applyMsg := raftapi.ApplyMsg{}
					applyMsg.CommandValid = true
					applyMsg.Command = rf.log[commitIdx].Command
					applyMsg.CommandIndex = commitIdx

					//tester.Annotate(fmt.Sprintf("Server %d", rf.me),
					//	fmt.Sprintf("Follower applied log %d commmand %v", commitIdx, applyMsg.Command),
					//	fmt.Sprintf("Server %d at %v", rf.me, time.Now().Format("15:04:05.000")))

					rf.applyCh <- applyMsg
					rf.lastApplied = commitIdx
				}
			}
		}

	} else {
		if args.Term < rf.currentTerm { // tell the sender: your term is too old
			reply.Term = rf.currentTerm
			reply.Success = false
			return
		}

		if args.Term > rf.currentTerm { // discover a new term
			if rf.Role != follower {
				rf.Role = follower
				tester.Annotate(fmt.Sprintf("Server %d", rf.me),
					"degraded to follower",
					fmt.Sprintf("reiecve APE with new term: %d", args.Term))
			}
			rf.votedFor = -1
			rf.currentTerm = args.Term
			rf.persist()
		}
		rf.resetElectionTimer()
		// then handle log

		if args.PrevLogIndex >= len(rf.log) {
			// My (follower's) log too short
			reply.Xlen = len(rf.log)
			reply.Success = false
			// fmt.Printf("follower's log too short, len %d\n", len(rf.log))
			return
		}

		if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
			reply.Xlen = -1 // indicate not the above problem
			reply.Xterm = rf.log[args.PrevLogIndex].Term
			left := 1
			right := len(rf.log) - 1

			for left <= right {
				mid := left + (right-left)/2
				if rf.log[mid].Term >= reply.Xterm {
					right = mid - 1
				} else {
					left = mid + 1
				}
			}
			reply.Xindex = left
			reply.Success = false
			return
		}

		// preLog match
		if args.PrevLogIndex+len(args.Entries) < len(rf.log) {
			isConflict := false
			for i := args.PrevLogIndex + 1; i <= args.PrevLogIndex+len(args.Entries); i++ {
				if rf.log[i].Term != args.Entries[i-args.PrevLogIndex-1].Term {
					rf.log[i] = args.Entries[i-args.PrevLogIndex-1]
					isConflict = true
				}
				//tester.Annotate(fmt.Sprintf("Server %d", rf.me),
				//	fmt.Sprintf("Follower appended log idx %d", i),
				//	fmt.Sprintf("Log length %d", len(rf.log)))
			}
			if isConflict {
				rf.log = rf.log[:args.PrevLogIndex+len(args.Entries)+1]
			}
		} else {
			rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...)
			//tester.Annotate(fmt.Sprintf("Server %d", rf.me),
			//	fmt.Sprintf("Follower appended log from idx %d", args.PrevLogIndex+1),
			//	fmt.Sprintf("Log length %d, last command %v", len(rf.log), rf.log[len(rf.log)-1].Command))
		}
		rf.persist()
		reply.Success = true

		preCommitIdx := rf.commitIndex
		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = min(len(rf.log)-1, args.LeaderCommit)
			for commitIdx := preCommitIdx + 1; commitIdx <= rf.commitIndex; commitIdx++ {
				applyMsg := raftapi.ApplyMsg{}
				applyMsg.CommandValid = true
				applyMsg.Command = rf.log[commitIdx].Command
				applyMsg.CommandIndex = commitIdx

				tester.Annotate(fmt.Sprintf("Server %d", rf.me),
					fmt.Sprintf("Follower applied log %d  Term %v comm %v", commitIdx, rf.log[commitIdx].Term, applyMsg.Command),
					fmt.Sprintf("Server %d at %v", rf.me, time.Now().Format("15:04:05.000")))

				rf.applyCh <- applyMsg
				rf.lastApplied = commitIdx
			}
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

	index = len(rf.log)
	term = rf.currentTerm
	isLeader = true
	rf.log = append(rf.log, LogEntry{rf.currentTerm, command})
	rf.persist()
	ch := make(chan appendRelyInfo, len(rf.peers)-1)
	//tester.Annotate(fmt.Sprintf("Server %d", rf.me),
	//	fmt.Sprintf("Leader send APE last comm %d term %d idx %d", rf.log[len(rf.log)-1].Command, rf.log[len(rf.log)-1].Term, len(rf.log)-1),
	//fmt.Sprintf("pre com %v term %d idx %d", rf.log[len(rf.log)-2].Command, rf.log[len(rf.log)-2].Term, len(rf.log)-2))
	//	fmt.Sprintf(""))
	rf.sendAPE(ch)
	go rf.processAPE(ch)

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

type voteInfo struct {
	term        int
	voteGranted bool
}

type appendRelyInfo struct {
	term    int
	success bool
	// used for APE, tell the leader which follower has appended its logs
	peer       int
	lastAPEIdx int
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
				rf.persist()
				rf.resetElectionTimer()

				ch := make(chan voteInfo, len(rf.peers)-1)
				tester.Annotate(fmt.Sprintf("Server %d", rf.me),
					fmt.Sprintf("candi‘s term: %d", rf.currentTerm),
					fmt.Sprintf("Server %d at %v", rf.me, time.Now().Format("15:04:05.000")))
				rf.startElection(ch)
				go rf.processElectionResult(ch)
			}
		}

		if rf.Role == leader {
			if elapsed := time.Since(rf.lastHeartbeatTime); elapsed > 100*time.Millisecond {
				// lab的hint里说测试会要求heartbeat是100ms，但我没想明白为什么测试可以控制hb，不得是我的代码里控制的么
				ch := make(chan appendRelyInfo, len(rf.peers)-1)
				rf.sendHeartBeat(ch)
				rf.lastHeartbeatTime = time.Now()
				go rf.processHeartbeatResult(ch)
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
	requestVoteArgs.LastLogIndex = len(rf.log) - 1
	requestVoteArgs.LastLogTerm = rf.log[len(rf.log)-1].Term
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
			if rf.Role == candidate {
				if v.term > rf.currentTerm {
					// degrade to follower
					tester.Annotate(fmt.Sprintf("Server %d", rf.me),
						"degraded to follower process ElectionResult",
						fmt.Sprintf("server %d is leader %v, term %d, but reply term: %d", rf.me, rf.Role, rf.currentTerm, v.term))
					rf.currentTerm = v.term
					rf.Role = follower
					rf.votedFor = -1
					rf.persist()
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
						ch := make(chan appendRelyInfo, len(rf.peers)-1)
						rf.sendHeartBeat(ch)
						go rf.processHeartbeatResult(ch)
						rf.nextIndex = make([]int, len(rf.peers))
						rf.matchIndex = make([]int, len(rf.peers))
						tester.Annotate(fmt.Sprintf("Server %d", rf.me), "I win Leader", fmt.Sprintf("Server %d term %d at %v", rf.me, rf.currentTerm, time.Now().Format("15:04:05.000")))
						for i := 0; i < len(rf.peers); i++ {
							if i != rf.me {
								rf.nextIndex[i] = len(rf.log)
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

func (rf *Raft) sendHeartBeat(ch chan appendRelyInfo) {
	args := &AppendEntriesArgs{}
	args.Term = rf.currentTerm
	args.LeaderId = rf.me
	args.Entries = nil // default is nil, just explicitly
	args.LeaderCommit = rf.commitIndex
	args.LeaderCommitTerm = rf.log[rf.commitIndex].Term

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(i int, ch chan appendRelyInfo) {
			reply := &AppendEntriesReply{}
			if ok := rf.sendAppendEntries(i, args, reply); ok {
				ch <- appendRelyInfo{reply.Term, reply.Success, i, -1}
			}
		}(i, ch)
	}
}

func (rf *Raft) processHeartbeatResult(ch chan appendRelyInfo) {
	timeout := time.After(300 * time.Second)
	for rf.killed() == false {
		select {
		case a := <-ch:
			rf.mu.Lock()
			if !a.success && a.term > rf.currentTerm {
				// become follower
				//tester.Annotate(fmt.Sprintf("Server %d", rf.me),
				//	"degraded to follower",
				//	fmt.Sprintf("server %d is leader %v, term %d, but heartbeart reply term: %d", rf.me, rf.Role, rf.currentTerm, a.term))
				tester.Annotate(fmt.Sprintf("Server %d", rf.me),
					"degraded to follower",
					fmt.Sprintf("server %d is leader %v, term %d, but heartbeart reply term: %d", rf.me, rf.Role, rf.currentTerm, a.term))
				rf.currentTerm = a.term
				rf.Role = follower
				rf.votedFor = -1
				rf.persist()
				rf.resetElectionTimer()

				rf.mu.Unlock()
				break
			}
			rf.mu.Unlock()
		case <-timeout:
			close(ch)
			break
		}
	}
}

func (rf *Raft) sendAPE(ch chan appendRelyInfo) {

	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		if len(rf.log)-1 < rf.nextIndex[peer] {
			// last index < nextIndex, no need to send ape
			continue
		}
		args := &AppendEntriesArgs{}
		args.Term = rf.currentTerm
		args.LeaderId = rf.me
		args.PrevLogIndex = rf.nextIndex[peer] - 1
		// fmt.Println(peer, args.PrevLogIndex, len(rf.log), rf.me, rf.currentTerm)
		args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
		// args.Entries = rf.log[rf.nextIndex[peer]:]
		if len(rf.log) > rf.nextIndex[peer] {
			entriesToCopy := rf.log[rf.nextIndex[peer]:]
			args.Entries = make([]LogEntry, len(entriesToCopy))
			copy(args.Entries, entriesToCopy) // 关键：复制数据到新切片
		}
		args.LeaderCommit = rf.commitIndex
		args.LeaderCommitTerm = rf.log[rf.commitIndex].Term
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

				if reply.Success {
					ch <- appendRelyInfo{reply.Term, reply.Success, i, args.PrevLogIndex + len(args.Entries)}
					break
				} else {
					rf.mu.Lock()
					if reply.Term > args.Term {
						// should use args.Term, not rf.currentTerm, because when rpc reply back, currentTerm may change
						// which leads the program to unexpect branch
						// fmt.Println(args.Term, rf.currentTerm)
						ch <- appendRelyInfo{reply.Term, reply.Success, i, -1}
						rf.mu.Unlock()
						break
					} else {
						// 处理不匹配的问题
						// 重新给rpcArgs赋值，重新发送rpc
						if reply.Xlen != -1 {
							// XLen's problem
							rf.nextIndex[peer] = reply.Xlen
						} else {
							left := 1
							right := len(rf.log) - 1
							for left <= right {
								mid := left + (right-left)/2
								if rf.log[mid].Term > reply.Xterm {
									right = mid - 1
								} else {
									left = mid + 1
								}
							}

							if rf.log[left-1].Term != reply.Xterm {
								// follower's conflict term not found in leader
								rf.nextIndex[peer] = reply.Xindex
							} else {
								rf.nextIndex[peer] = left
							}
						}
						args.PrevLogIndex = rf.nextIndex[peer] - 1
						if args.PrevLogIndex > len(rf.log) || args.PrevLogIndex < 0 {
							//fmt.Printf("reply Xlen %d, reply Xindex %d, PrevLogIndex %d, lenOfLOG %d\n", reply.Xlen, reply.Xindex, args.PrevLogIndex, len(rf.log))
							rf.mu.Unlock()
							return
						}
						args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
						args.Entries = rf.log[rf.nextIndex[peer]:]
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
					tester.Annotate(fmt.Sprintf("Server %d", rf.me),
						"degraded to follower When processAPE",
						fmt.Sprintf("My role is %d, my term is %d, but I reiecve term from : %d", rf.Role, rf.currentTerm, a.term))
					rf.Role = follower
					rf.currentTerm = a.term
					rf.votedFor = -1
					rf.persist()

					rf.resetElectionTimer()
				}
				rf.mu.Unlock()
				break
			} else {
				// fmt.Println(a.peer, a.lastAPEIdx)
				if a.lastAPEIdx > len(rf.log)-1 {
					fmt.Println("this leader became follower and its log has been truncated")
					rf.mu.Unlock()
					break
				}
				rf.nextIndex[a.peer] = a.lastAPEIdx + 1
				rf.matchIndex[a.peer] = a.lastAPEIdx

				tester.Annotate(fmt.Sprintf("Server %d", rf.me),
					fmt.Sprintf("Leader knew follower %d append last log %d com: %v", a.peer, a.lastAPEIdx, rf.log[a.lastAPEIdx].Command),
					fmt.Sprintf("Server %d at %v", rf.me, time.Now().Format("15:04:05.000")))
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
					if matchCnt > len(rf.peers)/2 && rf.log[a.lastAPEIdx].Term == rf.currentTerm {
						preCommitIdx := rf.commitIndex
						rf.commitIndex = a.lastAPEIdx
						//fmt.Printf("📌 Leader %d commitIndex: %d commd %d at (current term %d)\n",
						//	rf.me, rf.commitIndex, rf.log[rf.commitIndex].Command, rf.currentTerm)
						for commitIdx := preCommitIdx + 1; commitIdx <= rf.commitIndex; commitIdx++ {
							applyMsg := raftapi.ApplyMsg{}
							applyMsg.CommandValid = true
							applyMsg.Command = rf.log[commitIdx].Command
							applyMsg.CommandIndex = commitIdx
							rf.applyCh <- applyMsg
							rf.lastApplied = commitIdx
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

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	//tester.Annotate(fmt.Sprintf("Server %d", rf.me),
	//"server boots",
	//fmt.Sprintf("server %d is in term %d logLen %d with lastLogTerm %d", rf.me, rf.currentTerm, len(rf.log), rf.log[len(rf.log)-1].Term))

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
