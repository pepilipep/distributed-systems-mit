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
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"

	"../labrpc"
)

// import "bytes"
// import "../labgob"

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type Log struct {
	Command interface{}
	Term    int
}

type LogArray struct {
	log []Log
}

func (l LogArray) get(i int) *Log {
	if i-1 >= len(l.log) || i <= 0 {
		return nil
	}
	return &l.log[i-1]
}

func (l *LogArray) add(log Log, i int) {
	if i-1 >= len(l.log) {
		l.log = append(l.log, log)
	} else {
		l.log[i-1] = log
	}
}

func (l *LogArray) deleteAfter(i int) {
	l.log = l.log[0 : i-1]
}

func (l LogArray) lastIndex() int {
	return len(l.log)
}

func (l LogArray) lastTerm() int {
	if len(l.log) == 0 {
		return -1
	}

	return l.log[len(l.log)-1].Term
}

func (l LogArray) from(i int) []Log {
	return l.log[i-1:]
}

type RoleState int

const (
	Follower = iota
	Candidate
	Leader
)

const (
	heartBeatInterval = 150 * time.Millisecond
	timeoutLower      = 400
	timeoutUpper      = 800
)

func getTimeout() time.Duration {
	return time.Millisecond * time.Duration(rand.Intn(timeoutUpper-timeoutLower)+timeoutLower)
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	currentTerm int
	votedFor    *int
	log         LogArray

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	roleState RoleState
	tooLate   time.Time
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Your code here (2A).
	return rf.currentTerm, rf.roleState == Leader
}

func (rf *Raft) apply(applyCh chan ApplyMsg) {
	if rf.commitIndex > rf.lastApplied {
		rf.lastApplied += 1
		applyCh <- ApplyMsg{
			CommandValid: true,
			CommandIndex: rf.lastApplied,
			Command:      rf.log.get(rf.lastApplied).Command,
		}
	} else {
		time.Sleep(200 * time.Millisecond)
	}

	go rf.apply(applyCh)
}

func (rf *Raft) followerLoop() {
	time.Sleep(getTimeout())

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.roleState != Follower {
		return
	}

	if time.Now().After(rf.tooLate.Add(getTimeout())) {
		rf.convertToCandidate()
		return
	}

	go rf.followerLoop()
}

func (rf *Raft) candidateLoop() {
	rf.mu.Lock()

	if rf.roleState != Candidate {
		rf.mu.Unlock()
		return
	}

	rf.currentTerm += 1
	rf.votedFor = &rf.me
	rf.persist()

	// send RequestVote RPCs to all other servers
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.log.lastIndex(),
		LastLogTerm:  rf.log.lastTerm(),
	}

	votesCh := make(chan bool)

	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		go func(idx int) {
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(idx, &args, &reply)

			rf.mu.Lock()
			if ok && reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.convertToFollower()
			}
			rf.mu.Unlock()

			votesCh <- reply.VoteGranted
		}(i)
	}

	currentTerm := rf.currentTerm
	go func() {
		approvedCnt := 0
		for range rf.peers[1:] {
			vote := <-votesCh
			if vote {
				approvedCnt += 1

				if approvedCnt == len(rf.peers)/2 {

					rf.mu.Lock()
					if rf.currentTerm == currentTerm && rf.roleState == Candidate {
						rf.convertToLeader()
					}
					rf.mu.Unlock()

				}
			}
		}
	}()

	rf.mu.Unlock()

	time.Sleep(getTimeout())
	go rf.candidateLoop()

}

func (rf *Raft) leaderLoop() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.roleState != Leader {
		return
	}

	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		logLastIndex := rf.log.lastIndex()

		go func(idx int) {
			entries := []Log{}
			if logLastIndex >= rf.nextIndex[idx] {
				entries = rf.log.from(rf.nextIndex[idx])
			}

			prevLog := rf.log.get(rf.nextIndex[idx] - 1)
			prevLogTerm := 0
			if prevLog != nil {
				prevLogTerm = prevLog.Term
			}

			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				LeaderCommit: rf.commitIndex,
				Entries:      entries,
				PrevLogIndex: rf.nextIndex[idx] - 1,
				PrevLogTerm:  prevLogTerm,
			}
			reply := AppendEntriesReply{}
			if ok := rf.sendAppendEntries(idx, &args, &reply); !ok {
				return
			}

			rf.mu.Lock()
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.convertToFollower()
				rf.mu.Unlock()

				return
			}
			rf.mu.Unlock()

			if !reply.Success {
				// If AppendEntries fails because of log inconsistency:
				// decrement nextIndex and retry
				rf.nextIndex[idx] -= 1
				// @TODO: try again
			} else {
				// If successful: update nextIndex and matchIndex for
				// follower
				rf.nextIndex[idx] = logLastIndex + 1
				rf.matchIndex[idx] = logLastIndex
			}
		}(i)
	}

	// If there exists an N such that N > commitIndex, a majority
	// of matchIndex[i] ≥ N, and log[N].term == currentTerm:
	// set commitIndex = N
	for c := rf.log.lastIndex(); c > rf.commitIndex; c-- {
		term := rf.log.get(c).Term
		if term < rf.currentTerm {
			break
		}

		cnt := 0
		for i := range rf.peers {
			if i != rf.me && rf.matchIndex[i] >= c {
				cnt += 1
			}
		}

		if cnt >= len(rf.peers)/2 && rf.log.get(c).Term == rf.currentTerm {
			rf.commitIndex = c
			break
		}
	}

	time.Sleep(heartBeatInterval)
	go rf.leaderLoop()

}

func (rf *Raft) convertToFollower() {
	rf.votedFor = nil
	rf.persist()
	rf.roleState = Follower
	go rf.followerLoop()
}

func (rf *Raft) convertToCandidate() {
	rf.roleState = Candidate
	go rf.candidateLoop()
}

func (rf *Raft) convertToLeader() {
	rf.votedFor = nil
	rf.persist()
	rf.roleState = Leader
	rf.nextIndex = make([]int, len(rf.peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = rf.log.lastIndex() + 1
	}
	rf.matchIndex = make([]int, len(rf.peers))

	go rf.leaderLoop()
}

type persistedState struct {
	CurrentTerm int
	VotedFor    *int
	Log         []Log
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(persistedState{rf.currentTerm, rf.votedFor, rf.log.log})
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var s persistedState
	if d.Decode(&s) != nil {
		fmt.Println("error? idk")
	} else {
		rf.currentTerm = s.CurrentTerm
		rf.votedFor = s.VotedFor
		rf.log.log = s.Log
	}
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.convertToFollower()
	}

	// If votedFor is null or candidateId, and candidate’s log is at
	// least as up-to-date as receiver’s log, grant vote

	isUpToDate := rf.log.lastIndex() <= args.LastLogIndex
	if rf.log.lastTerm() != args.LastLogTerm {
		isUpToDate = rf.log.lastTerm() < args.LastLogTerm
	}

	reply.VoteGranted = (rf.votedFor == nil || *rf.votedFor == args.CandidateId) && isUpToDate
	if reply.VoteGranted {
		rf.votedFor = &args.CandidateId
		rf.persist()
	}

}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Log
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.tooLate = time.Now()

	if rf.roleState == Candidate {
		rf.currentTerm = args.Term
		rf.convertToFollower()
	}

	reply.Term = rf.currentTerm

	// Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}

	// fmt.Printf("Leader: %v, Me: %v, Entries: %v, Log: %v \n", args.LeaderId, rf.me, args.Entries, rf.log.log)

	if rf.roleState == Leader && args.Term > rf.currentTerm {
		fmt.Println("I SHOULDNT BE CALLED")
		rf.currentTerm = args.Term
		rf.convertToFollower()
	}

	// Reply false if log doesn’t contain an entry at prevLogIndex
	// whose term matches prevLogTerm
	prevLog := rf.log.get(args.PrevLogIndex)
	if args.PrevLogIndex > 0 && (prevLog == nil || prevLog.Term != args.PrevLogTerm) {
		reply.Success = false
		return
	}

	// Append any new entries not already in the log
	idx := args.PrevLogIndex + 1
	for _, e := range args.Entries {

		// If an existing entry conflicts with a new one (same index
		// 	but different terms), delete the existing entry and all that follow it

		curr := rf.log.get(idx)
		if curr != nil && curr.Term != e.Term {
			rf.log.deleteAfter(idx)
		}

		rf.log.add(e, idx)
		idx += 1

		rf.persist()
	}

	// If leaderCommit > commitIndex, set commitIndex =
	// 	min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(rf.log.lastIndex(), args.LeaderCommit)
	}

	reply.Success = true
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
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.roleState != Leader {
		return 0, 0, false
	}

	// If command received from client: append entry to local log,
	// respond after entry applied to state machine
	idx := rf.log.lastIndex() + 1
	rf.log.add(Log{Command: command, Term: rf.currentTerm}, idx)

	rf.persist()

	return idx, rf.currentTerm, true
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

	// Your initialization code here (2A, 2B, 2C).
	go rf.apply(applyCh)
	go rf.followerLoop()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
