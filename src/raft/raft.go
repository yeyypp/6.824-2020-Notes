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
	"../labrpc"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// import "bytes"
// import "../labgob"

type STATE string

const (
	FOLLOWER  STATE = "FOLLOWER"
	CANDIDATE STATE = "CANDIDATE"
	LEADER    STATE = "LEADER"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type Entry struct {
	Index   int
	Term    int
	Command string
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

	// Persistent state on all servers
	currentTerm int
	votedFor    int
	log         []*Entry

	// Volatile state on all servers
	commitIndex int //  a log is safe to apply to state machine(not apply)
	lastApplied int // index of highest log entry applied to state machine(already applied)

	state    STATE

	// Volatile state on leaders
	nextIndex  []int
	matchIndex []int

	// last election time
	lastElectionTime time.Time

	// election count
	count int

	// applyMsg chan
	applyCh chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isLeader := rf.state == LEADER
	return term, isLeader
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
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term              int
	LeaderId          int
	PrevLogIndex      int
	PrevLogTerm       int
	Entries           []*Entry
	LeaderCommitIndex int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Candidate term < currentTerm, return false
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// if args.term > currentTerm, update term, clear votedFor, reset timer
	if args.Term > rf.currentTerm {
		rf.changeToFollower(args.Term)
	}

	// when term is same and this rf has voted.
	if rf.votedFor != -1 {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	l := len(rf.log)
	if l == 0 || args.LastLogTerm > rf.log[l-1].Term || (args.LastLogTerm > rf.log[l-1].Term && args.LastLogIndex > rf.log[l-1].Index) {
		rf.votedFor = args.CandidateId
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		rf.lastElectionTime = time.Now()
	} else {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	}

}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	if args.PrevLogIndex != 0 && (len(rf.log) < args.PrevLogIndex || rf.log[args.PrevLogIndex-1].Term != args.PrevLogTerm) {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.changeToFollower(args.Term)
	}

	reply.Term = rf.currentTerm
	reply.Success = true
	rf.lastElectionTime = time.Now()

	if args.LeaderCommitIndex > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommitIndex, rf.log[len(rf.log)-1].Index)
		for rf.commitIndex > rf.lastApplied {
			rf.lastApplied += 1
			m := ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.lastApplied].Command,
				CommandIndex: rf.log[rf.lastApplied].Index,
			}
			rf.applyCh <- m
		}
	}
	// TODO update entries

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

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
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
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]*Entry, 0)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	rf.isLeader = false
	rf.state = FOLLOWER

	rf.lastElectionTime = time.Now()
	rf.count = 0
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	//check Election
	go rf.checkElection()


	return rf
}

// TODO need two goruntine to send heart beat and start election
// TODO send log in order on applyCh
func (rf *Raft) checkElection() {
	for {
		timeout := time.Duration(rf.getElectionTimeout()) * time.Millisecond
		time.Sleep(timeout)
		if time.Now().Sub(rf.lastElectionTime) >= timeout {
			switch rf.state {
			case FOLLOWER:
				go rf.reElection()
			case CANDIDATE:
				go rf.reElection()
			case LEADER:
				fmt.Printf("%d is leader now\n", rf.me)
			}
		}
	}

}

func (rf *Raft) reElection() {
	var curTerm int
	var lastLogTerm int
	var lastLogIndex int

	rf.mu.Lock()
	rf.currentTerm += 1
	curTerm = rf.currentTerm
	if len(rf.log) != 0 {
		lastLogTerm = rf.log[len(rf.log)-1].Term
		lastLogIndex = rf.log[len(rf.log)-1].Index
	}

	rf.votedFor = rf.me
	rf.state = CANDIDATE
	rf.lastElectionTime = time.Now()
	rf.isLeader = false
	rf.mu.Unlock()

	electionResult := make(chan int)

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func() {
			if rf.state != CANDIDATE {
				electionResult <- 0
				return
			}
			args := &RequestVoteArgs{
				Term:         curTerm,
				CandidateId:  rf.me,
				LastLogTerm:  lastLogTerm,
				LastLogIndex: lastLogIndex,
			}

			reply := &RequestVoteReply{}

			for !rf.sendRequestVote(i, args, reply) {
				continue
			}

			curTerm2 := rf.currentTerm
			if reply.Term > curTerm2 {
				rf.mu.Lock()
				fmt.Printf("reply.Term is %d, bigger than rf.currentTerm %d, change to follower.\n", reply.Term, curTerm2)
				rf.changeToFollower(reply.Term)
				rf.mu.Unlock()

				electionResult <- 0
				return
			}

			if reply.VoteGranted {
				electionResult <- 1
			} else {
				electionResult <- 0
			}
		}()
	}

	var count int
	for i := 0; i < len(rf.peers); i++ {
		count += <-electionResult
	}

	rf.mu.Lock()
	if rf.currentTerm == curTerm && count > (len(rf.peers)-1)/2 && rf.state == CANDIDATE {
		rf.state = LEADER
		rf.isLeader = true
		go rf.sendHeartBeat()
	}
	rf.mu.Unlock()
}

func (rf *Raft) sendHeartBeat() {

	for rf.state == LEADER {
		var curTerm int
		var preLogTerm int
		var preLogIndex int
		var leaderCommitIndex int

		rf.mu.Lock()
		curTerm = rf.currentTerm
		l := len(rf.log)
		if l != 0 {
			preLogTerm = rf.log[l-1].Term
			preLogIndex = rf.log[l-1].Index
		}
		rf.mu.Unlock()

		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			go func() {
				if rf.state != LEADER {
					return
				}
				args := &AppendEntriesArgs{
					Term:     curTerm,
					LeaderId: rf.me,
					PrevLogTerm: preLogTerm,
					PrevLogIndex: preLogIndex,
					LeaderCommitIndex: leaderCommitIndex,
				}

				reply := &AppendEntriesReply{}

				for !rf.sendAppendEntries(i, args, reply) {
					continue
				}

				curTerm2 := rf.currentTerm

				if reply.Term > curTerm2 {
					rf.mu.Lock()
					fmt.Printf("reply.Term is %d, bigger than rf.currentTerm %d, change to follower.\n", reply.Term, curTerm2)
					rf.changeToFollower(reply.Term)
					rf.mu.Unlock()
					return
				}

				if reply.Term == curTerm2 && reply.Success == false {
					rf.mu.Lock()
					rf.nextIndex[i] -= 1
					rf.mu.Unlock()
				}
			}()
		}

		timeout := time.Duration(100) * time.Millisecond + time.Duration(rf.getElectionTimeout()) * time.Millisecond
		time.Sleep(timeout)
	}
}

func (rf *Raft) changeToFollower(term int) {
	rf.currentTerm = term
	rf.votedFor = -1
	rf.lastElectionTime = time.Now()
	rf.state = FOLLOWER
}

func (rf *Raft) getElectionTimeout() int {
	return 150 + rand.Intn(151)
}


func max(i, j int) int {
	if i > j {
		return i
	}
	return j
}

func min(i, j int) int {
	if i < j {
		return i
	}
	return j
}

