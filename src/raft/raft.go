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

	state STATE

	// Volatile state on leaders
	nextIndex  []int
	matchIndex []int

	// last election time
	lastElectionTime time.Time

	// applyMsg chan
	applyCh chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	fun := "GetState --->"
	var term int
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isLeader := rf.state == LEADER
	DPrintf("%s node:%d, term:%d, state:%s\n", fun, rf.me, rf.currentTerm, rf.state)
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
	fun := "RequestVote Receive --->"
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.killed() {
		return
	}
	fmt.Printf("%s node:%d, term:%d, state:%s, args:%+v\n", fun, rf.me, rf.currentTerm, rf.state, args)

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


	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.lastElectionTime = time.Now()
		reply.Term = rf.currentTerm
		return
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	fun := "AppendEntries Receive --->"
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.killed() {
		return
	}
	fmt.Printf("%s node:%d, term:%d, state:%s, args:%+v\n", fun, rf.me, rf.currentTerm, rf.state, args)

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.changeToFollower(args.Term)
	}

	// TODO fix log comparing
	//if args.PrevLogIndex != 0 && (len(rf.log) < args.PrevLogIndex || rf.log[args.PrevLogIndex-1].Term != args.PrevLogTerm) {
	//	reply.Term = rf.currentTerm
	//	reply.Success = false
	//	return
	//}

	// if cur state is candidate, turn to follower
	if rf.state == CANDIDATE {
		rf.changeToFollower(args.Term)
	}
	reply.Term = rf.currentTerm
	reply.Success = true
	rf.lastElectionTime = time.Now()

	//if args.LeaderCommitIndex > rf.commitIndex {
	//	rf.commitIndex = min(args.LeaderCommitIndex, rf.log[len(rf.log)-1].Index)
	//	for rf.commitIndex > rf.lastApplied {
	//		rf.lastApplied += 1
	//		m := ApplyMsg{
	//			CommandValid: true,
	//			Command:      rf.log[rf.lastApplied].Command,
	//			CommandIndex: rf.log[rf.lastApplied].Index,
	//		}
	//		rf.applyCh <- m
	//	}
	//}
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
	fun := "Start --->"
	index := -1
	term := -1
	isLeader := true

	fmt.Printf("%s node:%d, term:%d, state:%s, command:%v\n", fun, rf.me, rf.currentTerm, rf.state, command)

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

	rf.state = FOLLOWER

	rf.lastElectionTime = time.Now()
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	//check Election
	go rf.checkElection()

	return rf
}

// TODO send log in order on applyCh
func (rf *Raft) checkElection() {
	fun := "CheckElection --->"

	for !rf.killed() {
		timeout := time.Duration(rf.getElectionTimeout()) * time.Millisecond
		time.Sleep(timeout)
		rf.mu.Lock()
		lastElectionTime := rf.lastElectionTime
		state := rf.state
		rf.mu.Unlock()

		if time.Now().Sub(lastElectionTime) >= timeout {
			fmt.Printf(" %s Timeout! node:%d, term:%d, state:%s \n", fun, rf.me, rf.currentTerm, rf.state)
			switch state {
			case FOLLOWER:
				go rf.reElection()
			case CANDIDATE:
				go rf.reElection()
			case LEADER:
				fmt.Printf("Node:%d is leader now\n", rf.me)
			}
		}
	}
	fmt.Printf("node:%d, killed:%v\n", rf.me, rf.killed())

}

func (rf *Raft) reElection() {
	if rf.killed() {
		fmt.Printf("Node:%d is down.", rf.me)
		return
	}

	// that's the term and index when this rf request votes
	var sendingTerm int
	var sendingLastLogTerm int
	var sendingLastLogIndex int

	rf.mu.Lock()
	rf.currentTerm += 1
	sendingTerm = rf.currentTerm
	if len(rf.log) != 0 {
		sendingLastLogTerm = rf.log[len(rf.log)-1].Term
		sendingLastLogIndex = rf.log[len(rf.log)-1].Index
	}

	rf.votedFor = rf.me
	rf.state = CANDIDATE
	rf.lastElectionTime = time.Now()
	rf.mu.Unlock()

	var count uint64
	//var wg sync.WaitGroup

	//for i := 0; i < len(rf.peers); i++ {
	//	if rf.killed() {
	//		fmt.Printf("Before sending peer ---> node:%d down\n", rf.me)
	//		return
	//	}
	//	if atomic.LoadUint64(&count) > uint64((len(rf.peers)/2 - 1)) {
	//		rf.mu.Lock()
	//		if !rf.killed() && rf.currentTerm == sendingTerm && rf.state == CANDIDATE {
	//			fmt.Printf("Node:%d become leader. term:%d\n", rf.me, rf.currentTerm)
	//
	//			rf.state = LEADER
	//			go rf.sendHeartBeat()
	//		}
	//		rf.mu.Unlock()
	//		return
	//	}
	//	if rf.me == i {
	//		continue
	//	}
	//	//wg.Add(1)
	//	curI := i
	//	go func() {
	//		//defer wg.Done()
	//
	//		// when actually send the request, the term and state may different than the old
	//		rf.mu.Lock()
	//		state := rf.state
	//		term := rf.currentTerm
	//		rf.mu.Unlock()
	//
	//		if state != CANDIDATE || term != sendingTerm {
	//			return
	//		}
	//		args := &RequestVoteArgs{
	//			Term:         sendingTerm,
	//			CandidateId:  rf.me,
	//			LastLogTerm:  sendingLastLogTerm,
	//			LastLogIndex: sendingLastLogIndex,
	//		}
	//
	//		reply := &RequestVoteReply{}
	//
	//		if !rf.sendRequestVote(curI, args, reply) {
	//			return
	//		}
	//
	//		if rf.killed() {
	//			fmt.Printf("During sending peer node:%d down\n", rf.me)
	//			return
	//		}
	//
	//		rf.mu.Lock()
	//		term = rf.currentTerm
	//		state = rf.state
	//		rf.mu.Unlock()
	//		if term != sendingTerm || state != CANDIDATE {
	//			return
	//		}
	//
	//		if reply.Term < sendingTerm {
	//			return
	//		}
	//
	//		if reply.Term > sendingTerm && rf.state == CANDIDATE {
	//			rf.mu.Lock()
	//			fmt.Printf("Step down, node:%d change to follower.\n", rf.me)
	//			rf.changeToFollower(reply.Term)
	//			rf.mu.Unlock()
	//
	//			return
	//		}
	//
	//		if reply.VoteGranted {
	//			atomic.AddUint64(&count, 1)
	//		}
	//	}()
	//}
	for i := 0; i < len(rf.peers); i++ {
		if rf.killed() {
			return
		}
		if rf.me == i {
			continue
		}

		rf.mu.Lock()
		curTerm := rf.currentTerm
		curState := rf.state
		rf.mu.Unlock()

		if curTerm != sendingTerm || curState != CANDIDATE {
			return
		}

		go func(i int) {
			args := &RequestVoteArgs{}
			reply := &RequestVoteReply{}

			args.Term = sendingTerm
			args.CandidateId = rf.me
			args.LastLogTerm = sendingLastLogTerm
			args.LastLogIndex = sendingLastLogIndex

			if rf.sendRequestVote(i, args, reply) {
				if rf.killed() {
					return
				}

				rf.mu.Lock()
				curTerm := rf.currentTerm
				curState := rf.state
				rf.mu.Unlock()

				if curTerm != sendingTerm || curState != CANDIDATE {
					return
				}

				if reply.Term < sendingTerm {
					return
				}

				if reply.Term > sendingTerm {
					rf.mu.Lock()
					rf.changeToFollower(reply.Term)
					rf.mu.Unlock()
					return
				}

				if reply.VoteGranted {
					atomic.AddUint64(&count, 1)
					v := atomic.LoadUint64(&count)
					if v > uint64((len(rf.peers) - 1)/2){
						rf.mu.Lock()
						rf.state = LEADER
						go rf.sendHeartBeat()
						rf.mu.Unlock()
					}
				}
			}
		}(i)
	}

	//wg.Wait()
	//rf.mu.Lock()
	//if !rf.killed() && rf.currentTerm == sendingTerm && atomic.LoadUint64(&count) > uint64((len(rf.peers)/2-1)) && rf.state == CANDIDATE {
	//	rf.state = LEADER
	//	fmt.Printf("count * 2:%d, total - 1:%d\n", atomic.LoadUint64(&count)*2, len(rf.peers)-1)
	//
	//	go rf.sendHeartBeat()
	//}
	//rf.mu.Unlock()
}

func (rf *Raft) sendHeartBeat() {

	for !rf.killed() {
		timeout := time.Duration(rand.Intn(100)) * time.Millisecond
		var curState STATE
		var sendingTerm int
		var sendingPreLogTerm int
		var sendingPreLogIndex int
		var sendingLeaderCommitIndex int

		rf.mu.Lock()
		curState = rf.state
		if curState != LEADER {
			rf.mu.Unlock()
			return
		}
		sendingTerm = rf.currentTerm
		l := len(rf.log)
		if l != 0 {
			sendingPreLogTerm = rf.log[l-1].Term
			sendingPreLogIndex = rf.log[l-1].Index
		}
		rf.mu.Unlock()

		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			curI := i

			go func() {

				args := &AppendEntriesArgs{
					Term:              sendingTerm,
					LeaderId:          rf.me,
					PrevLogTerm:       sendingPreLogTerm,
					PrevLogIndex:      sendingPreLogIndex,
					LeaderCommitIndex: sendingLeaderCommitIndex,
				}

				reply := &AppendEntriesReply{}

				if !rf.sendAppendEntries(curI, args, reply) {
					return
				}

				//if rf.killed() {
				//	fmt.Printf("During sending heart beat node:%d down\n", rf.me)
				//	return
				//}

				rf.mu.Lock()
				state := rf.state
				term := rf.currentTerm
				rf.mu.Unlock()

				if state != LEADER || term != sendingTerm {
					return
				}

				if reply.Term < sendingTerm {
					return
				}

				if reply.Term > sendingTerm {
					rf.mu.Lock()
					rf.changeToFollower(reply.Term)
					rf.mu.Unlock()
					return
				}

				if reply.Term == sendingTerm && reply.Success == false {
					rf.mu.Lock()
					rf.nextIndex[i] -= 1
					rf.mu.Unlock()
				}

			}()
		}
		for i := 0; i < len(rf.peers); i++ {
			if rf.killed() {
				return
			}
			if rf.me == i {
				continue
			}

			rf.mu.Lock()
			curTerm := rf.currentTerm
			curState := rf.state
			rf.mu.Unlock()

			if curTerm != sendingTerm || curState != LEADER {
				return
			}

			go func(i int) {
				args := &AppendEntriesArgs{
					Term:              sendingTerm,
					LeaderId:          rf.me,
					PrevLogTerm:       sendingPreLogTerm,
					PrevLogIndex:      sendingPreLogIndex,
					LeaderCommitIndex: sendingLeaderCommitIndex,
				}

				reply := &AppendEntriesReply{}

				if rf.sendAppendEntries(i, args, reply) {
					if rf.killed() {
						return
					}

					rf.mu.Lock()
					curTerm := rf.currentTerm
					curState := rf.state
					rf.mu.Unlock()

					if curTerm != sendingTerm || curState != LEADER {
						return
					}

					if reply.Term < sendingTerm {
						return
					}

					if reply.Term > sendingTerm {
						rf.mu.Lock()
						rf.changeToFollower(reply.Term)
						rf.mu.Unlock()
						return
					}
				}
			}(i)

		}
		time.Sleep(timeout)
	}
}

func (rf *Raft) changeToFollower(term int) {
	rf.currentTerm = term
	rf.votedFor = -1
	rf.state = FOLLOWER
}

func (rf *Raft) getElectionTimeout() int {
	return 150 + rand.Intn(151)
}
