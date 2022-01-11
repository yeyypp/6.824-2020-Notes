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
	Command interface{}
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
	// fun := "GetState --->"
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.currentTerm
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
	Term int
	// true means follower contained entry matching prevLogIndex and prevLogTerm
	Success bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {

	// Your code here (2A, 2B).
	fun := "RequestVote Received --->"
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.killed() {
		return
	}
	DPrintf("%s node:%d, term:%d, state:%s, args:%+v\n", fun, rf.me, rf.currentTerm, rf.state, args)

	// Candidate term < currentTerm, return false
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// if args.term > currentTerm, update term, clear votedFor
	if args.Term > rf.currentTerm {
		rf.changeToFollower(args.Term)
	}

	lastLogIndex, lastLogTerm := rf.getLastLogTermAndIndex()

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && (args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)) {
		rf.grantVote(reply, args.CandidateId)
		return
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	fun := "AppendEntries Received --->"
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.killed() {
		return
	}

	DPrintf("%s node:%d, term:%d, state:%s, args:%+v\n", fun, rf.me, rf.currentTerm, rf.state, args)


	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	if args.Term > rf.currentTerm || rf.state == CANDIDATE {
		rf.changeToFollower(args.Term)
	}

	//If you get an AppendEntries RPC with a prevLogIndex that points beyond the end of your log,
	//you should handle it the same as if you did have that entry but the term did not match (i.e., reply false).
	if args.PrevLogIndex != 0 && (len(rf.log) < args.PrevLogIndex || rf.log[args.PrevLogIndex-1].Term != args.PrevLogTerm) {
		reply.Term = rf.currentTerm
		reply.Success = false
		rf.lastElectionTime = time.Now()
		return
	}

	// if log at the first new entry.Index conflicts with existing entry, delete it and all that follow it.
	if args.Entries != nil {
		e := args.Entries[0]
		if e.Index > len(rf.log) {
			rf.log = append(rf.log, args.Entries...)
		} else {
			for _, v := range args.Entries {
				if rf.log[v.Index-1].Term != v.Term {
					rf.log = rf.log[:v.Index-1]
					rf.log = append(rf.log, args.Entries...)
					break
				}
			}
		}
	}

	//The min in the final step (#5) of AppendEntries is necessary,
	//and it needs to be computed with the index of the last new entry.
	//It is not sufficient to simply have the function that applies things from your log
	//between lastApplied and commitIndex stop when it reaches the end of your log.
	//This is because you may have entries in your log that differ from the leader’s log
	//after the entries that the leader sent you (which all match the ones in your log).
	//Because #3 dictates that you only truncate your log if you have conflicting entries,
	//those won’t be removed, and if leaderCommit is beyond the entries the leader sent you,
	//you may apply incorrect entries.
	if args.LeaderCommitIndex > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommitIndex, len(rf.log))
		if rf.commitIndex > rf.lastApplied {
			for rf.commitIndex > rf.lastApplied {
				rf.applyCh <- ApplyMsg{
					CommandValid: true,
					Command: rf.log[rf.lastApplied + 1 - 1].Command,
					CommandIndex: rf.lastApplied + 1,
				}
				rf.lastApplied += 1
			}

		}
	}

	reply.Term = rf.currentTerm
	reply.Success = true
	rf.lastElectionTime = time.Now()

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
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := len(rf.log) + 1
	term := rf.currentTerm
	isLeader := rf.state == LEADER
	if !isLeader {
		return index, term, isLeader
	}
	e := &Entry{}
	e.Term = rf.currentTerm
	e.Index = len(rf.log) + 1
	e.Command = command
	rf.log = append(rf.log, e)

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

	for !rf.killed() {
		timeout := time.Duration(rf.getElectionTimeout()) * time.Millisecond
		time.Sleep(timeout)
		rf.mu.Lock()
		lastElectionTime := rf.lastElectionTime
		state := rf.state
		rf.mu.Unlock()

		if time.Now().Sub(lastElectionTime) >= timeout {
			switch state {
			case FOLLOWER:
				go rf.reElection()
			case CANDIDATE:
				go rf.reElection()
			case LEADER:
				DPrintf("Node:%d is leader now\n", rf.me)
			}
		}
	}
}

func (rf *Raft) reElection() {
	if rf.killed() {
		return
	}

	var sendingTerm int

	rf.mu.Lock()
	rf.currentTerm += 1
	sendingTerm = rf.currentTerm
	rf.votedFor = rf.me
	rf.state = CANDIDATE
	rf.lastElectionTime = time.Now()
	rf.mu.Unlock()

	var count uint64
	for i := 0; i < len(rf.peers); i++ {
		if rf.killed() {
			return
		}
		if rf.me == i {
			continue
		}

		go func(peer int) {
			rf.mu.Lock()
			if rf.currentTerm != sendingTerm || rf.state != CANDIDATE {
				rf.mu.Unlock()
				return
			}

			args := &RequestVoteArgs{}
			reply := &RequestVoteReply{}

			args.Term = rf.currentTerm
			args.CandidateId = rf.me
			args.LastLogIndex = len(rf.log)
			if args.LastLogIndex != 0 {
				args.LastLogTerm = rf.log[len(rf.log)-1].Term
			}
			rf.mu.Unlock()

			if !rf.sendRequestVote(peer, args, reply) {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			// Only if the two terms are the same should you continue processing the reply.
			if reply.Term < rf.currentTerm {
				return
			}

			if reply.Term > rf.currentTerm {
				rf.changeToFollower(reply.Term)
				return
			}

			if rf.state != CANDIDATE {
				return
			}

			if reply.VoteGranted {
				atomic.AddUint64(&count, 1)
				v := atomic.LoadUint64(&count)
				// Can tolerate at most f nodes fail if there are 2f + 1 nodes
				// when count the majority, the number of all nodes include fail nodes, and the vote include itself.
				if v > uint64(len(rf.peers)/2-1) {
					DPrintf("%d becomes leader!\n", rf.me)
					rf.state = LEADER
					for i := 0; i < len(rf.peers); i++ {
						rf.nextIndex[i] = len(rf.log) + 1
						rf.matchIndex[i] = 0
					}
					go rf.sendHeartBeat()
					return
				}
			}
		}(i)
	}

}

func (rf *Raft) sendHeartBeat() {

	for !rf.killed() {
		timeout := time.Duration(rand.Intn(100)) * time.Millisecond
		var sendingTerm int

		rf.mu.Lock()
		if rf.state != LEADER {
			rf.mu.Unlock()
			return
		}
		sendingTerm = rf.currentTerm
		rf.mu.Unlock()

		for i := 0; i < len(rf.peers); i++ {
			if rf.killed() {
				return
			}

			if i == rf.me {
				continue
			}

			go func(peer int) {

				rf.mu.Lock()
				if rf.currentTerm != sendingTerm || rf.state != LEADER {
					rf.mu.Unlock()
					return
				}

				args := &AppendEntriesArgs{}
				reply := &AppendEntriesReply{}

				args.LeaderId = rf.me
				args.Term = rf.currentTerm
				args.LeaderCommitIndex = rf.commitIndex
				args.PrevLogIndex = rf.nextIndex[peer] - 1
				if args.PrevLogIndex > 0 {
					args.PrevLogTerm = rf.log[args.PrevLogIndex-1].Term
				}

				if len(rf.log) != 0 && len(rf.log) >= rf.nextIndex[peer] {
					args.Entries = rf.log[rf.nextIndex[peer]-1:]
				}
				rf.mu.Unlock()

				if !rf.sendAppendEntries(peer, args, reply) {
					return
				}

				rf.mu.Lock()
				defer rf.mu.Unlock()

				// Only if the two terms are the same should you continue processing the reply.
				if reply.Term < rf.currentTerm {
					return
				}

				if reply.Term > rf.currentTerm {
					rf.changeToFollower(reply.Term)
					return
				}

				if rf.state == LEADER {
					if reply.Success {
						rf.nextIndex[peer] = args.PrevLogIndex + len(args.Entries) + 1
						rf.matchIndex[peer] = args.PrevLogIndex + len(args.Entries)

						match := rf.matchIndex[peer]
						if match != 0 {
							count := 1
							for j := 0; j < len(rf.peers); j++ {
								if j == rf.me {
									continue
								}
								if rf.matchIndex[j] >= match {
									count++
								}
								// TODO is this formula correct ?
								if count > (len(rf.peers)-1)/2 && rf.currentTerm == rf.log[match-1].Term {
									rf.commitIndex = max(match, rf.commitIndex)

									for rf.commitIndex > rf.lastApplied {
										rf.applyCh <- ApplyMsg{
											CommandValid: true,
											Command: rf.log[rf.lastApplied + 1 - 1].Command,
											CommandIndex: rf.lastApplied + 1,
										}
										rf.lastApplied += 1
									}
									break
								}
							}
						}
					} else {
						// TODO optimization
						rf.nextIndex[peer] -= 1
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

func (rf *Raft) getLastLogTermAndIndex() (index int, term int) {
	if len(rf.log) == 0 {
		return 0, 0
	}
	return len(rf.log), rf.log[len(rf.log)-1].Term
}

func (rf *Raft) grantVote(reply *RequestVoteReply, candidateId int) {
	reply.VoteGranted = true
	rf.votedFor = candidateId
	rf.lastElectionTime = time.Now()
	reply.Term = rf.currentTerm
}

func min(a, b int) int {
	if a > b {
		return b
	}
	return a
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
