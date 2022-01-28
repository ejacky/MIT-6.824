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
	"log"
	"math/big"
	//	"bytes"
	"crypto/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"MIT-6.824/src/labgob"
	"MIT-6.824/src/labrpc"
)

const Follower = 0
const Candidate = 1
const Leader = 2
const HeartbeatTimeout = 100
const Xdd = 150

const TimeoutMin = 250
const TimeoutMax = 600

const BroadCastHeartBeat = 1
const BroadCastRequestVote = 2

const (
	RPCOk        = iota // Remote accepts the RPC
	RPCRejected         // Remote rejects the RPC
	RPCLost             // RPC Call function returns false
	RPCRetracted        // This node regards the RPC as rejected for some reason
)

func rnd(min int, max int) int {
	x, _ := rand.Int(rand.Reader, big.NewInt(int64(max-min)))
	return int(x.Int64()) + min
}

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

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.RWMutex        // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	timer time.Timer

	term int

	voteFor      int
	receiveVotes int64

	role          int64
	receivedVotes int
	leaderId      int64

	lastRPC Timestamp

	receiveVoteReq     chan struct{}
	receiveHeatBeatReq chan struct{}
}

type Timestamp struct {
	mut sync.Mutex
	t   time.Time
}

func (at *Timestamp) Set() {
	at.mut.Lock()
	defer at.mut.Unlock()
	at.t = time.Now()
}

func (at *Timestamp) Get() time.Time {
	at.mut.Lock()
	defer at.mut.Unlock()
	return at.t
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	// Your code here (2A).
	return rf.term, atomic.LoadInt64(&rf.role) == Leader
}

func (rf *Raft) SetState() {

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
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int
	CandidateId int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term int
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (2A, 2B).
	reply.Term = rf.term
	log.Printf("me %d receive %d requstvote, me term is %d, args term  %d\n", rf.me, args.CandidateId, rf.term, args.Term)

	if rf.term > args.Term {
		reply.Term = rf.term
		return
	}

	if rf.term < args.Term {
		log.Printf("1111 xxxxxxx")

		rf.changeTerm(args.Term, false)

	}

	if rf.voteFor != args.CandidateId && rf.voteFor != -1 {
		log.Printf("xxxx xxxxxxx")
		return
	}

	if rf.voteFor == -1 || rf.voteFor == args.CandidateId {
		rf.voteFor = args.CandidateId
		rf.lastRPC.Set()

		log.Printf("me %d vote to : %d , args term  %d\n", rf.me, args.CandidateId, args.Term)

	}

	return

}

type HeatBeatArgs struct {
	// Your data here (2A, 2B).
	Term     int
	LeaderId int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type HeatBeatReply struct {
	// Your data here (2A).
	Term int
}

func (rf *Raft) HeatBeat(args *HeatBeatArgs, reply *HeatBeatReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	log.Printf("me %d receive %d heatbeat, args term  %d\n", rf.me, args.LeaderId, args.Term)

	reply.Term = rf.term
	if rf.term > args.Term {
		return
	}

	role := atomic.LoadInt64(&rf.role)

	if role == Leader {
		log.Printf("me %d convert leader to foller , args term  %d\n", rf.me, args.Term)
	}

	atomic.StoreInt64(&rf.role, Follower)
	if args.Term > reply.Term {
		rf.changeTerm(args.Term, false)
	}

	atomic.StoreInt64(&rf.leaderId, int64(args.LeaderId))
	rf.lastRPC.Set()
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

func (rf *Raft) sendHeartBeat(server int, args *HeatBeatArgs, reply *HeatBeatReply) bool {
	ok := rf.peers[server].Call("Raft.HeatBeat", args, reply)
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

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		//rand.Seed(time.Now().Unix())
		//timer := time.NewTimer(time.Millisecond * time.Duration(rand.Intn(200)+100))

		last := rf.lastRPC.Get()
		time.Sleep(time.Millisecond * time.Duration(rnd(TimeoutMin, TimeoutMax)))

		curr := rf.lastRPC.Get()
		if last == curr {
			if atomic.LoadInt64(&rf.role) != Follower {
				continue
			}

			for !rf.killed() {
				rf.mu.Lock()
				rf.term++

				term := rf.term
				atomic.StoreInt64(&rf.role, Candidate)
				rf.voteFor = rf.me

				rf.mu.Unlock()

				// 开始选举
				log.Printf("%d start elect, isLeader %t, term %d\n", rf.me, rf.role == Leader, rf.term)

				timeout := time.Millisecond * time.Duration(rnd(TimeoutMin, TimeoutMax))
				atomic.StoreInt64(&rf.receiveVotes, 1)

				rf.broadcast(BroadCastRequestVote, term)

				startTime := time.Now()
				for !rf.killed() && atomic.LoadInt64(&rf.role) == Candidate {
					if time.Since(startTime) > timeout {
						log.Printf("%d elect timeout, isLeader %t, term %d\n", rf.me, rf.role == Leader, rf.term)
						break
					}

					//log.Printf("%d receive votes  %d\n", rf.me, rf.receiveVotes)
					if atomic.LoadInt64(&rf.receiveVotes) > int64(len(rf.peers)/2) {
						log.Printf("%d win, isLeader %t, term %d\n", rf.me, rf.role == Leader, rf.term)
						atomic.StoreInt64(&rf.role, Leader)
						atomic.StoreInt64(&rf.leaderId, int64(rf.me))
						rf.broadcast(BroadCastHeartBeat, term)
					}

					time.Sleep(time.Millisecond * 10)
				}
			}
		}
	}
}

func (rf *Raft) heartbeat() {

	for {
		// 发送心跳
		if term, isLeader := rf.GetState(); isLeader {
			rf.broadcast(BroadCastHeartBeat, term)
		}

		time.Sleep(100 * time.Millisecond)
	}
}

// when happen to a term greater self term
func (rf *Raft) changeTerm(term int, needLock bool) {
	if needLock {
		rf.mu.Lock()
		defer rf.mu.Unlock()
	}

	rf.term = term
	atomic.StoreInt64(&rf.role, Follower)
	rf.voteFor = -1
}

func (rf *Raft) broadcast(bType int, term int) {

	for index := range rf.peers {
		if index == rf.me {
			continue
		}

		switch bType {
		case BroadCastHeartBeat:
			go func(index int) int {
				reply := &HeatBeatReply{}
				ok := rf.sendHeartBeat(index, &HeatBeatArgs{
					Term:     term,
					LeaderId: rf.me,
				}, reply)

				if !ok {
					return RPCLost
				}

				if reply.Term > rf.term {
					rf.changeTerm(reply.Term, false)
					return RPCRetracted
				}

				return RPCOk
			}(index)
		case BroadCastRequestVote:
			go func(index, term int) int {
				args := RequestVoteArgs{}
				reply := RequestVoteReply{}

				args.Term = term
				ok := rf.sendRequestVote(index, &RequestVoteArgs{
					Term:        term,
					CandidateId: rf.me,
				}, &reply)
				if !ok {
					return RPCLost
				}

				if reply.Term > term {
					rf.changeTerm(reply.Term, true)
					return RPCRetracted
				}

				rf.mu.RLock()
				defer rf.mu.Unlock()

				if atomic.LoadInt64(&rf.role) != Candidate || rf.term != term {
					return RPCRetracted
				}

				if reply.Term == term {
					atomic.AddInt64(&rf.receiveVotes, 1)
					return RPCOk
				}

				return RPCRejected
			}(index, term)
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

	// Your initialization code here (2A, 2B, 2C).
	rf.role = Follower

	rf.lastRPC.Set()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.heartbeat()

	return rf
}

func init() {
	log.SetFlags(log.Lmicroseconds)
}
