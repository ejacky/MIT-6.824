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
	"math"
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

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
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

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.RWMutex        // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	timer        time.Timer
	receiveVotes int64
	role         int64
	leaderId     int64
	lastRPC      Timestamp

	// persistent state on all servers
	term    int
	voteFor int
	log     []LogEntry

	// volatile state on all servers
	commitIndex int

	// volatile state on leaders
	nextIndex  []int // 从该所有以后的都要复制
	matchIndex []int
}

type Timestamp struct {
	mut sync.Mutex
	t   time.Time
}

type LogEntry struct {
	Command interface{}
	Term    int
}

type AppendEntriesArgs struct {
	// Your data here (2A, 2B).
	Term     int
	LeaderId int

	PrevLogIndex int
	PrevLogTerm  int

	Entries []LogEntry

	LeaderCommit int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type AppendEntriesReply struct {
	// Your data here (2A).
	Term    int
	Success bool
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

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
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

// restore previously persisted state.
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

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
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
	Term int
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (2A, 2B).
	reply.Term = args.Term
	log.Printf("me %d receive %d requstvote, me term is %d, args term  %d\n", rf.me, args.CandidateId, rf.term, args.Term)

	if rf.term > args.Term {
		reply.Term = rf.term
		return
	}

	if rf.term < args.Term {
		log.Printf("1111 xxxxxxx")

		rf.changeTerm(args.Term, false, false)

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

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
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
		rf.changeTerm(args.Term, false, true)
	}

	atomic.StoreInt64(&rf.leaderId, int64(args.LeaderId))
	rf.lastRPC.Set()

	reply.Success = true
	// 心跳
	if len(args.Entries) == 0 {
		return
	}

	logLens := len(rf.log)

	// 原来无日志
	if rf.commitIndex == -1 {

		rf.log = append(rf.log[0:], args.Entries...)
		rf.commitIndex = logLens - 1
		return
	}

	if args.PrevLogIndex > logLens-1 {
		reply.Success = false
		return
	}

	// follower 中的日志与 Entries 均不相同
	if args.PrevLogIndex == -1 {
		rf.log = append(rf.log[0:], args.Entries...)
		rf.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(len(rf.log))))
		return
	}

	// leader 的日志比 follower 多， 否则不会被选择为 leader
	if rf.commitIndex <= args.LeaderCommit && rf.log[args.PrevLogIndex].Term == args.PrevLogTerm && rf.commitIndex == args.LeaderCommit {
		reply.Success = true
		rf.log = append(rf.log[:args.PrevLogIndex], args.Entries...)

		rf.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(len(rf.log))))
	} else if rf.commitIndex > args.LeaderCommit { // leader 中的日志比 follower 中还少， 异常情况
		log.Printf("failure happen , follow commitIndex %d > leader commitIndex %d", rf.commitIndex, args.LeaderCommit)
	}
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
// Call() is guaranteed to return (perhaps after a delay) *except* if thef
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	index = rf.commitIndex + 1
	term, isLeader = rf.GetState()
	if isLeader == true {
		rf.log = append(rf.log, LogEntry{
			Command: command,
			Term:    term,
		})
	}

	rf.nextIndex = make([]int, len(rf.peers))

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
					//log.Printf("333333")

					log.Printf("%d receive votes  %d\n", rf.me, atomic.LoadInt64(&rf.receiveVotes))
					if atomic.LoadInt64(&rf.receiveVotes) > int64(len(rf.peers)/2) {
						log.Printf("%d win, isLeader %t, term %d\n", rf.me, rf.role == Leader, rf.term)
						rf.initFollower()

						atomic.StoreInt64(&rf.role, Leader)
						atomic.StoreInt64(&rf.leaderId, int64(rf.me))
						rf.broadcast(BroadCastHeartBeat, term)
					}

					time.Sleep(time.Millisecond * 10)
				}

				if atomic.LoadInt64(&rf.role) != Candidate {
					break
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
func (rf *Raft) changeTerm(term int, needLock bool, needRefreshRPC bool) {
	if needLock {
		rf.mu.Lock()
		defer rf.mu.Unlock()
	}

	rf.term = term
	atomic.StoreInt64(&rf.role, Follower)
	rf.voteFor = -1

	if needRefreshRPC {
		rf.lastRPC.Set()
	}
}

func (rf *Raft) broadcast(bType int, term int) {

	for peerId := range rf.peers {
		if peerId == rf.me {
			continue
		}

		switch bType {
		case BroadCastHeartBeat:
			go func(peerId, term int, logs1 []LogEntry) int {
				reply := &AppendEntriesReply{}
				entries := []LogEntry{}
				preIndex := -1
				preTerm := -1

				tryNum := 3
				nextIndex := rf.nextIndex[peerId]
				if len(rf.log) > 0 && nextIndex < len(rf.log) {
					entries = rf.log[nextIndex:]
					preIndex = nextIndex - 1
					preTerm = rf.log[nextIndex].Term
				}
				logs := rf.log
				for reply.Success == false && tryNum > 0 {
					ok := rf.sendAppendEntries(peerId, &AppendEntriesArgs{
						Term:         term,
						LeaderId:     rf.me,
						Entries:      entries,
						PrevLogIndex: preIndex,
						PrevLogTerm:  preTerm,
						LeaderCommit: rf.commitIndex,
					}, reply)
					if !ok {
						tryNum--
						continue
					}
					nextIndex--
					if nextIndex < 0 {
						log.Println("nextIndex < 0 , log replicate err")
						break
					}

					if len(entries) == 0 {
						break
					}
				}

				if reply.Success == true {
					rf.nextIndex[peerId] = len(logs)
					rf.matchIndex[peerId] = len(logs) - 1
				}

				rf.mu.RLock()
				currentTerm := rf.term
				rf.mu.RUnlock()

				// todo 先 changeTerm 还是先完成复制？
				if reply.Term > term {
					rf.changeTerm(reply.Term, true, true)
					return RPCRetracted
				} else if currentTerm > term {
					return RPCRetracted
				}

				return RPCOk
			}(peerId, term, rf.log)
		case BroadCastRequestVote:
			go func(peerId, term int) int {
				args := RequestVoteArgs{}
				reply := RequestVoteReply{}

				args.Term = term
				ok := rf.sendRequestVote(peerId, &RequestVoteArgs{
					Term:        term,
					CandidateId: rf.me,
				}, &reply)
				if !ok {
					return RPCLost
				}

				if reply.Term > term {
					rf.changeTerm(reply.Term, true, true)
					return RPCRetracted
				}

				rf.mu.RLock()
				defer rf.mu.RUnlock()

				if atomic.LoadInt64(&rf.role) != Candidate || rf.term != term {
					return RPCRetracted
				}
				log.Printf("3333 xxxxxxx reply term : %d, term :%d", reply.Term, term)

				if reply.Term == term {
					//log.Printf("3333 xxxxxxx")

					atomic.AddInt64(&rf.receiveVotes, 1)
					return RPCOk
				}

				return RPCRejected
			}(peerId, term)
		}
	}
}

func (rf *Raft) initFollower() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	logNum := len(rf.log)

	rf.nextIndex = make([]int, 0)
	rf.matchIndex = make([]int, 0)
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex = append(rf.nextIndex, logNum)
		rf.matchIndex = append(rf.matchIndex, -1)
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

	rf.commitIndex = -1

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
