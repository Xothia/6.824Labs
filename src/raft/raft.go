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
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// ApplyMsg
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

type Entry struct {
	Term    int
	Command interface{}
}

const (
	LEADER    = "Leader"
	CANDIDATE = "Candidate"
	FOLLOWER  = "Follower"
	// HEARTBEAT The tester requires that the leader send heartbeat RPCs no more than ten times per second.
	HEARTBEAT         = 113
	ELEC_TOUT_LOBOUND = HEARTBEAT*2 + 3
	ELEC_TOUT_UPBOUND = ELEC_TOUT_LOBOUND * 2
)

// Raft A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.RWMutex        // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	//timer
	electionTimeoutTicker *time.Ticker
	//channels
	ElectionTimeoutEventChan  <-chan time.Time
	TransferLeaderInfoChan    chan bool
	TransferCandidateInfoChan chan bool
	AppendEntriesReplyChan    chan *AppendEntriesReply
	//State:
	state string
	//Persistent state on all servers:
	currentTerm int
	votedFor    int
	log         []Entry
	//Volatile state on all servers:
	commitIndex int
	lastApplied int
	//Volatile state on leaders:
	nextIndex  []int
	matchIndex []int
	//other const
	majorityNum int
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
}

// GetState return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	//todo
	var term int
	var isleader bool
	// Your code here (2A).

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
	// Your code here (2C).
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

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int
	Entries  []Entry //empty for heartbeat
}

func (rf *Raft) makeHeartbeatArg() *AppendEntriesArgs {
	//ONLY leader will invoke this method
	//TODO
	return &AppendEntriesArgs{
		Term:     rf.currentTerm,
		LeaderId: rf.me, //me must be leader
		Entries:  nil,
	}
}

// only leader invoke this method
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// AppendEntries Followers will be invoked
// heartbeat detect
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	rf.appendEntriesEventHandler(args, reply)
	//if rf.state==CANDIDATE{
	//
	//}
	//AppendEntries tells leader still alive then reset the electionTimeoutTicker
	return nil
}

// RequestVoteArgs
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// RequestVoteReply
// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term        int  //currentTerm, for candidate to update itself
	VoteGranted bool //true means candidate received vote
}

// RequestVote example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	// Your code here (2A, 2B).
	//todo
	rf.requestVoteEventHandler(args, reply)
	return nil
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

// Start
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

	return index, term, isLeader
}

// Kill
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
func (rf *Raft) leaderRoutines() {
	for {
		select {
		case <-rf.TransferLeaderInfoChan: //server becomes leader begin to do leader routines
			for rf.state == LEADER && rf.killed() == false { //if not be killed and still leader then
				//todo sends hb
				rf.mu.RLock()
				args := rf.makeVoidAppendEntriesArgForLeaderWithoutLock()
				args.Entries = rf.log
				rf.mu.RUnlock()
				rf.sendAppendEntriesToPeersWithoutLock(args, rf.AppendEntriesReplyChan)
				time.Sleep(time.Duration(HEARTBEAT) * time.Millisecond)
			}
		}
	}
}

// Check if a leader election should be started.
// The tester requires your Raft to elect a new leader within five seconds of the failure of the old leader
// you will have to use an election timeout larger than the paper's 150 to 300 milliseconds,
// pause for a random amount of time between ELEC_TOUT_LOBOUND and ELEC_TOUT_UPBOUND
func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here (2A)
		select {
		case <-rf.ElectionTimeoutEventChan: //handler election timeout event
			//todo
			rf.electionTimeoutEventHandler()
		}

	}
}
func (rf *Raft) appendEntriesEventHandler(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	if rf.isCandidate() || rf.isLeader() {
		//todo handle hb
		// IfAppendEntries RPC received from new leader: convert to follower
		//If the leader’s term (included in its RPC) is at least
		//as large as the candidate’s current term, then the candidate
		//recognizes the leader as legitimate and returns to follower
		//state.
		if args.Entries == nil { //a heartbeat signal
			if rf.currentTerm > args.Term { //hb from an old leader
				return
			}
			// there is a new leader
			rf.mu.Lock()
			rf.currentTerm = args.Term
			rf.transferToFollower()
			rf.mu.Unlock()
		} else {

		}
	} else if rf.isFollower() {
		//todo check term or other things
		rf.resetTickerWithLock(ELEC_TOUT_LOBOUND, ELEC_TOUT_UPBOUND)
	} //else if rf.isLeader()

}
func (rf *Raft) electionTimeoutEventHandler() {
	//better to take a method lock to prevent reentry this method
	voteReply, originTerm := rf.transferToCandidate()
	if voteReply == nil { //server is leader
		return
	}

	win := rf.handleVotes(voteReply)
	if !win { //do not win election
		return
	}

	if rf.currentTerm == originTerm && rf.isCandidate() { //rf did not change (optimistic lock)
		//todo no-op append entry
		rf.transferToLeader()
	}
}
func (rf *Raft) transferToLeader() {
	//todo no-op append entry
	rf.mu.Lock()
	rf.state = LEADER
	rf.TransferLeaderInfoChan <- true //start leader routine
	rf.mu.Unlock()
}

func (rf *Raft) requestVoteEventHandler(args *RequestVoteArgs, reply *RequestVoteReply) {
	//todo
}
func (rf *Raft) appendEntriesReplyHandler(replyChan chan *AppendEntriesReply) {
	for reply := range replyChan {
		//todo handle reply
		if rf.killed() {
			break
		}
		if !rf.isLeader() {
			continue
		}
		log.Fatal(reply.Term)
	}
}
func (rf *Raft) transferToCandidate() (chan *RequestVoteReply, int) { //return reply and term
	rf.mu.Lock()
	if rf.state == LEADER { //if leader then return
		rf.mu.Unlock()
		return nil, -1
	}
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.resetTicker(ELEC_TOUT_LOBOUND, ELEC_TOUT_UPBOUND)
	replyChan := rf.sendRequestVoteToPeersWithoutLock()
	rf.mu.Unlock()
	return replyChan, rf.currentTerm
}
func (rf *Raft) handleVotes(replyChan chan *RequestVoteReply) bool { //may take some time
	voteRes := false
	oldTerm := rf.currentTerm
	tempTerm := rf.currentTerm
	voteNum, totalNum := 0, 0
	for reply := range replyChan { //count the votes
		totalNum++
		if reply == nil {
			continue
		}
		if reply.VoteGranted { //get vote
			voteNum++
		} else if reply.Term > tempTerm { //left behind then transfer to follower
			rf.mu.Lock()
			if rf.isCandidate() && (reply.Term > rf.currentTerm) { // candidate transfer to follower
				rf.transferToFollower()
				rf.currentTerm = reply.Term
				tempTerm = reply.Term
			}
			rf.mu.Unlock()
		}
		if totalNum >= len(rf.peers) { //all calls return
			break
		}
	}
	if rf.isCandidate() && rf.currentTerm == oldTerm && voteNum >= rf.majorityNum { //become leader
		voteRes = true
	}
	close(replyChan)
	return voteRes
}
func (rf *Raft) transferToFollower() {
	rf.state = FOLLOWER
	rf.resetTicker(ELEC_TOUT_LOBOUND, ELEC_TOUT_UPBOUND)
}

func (rf *Raft) makeVoidAppendEntriesArgForLeaderWithoutLock() *AppendEntriesArgs {
	return &AppendEntriesArgs{
		Term:     rf.currentTerm,
		LeaderId: rf.me,
		Entries:  nil,
	}
}
func (rf *Raft) sendAppendEntriesToPeersWithoutLock(args *AppendEntriesArgs, replyChan chan *AppendEntriesReply) {
	for id := range rf.peers {
		if id != rf.me { //except me
			go func(peerId int) {
				reply := AppendEntriesReply{}
				ok := rf.sendAppendEntries(peerId, args, &reply)
				if ok {
					replyChan <- &reply
				} else {
					replyChan <- nil
				}
			}(id)
		}
	}
}
func (rf *Raft) sendRequestVoteToPeersWithoutLock() chan *RequestVoteReply {
	replyChan := make(chan *RequestVoteReply, rf.majorityNum)
	requestVoteArgs := rf.makeRequestVoteArgForCandidateWithoutLock()
	for id := range rf.peers {
		if id != rf.me { //except me
			go func(peerId int) {
				reply := RequestVoteReply{}
				ok := rf.sendRequestVote(peerId, requestVoteArgs, &reply)
				if ok {
					replyChan <- &reply
				} else {
					replyChan <- nil
				}
			}(id)
		}
	}
	return replyChan
}
func (rf *Raft) makeRequestVoteArgForCandidateWithoutLock() *RequestVoteArgs {
	index, lastLog := rf.getLastLogWithoutLock()
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: index,
		LastLogTerm:  lastLog.Term,
	}
	return args
}
func (rf *Raft) makeNoOpEntry() Entry {
	return Entry{
		Term:    rf.currentTerm,
		Command: nil,
	}
}

func (rf *Raft) resetTicker(lowerBound int64, upperBound int64) {
	ms := lowerBound + (rand.Int63() % (upperBound - lowerBound))
	rf.electionTimeoutTicker.Reset(time.Duration(ms) * time.Millisecond)
}
func (rf *Raft) resetTickerWithLock(lowerBound int64, upperBound int64) {
	rf.mu.RLock()
	rf.resetTicker(lowerBound, upperBound)
	rf.mu.RUnlock()
}
func (rf *Raft) appendLogWithoutLock(e Entry) {
	rf.log = append(rf.log, e)
}
func (rf *Raft) getLastLogWithoutLock() (int, Entry) {
	return len(rf.log), rf.log[len(rf.log)] //return index and lastLog
}
func (rf *Raft) isCandidate() bool {
	return rf.state == CANDIDATE
}
func (rf *Raft) isLeader() bool {
	return rf.state == LEADER
}
func (rf *Raft) isFollower() bool {
	return rf.state == FOLLOWER
}

// Make : the service or tester wants to create a Raft server. the ports
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
	//timer
	ms := ELEC_TOUT_LOBOUND + (rand.Int63() % (ELEC_TOUT_UPBOUND - ELEC_TOUT_LOBOUND))
	rf.electionTimeoutTicker = time.NewTicker(time.Duration(ms) * time.Millisecond)
	//channels
	rf.ElectionTimeoutEventChan = rf.electionTimeoutTicker.C
	rf.TransferLeaderInfoChan = make(chan bool, 1)
	rf.TransferCandidateInfoChan = make(chan bool, 1)
	rf.AppendEntriesReplyChan = make(chan *AppendEntriesReply, rf.majorityNum)
	//State:
	rf.state = FOLLOWER
	//Persistent state on all servers:
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]Entry, 0)
	rf.appendLogWithoutLock(rf.makeNoOpEntry())
	//Volatile state on all servers:
	rf.commitIndex = 0
	rf.lastApplied = 0
	//Volatile state on leaders:
	rf.nextIndex = make([]int, len(rf.peers))
	for i := range rf.nextIndex {
		//initialized to leader last log index + 1
		rf.nextIndex[i] = len(rf.log)
	}
	rf.matchIndex = make([]int, len(rf.peers)) //initialized to 0
	//other const
	rf.majorityNum = (len(rf.peers) + 1) / 2

	// initialize from state persisted before a crash
	//rf.readPersist(persister.ReadRaftState())
	//todo bugs may be here (nextIndex/Log)

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.leaderRoutines()
	go rf.appendEntriesReplyHandler(rf.AppendEntriesReplyChan)
	return rf
}
