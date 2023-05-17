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
	"6.5840/labgob"
	"bytes"
	"fmt"
	//	"bytes"
	"math/rand"
	"strconv"
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
	HEARTBEAT         = 103
	ELEC_TOUT_LOBOUND = HEARTBEAT*2 + 10
	ELEC_TOUT_UPBOUND = ELEC_TOUT_LOBOUND * 1.5
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
	applyCh                  chan<- ApplyMsg
	ElectionTimeoutEventChan <-chan time.Time
	TransferLeaderInfoChan   chan bool
	TransferFollowerInfoChan chan bool
	AppendEntriesReplyChan   chan *AppendEntriesReply
	NewEntryInfoChan         chan int  //new entry index
	NewCommitInfoChan        chan bool //new Commit
	//State:
	state string
	//Persistent state on all servers: Updated on stable storage before responding to RPCs
	currentTerm       int
	votedFor          int
	log               []Entry
	lastIncludedIndex int
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
	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.RLock()
	term = rf.currentTerm
	isleader = rf.isLeader()
	rf.mu.RUnlock()
	return term, isleader
}

// currentTerm:-1 for nil; voteFor:-2 for nil; log nil for nil
func (rf *Raft) setPersistentState(currentTerm int, votedFor int, log []Entry, lastIncludedIndex int, snapshot []byte) {
	someThingChanged := false

	if currentTerm > -1 {
		rf.currentTerm = currentTerm
		someThingChanged = true
	}
	if votedFor > -2 {
		rf.votedFor = votedFor
		someThingChanged = true
	}
	if log != nil {
		rf.log = log
		someThingChanged = true
	}
	if lastIncludedIndex > -1 {
		rf.lastIncludedIndex = lastIncludedIndex
		someThingChanged = true
	}
	if someThingChanged {
		DPrintf("%v %v:SOME Thing Changed, currentTerm:%v ,votedFor:%v, log:%v", rf.state, rf.me, currentTerm, votedFor, log)
		rf.persist(snapshot)
	}
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist(snapshot []byte) {
	// Your code here (2C).
	// Example:
	//rf.mu.RLock()
	//defer rf.mu.RUnlock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	//currentTerm int
	//votedFor    int
	//log         []Entry
	if e.Encode(rf.currentTerm) != nil ||
		e.Encode(rf.votedFor) != nil ||
		e.Encode(rf.log) != nil ||
		e.Encode(rf.lastIncludedIndex) != nil {
		DPrintf("persist failed.")
	} else {
		raftState := w.Bytes()
		rf.persister.Save(raftState, snapshot)
		DPrintf("persist success, raft state:%v", rf.persister.ReadRaftState())
	}
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
	var currentTerm int
	var votedFor int
	var log []Entry
	var lastIncludedIndex int
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&lastIncludedIndex) != nil {
		DPrintf("readPersist failed.")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		rf.lastIncludedIndex = lastIncludedIndex
		DPrintf("readPersist success.currentTerm:%v,votedFor:%v,log:%v,date:%v", currentTerm, votedFor, log, data)
	}
}

func (rf *Raft) logicLogLen() int {
	return len(rf.log) + rf.lastIncludedIndex
}
func (rf *Raft) logic2localIndex(logicIndex int) int {
	return logicIndex - rf.lastIncludedIndex
}
func (rf *Raft) local2logicIndex(localIndex int) int {
	return localIndex + rf.lastIncludedIndex
}

type InstallSnapshotArgs struct {
	Term              int //leader’s term
	LeaderId          int //so follower can redirect clients
	LastIncludedIndex int
	LastIncludedTerm  int //term of lastIncludedIndex
	Snapshot          []byte
}
type InstallSnapshotReply struct {
	Term int //currentTerm, for leader to update itself
}

func (rf *Raft) callInstallSnapshot(server int, args *InstallSnapshotArgs) (ok bool, reply *InstallSnapshotReply) {
	reply = new(InstallSnapshotReply)
	ok = rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		rf.mu.Unlock()
		return
	}
	reLastIn := args.LastIncludedIndex
	if reLastIn < rf.lastIncludedIndex {
		rf.mu.Unlock()
		return
	}
	newLogs := make([]Entry, 0)
	newLogs = append(newLogs, Entry{
		Term:    args.LastIncludedTerm,
		Command: nil,
	})

	if rf.lastIncludedIndex <= reLastIn && reLastIn < rf.logicLogLen() &&
		rf.log[rf.logic2localIndex(reLastIn)].Term == args.LastIncludedTerm {
		//contains last included entry means: THAT entry is committed and applied
		//rf.commitIndex = Max(reLastIn, rf.commitIndex)
		newLogs = rf.log[rf.logic2localIndex(reLastIn):]
	}

	rf.commitIndex = reLastIn
	rf.lastApplied = reLastIn
	rf.setPersistentState(-1, -2, newLogs, reLastIn, args.Snapshot)
	for i := 0; i < len(rf.NewCommitInfoChan); i++ {
		<-rf.NewCommitInfoChan
	}

	//send snapshot for service to update
	applyMsg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Snapshot,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}
	rf.mu.Unlock()
	rf.applyCh <- applyMsg
	HPrintf("%v %v:SUCCESS Install Snapshot, curTerm:%v", rf.state, rf.me, rf.currentTerm)

}

// Snapshot the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	HPrintf("%v %v:Snapshot INVOKED, index:%v , curTerm:%v", rf.state, rf.me, index, rf.currentTerm)
	rf.mu.Lock()
	discardIndex := rf.logic2localIndex(index)
	rf.setPersistentState(-1, -2, rf.log[discardIndex:], index, snapshot)
	rf.mu.Unlock()
	HPrintf("%v %v:Snapshot END, curTerm:%v", rf.state, rf.me, rf.currentTerm)
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry //empty for heartbeat
	LeaderCommit int
}

// only leader invoke this method
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

type AppendEntriesReply struct {
	ConflictTerm           int
	ConflictTermFirstIndex int //use for quickly over incorrect log
	Term                   int
	Success                bool
}

// AppendEntries Followers will be invoked
// heartbeat detect
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.appendEntriesEventHandler(args, reply)
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
	PeerId      int  //used for test
	Term        int  //currentTerm, for candidate to update itself
	VoteGranted bool //true means candidate received vote
}

// RequestVote example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	//todo
	rf.requestVoteEventHandler(args, reply)
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
func (rf *Raft) lock(methodName string) {
	rf.mu.Lock()
	GPrintf("%v %v:%v get lock", rf.state, rf.me, methodName)
}
func (rf *Raft) unlock(methodName string) {
	rf.mu.Unlock()
	GPrintf("%v %v:%v release lock", rf.state, rf.me, methodName)
}

// Start
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately.
// there is no guarantee that this command will ever be committed to the Raft log,
// since the leader may fail or lose an election.
// even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at if it's ever committed.
// the second return value is the current term.
// the third return value is true if this server believes it is the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	GPrintf("%v %v:Start() is invoked,command:%v", rf.state, rf.me, command)
	rf.mu.Lock()
	index := -1
	term := rf.currentTerm
	isLeader := rf.isLeader()
	// Your code here (2B).
	if !isLeader {
		rf.mu.Unlock()
		return index, term, isLeader
	}
	//is Leader
	newEntry := Entry{
		Term:    rf.currentTerm,
		Command: command,
	}
	index = rf.appendLogWithoutLock(newEntry)
	//info ae here comes a new log
	GPrintf("%v %v:Start() is going to write NewEntryInfoChan, index:%v ,command:%v", rf.state, rf.me, index, command)
	rf.printLogs()
	// todo update nextIndex[]
	rf.NewEntryInfoChan <- index
	rf.mu.Unlock()
	GPrintf("%v %v:Start() is end, NewEntryInfoChan <- index, index:%v ,command:%v", rf.state, rf.me, index, command)
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
func (rf *Raft) sendHeartBeatToPeersWithoutLock() {
	args := rf.makeVoidAppendEntriesArgForLeaderWithoutLock()
	rf.sendAppendEntriesToPeersWithoutLock(args, rf.AppendEntriesReplyChan)
}

func (rf *Raft) heartBeatService() {
	for {
		time.Sleep(time.Duration(HEARTBEAT) * time.Millisecond)
		HPrintf("%v %v:BEGIN SENDS HB TO PEERS,curTerm:%v", rf.state, rf.me, rf.currentTerm)
		rf.mu.RLock()
		if rf.state != LEADER || rf.killed() { //double check
			rf.mu.RUnlock()
			break
		}
		rf.sendHeartBeatToPeersWithoutLock()
		DPrintf("%v %v:END SENDS HB TO PEERS,curTerm:%v", rf.state, rf.me, rf.currentTerm)
		rf.mu.RUnlock()
	}
}

func (rf *Raft) appendNewEntry(peer int, tempLog []Entry, newEntryIndex int, successInfoChan chan<- bool, terminateInfoChan <-chan bool) {
	rf.mu.RLock()
	index := rf.nextIndex[peer]
	rf.mu.RUnlock()
	HPrintf("%v %v:appendNewEntry to %v has been called, term:%v, index:%v, newEntryIndex:%v",
		rf.state, rf.me, peer, rf.currentTerm, index, newEntryIndex)
	reply := AppendEntriesReply{
		ConflictTerm:           -1,
		ConflictTermFirstIndex: -1,
		Term:                   0,
		Success:                false,
	}
	retryTimes := 0
	MaxRetryTimes := 6
	oldTerm := rf.currentTerm
	runFlag := true
	for !reply.Success && rf.state == LEADER && !rf.killed() && runFlag { //until success or terminated
		select {
		case <-terminateInfoChan:
			return
		default:
			rf.mu.Lock()
			//if index <= rf.lastIncludedIndex || index > newEntryIndex+1 { // todo ??? index > newEntryIndex+1 ?
			if index > newEntryIndex+1 {
				HPrintf("%v %v:appendNewEntry to %v FINAL failed due to ???, index:%v, newEntryIndex+1:%v", rf.state, rf.me, peer, index, newEntryIndex+1)
				runFlag = false
				rf.mu.Unlock()
				break
			}
			if index <= rf.lastIncludedIndex {
				HPrintf("%v %v:appendNewEntry to %v failed due to left behind try install snapshot, index:%v, newEntryIndex+1:%v", rf.state, rf.me, peer, index, newEntryIndex+1)
				rf.mu.Unlock()
				// todo what if never return
				// todo need to be done
				ok, iSReply := rf.callInstallSnapshot(peer, &InstallSnapshotArgs{
					Term:              rf.currentTerm,
					LeaderId:          rf.me,
					LastIncludedIndex: rf.lastIncludedIndex,
					LastIncludedTerm:  rf.log[0].Term,
					Snapshot:          rf.persister.ReadSnapshot(),
				})

				rf.mu.Lock()
				if !ok { //network failed
					//index = rf.logicLogLen()
					rf.mu.Unlock()
					return
				}
				if iSReply.Term > rf.currentTerm {
					DPrintf("%v %v:appendEntriesReply: callInstallSnapshot find a bigger term:%v, curTerm:%v", rf.state, rf.me, iSReply.Term, rf.currentTerm)
					rf.transferToFollower(iSReply.Term)
					rf.mu.Unlock()
					return
				}
				rf.nextIndex[peer] = newEntryIndex + 1
				rf.matchIndex[peer] = newEntryIndex
				rf.mu.Unlock()
				successInfoChan <- true
				HPrintf("%v %v:appendNewEntry FINAL SUCCESS because callInstallSnapshot, index:%v", rf.state, rf.me, index)
				return
			}
			//}

			arg := rf.makeAEArgsByIndex(index, newEntryIndex+1, tempLog)
			rf.mu.Unlock()

			ok := rf.sendAppendEntries(peer, &arg, &reply) //may return slowly
			if !ok && retryTimes < MaxRetryTimes {
				retryTimes++
				DPrintf("%v %v:appendNewEntry failed due to network, retrying..., retrytime:%v, term:%v, peer:%v, index:%v, newEntryIndex:%v",
					rf.state, rf.me, retryTimes, rf.currentTerm, peer, index, newEntryIndex)
				continue //network fail retry at most MaxRetryTimes times
			} else if retryTimes >= MaxRetryTimes {
				DPrintf("%v %v:appendNewEntry FINAL failed STOP RETRY , index:%v", rf.state, rf.me, index)
				runFlag = false
				break
			}

			if !reply.Success && len(terminateInfoChan) == 0 {
				// todo wait for optimize
				if reply.ConflictTerm != -1 {
					index = reply.ConflictTermFirstIndex
					HPrintf("%v %v:appendNewEntry :reply.ConflictTerm:%v, ConflictTermFirstIndex:%v, index:%v", rf.state, rf.me, reply.ConflictTerm, reply.ConflictTermFirstIndex, index)
				} else {
					index--
				}
				//index--
				rf.mu.Lock()
				rf.nextIndex[peer] = index
				rf.mu.Unlock()
				HPrintf("%v %v:appendNewEntry FAILED, retry with index DECREMENT:%v", rf.state, rf.me, index)
			}
		}
	}

	if reply.Success && rf.state == LEADER && rf.currentTerm == oldTerm && !rf.killed() { // update nextIndex[] matchIndex
		rf.mu.Lock()
		if newEntryIndex+1 > rf.nextIndex[peer] {
			rf.nextIndex[peer] = newEntryIndex + 1
		}
		if newEntryIndex > rf.matchIndex[peer] {
			rf.matchIndex[peer] = newEntryIndex
		}
		rf.mu.Unlock()
		successInfoChan <- true
		HPrintf("%v %v:appendNewEntry to %v FINAL SUCCESS, index:%v, new next Index:%v",
			rf.state, rf.me, peer, index, rf.nextIndex[peer])
		return
	}
	DPrintf("%v %v:appendNewEntry TO %v FINAL failed, index:%v", rf.state, rf.me, peer, index)
	successInfoChan <- false
}

func (rf *Raft) sendAppendEntryToPeers(newEntryIndex int) (chan bool, chan bool) {
	successInfoChan := make(chan bool, 1)
	terminateAppendNewEntryInfoChan := make(chan bool, len(rf.peers))
	tempLog := rf.log[:]
	for peer := range rf.peers {
		if peer != rf.me {
			HPrintf("%v %v:INVOKE rf.appendNewEntry(peer:%v, newEntryIndex:%v), curTerm:%v", rf.state, rf.me, peer, newEntryIndex, rf.currentTerm)
			go rf.appendNewEntry(peer, tempLog, newEntryIndex, successInfoChan, terminateAppendNewEntryInfoChan)
		} else {
			rf.nextIndex[peer] = newEntryIndex + 1
			rf.matchIndex[peer] = newEntryIndex
			continue
		}
	}
	return successInfoChan, terminateAppendNewEntryInfoChan
}
func (rf *Raft) appendNewEntryReplyHandler(successInfoChan <-chan bool, terminateInfoChan <-chan bool) bool {
	successNum := 1
	replyNum := 1
	committed := false
	run := true
	for run {
		select {
		case <-terminateInfoChan:
			run = false
			break
		case success := <-successInfoChan:
			replyNum++
			if success {
				successNum++
			}
			if successNum >= rf.majorityNum { // committed
				committed = true
				run = false
				break
			}
			if replyNum >= len(rf.peers) {
				run = false
				break
			}
		}
	}
	return committed
}

func (rf *Raft) newEntryEventHandler() {
	terminateReplyHandlerChan := make(chan bool, 0)
	aReplyHandlerIsProcessing := false
	var terminateSubRouInfoChan chan bool

	for rf.state == LEADER && !rf.killed() {
		select {
		case newEntryIndex := <-rf.NewEntryInfoChan: //AEArgsChan
			rf.mu.Lock()
			for i := 0; i < len(rf.NewEntryInfoChan); i++ {
				newEntryIndex = <-rf.NewEntryInfoChan
			}
			if aReplyHandlerIsProcessing {
				close(terminateReplyHandlerChan)
				terminateReplyHandlerChan = make(chan bool, 0)
				close(terminateSubRouInfoChan)
				aReplyHandlerIsProcessing = false
			}

			if rf.state != LEADER || rf.killed() { //double check
				rf.mu.Unlock()
				return
			}
			HPrintf("%v %v:SENDS NEW ENTRY TO PEERS, index:%v, curTerm:%v, PEERS NUM:%v", rf.state, rf.me, newEntryIndex, rf.currentTerm, len(rf.nextIndex))

			//processing
			successInfoChan, terminateAppendNewEntryInfoChan := rf.sendAppendEntryToPeers(newEntryIndex)
			terminateSubRouInfoChan = terminateAppendNewEntryInfoChan
			aReplyHandlerIsProcessing = true
			rf.mu.Unlock()

			HPrintf("%v %v:WAITING FOR appendNewEntry Reply..., index:%v, curTerm:%v", rf.state, rf.me, newEntryIndex, rf.currentTerm)
			go func(runFlag *bool) {
				//will block
				committed := rf.appendNewEntryReplyHandler(successInfoChan, terminateReplyHandlerChan)
				*runFlag = false
				if !committed {
					HPrintf("%v %v:appendNewEntry Reply WAITING END:FAILED, New commitIndex:%v, curTerm:%v", rf.state, rf.me, rf.commitIndex, rf.currentTerm)
					return
				}
				rf.mu.Lock()
				if rf.updateCommitIndex() {
					rf.NewCommitInfoChan <- true
				}
				rf.mu.Unlock()
				HPrintf("%v %v:appendNewEntry Reply WAITING END:SUCCESS, New commitIndex:%v, curTerm:%v", rf.state, rf.me, rf.commitIndex, rf.currentTerm)
			}(&aReplyHandlerIsProcessing)

		case <-time.After(time.Duration(HEARTBEAT) * time.Millisecond):
		}
	}
}
func (rf *Raft) updateCommitIndex() bool {
	oldCommitIndex := rf.commitIndex
	for N := rf.commitIndex + 1; N < rf.logicLogLen(); N++ {
		sum := 0
		for _, lastLogIndex := range rf.matchIndex {
			if lastLogIndex >= N {
				sum++
			}
		}
		if sum >= rf.majorityNum && rf.log[rf.logic2localIndex(N)].Term == rf.currentTerm {
			DPrintf("%v %v:UPDATE COMMIT INDEX SUCCESS, Old commitIndex:%v, New commitIndex:%v, curTerm:%v", rf.state, rf.me, rf.commitIndex, N, rf.currentTerm)
			rf.commitIndex = N
		}
	}
	return oldCommitIndex != rf.commitIndex
}
func (rf *Raft) makeAEArgsByIndex(beginIndex int, endIndex int, tempLog []Entry) AppendEntriesArgs {
	arg := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: beginIndex - 1,
		PrevLogTerm:  rf.log[rf.logic2localIndex(beginIndex-1)].Term,
		Entries:      rf.log[rf.logic2localIndex(beginIndex):rf.logic2localIndex(endIndex)], //todo panic: runtime error: slice bounds out of range [39:38]
		LeaderCommit: rf.commitIndex,
	}
	return arg
}

// todo 1.follower routine 2.send new entry every hb
func (rf *Raft) serverRoutines() {
	for {
		select {
		case <-rf.TransferLeaderInfoChan: //server becomes leader begin to do leader routines
			go rf.heartBeatService()
			go rf.newEntryEventHandler()
		}
	}
}

func (rf *Raft) appendEntriesReplyHandler(replyChan chan *AppendEntriesReply) {
	DPrintf(strconv.Itoa(rf.me) + ":AE REPLY HANDLER STARTED")
	for reply := range replyChan {
		if rf.killed() {
			break
		}
		if !rf.isLeader() {
			continue
		}
		if reply != nil {
			DPrintf("%v %v:HANDLED A appendEntriesReply,replyTerm:%v, reply:%v", rf.state, rf.me, reply.Term, reply.Success)
		} else {
			DPrintf("%v %v:HANDLED A nil appendEntriesReply, curTerm:%v", rf.state, rf.me, rf.currentTerm)
		}
		//todo handle appendEntries reply
		//reply maybe nil due to network failure
		if reply != nil && reply.Term > rf.currentTerm {
			DPrintf("%v %v:appendEntriesReply: find a bigger term:%v, curTerm:%v", rf.state, rf.me, reply.Term, rf.currentTerm)
			rf.mu.Lock()
			rf.transferToFollower(reply.Term)
			rf.mu.Unlock()
		}

	}
}

// Check if a leader election should be started.
// The tester requires your Raft to elect a new leader within five seconds of the failure of the old leader
// you will have to use an election timeout larger than the paper's 150 to 300 milliseconds,
// pause for a random amount of time between ELEC_TOUT_LOBOUND and ELEC_TOUT_UPBOUND
func (rf *Raft) ticker() {
	DPrintf(strconv.Itoa(rf.me) + ":TICKER STARTED")
	terminateVoteHandlingChan := make(chan bool, 0)
	aVoteHandlingIsProcessing := false
	for rf.killed() == false {
		// Your code here (2A)
		select {
		case <-rf.ElectionTimeoutEventChan: //handler election timeout event
			//todo!!!!!!!!!!!!!!
			if rf.state == LEADER {
				break
			}
			if aVoteHandlingIsProcessing { //candidate transfer to candidate
				terminateVoteHandlingChan <- true
			} else {
				go func() {
					aVoteHandlingIsProcessing = true
					aVoteHandlingIsProcessing = !rf.electionTimeoutEventHandler(terminateVoteHandlingChan)
				}()
			}
		}
	}
}
func (rf *Raft) appendEntriesEventHandler(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	//todo what if candidate this
	// IfAppendEntries RPC received from new leader: convert to follower
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.ConflictTerm = -1
	reply.ConflictTermFirstIndex = -1
	reply.Term = rf.currentTerm
	reply.Success = false
	if args.Term < rf.currentTerm { //hb from an old leader
		if rf.state == CANDIDATE { //overhead candidate
			reply.ConflictTerm = 1
			reply.ConflictTermFirstIndex = 1
		}
		return
	} else if args.Term >= rf.currentTerm { // there is a new leader or just reset ticker
		rf.transferToFollower(args.Term)
	}

	//handle entries
	lastLogIndex, lastEntry := rf.getLastLogWithoutLock()
	if lastLogIndex < args.PrevLogIndex {
		// todo need to be think deeply
		reply.ConflictTerm = lastEntry.Term
		reply.ConflictTermFirstIndex = lastLogIndex + 1
		return
	}
	if rf.logic2localIndex(args.PrevLogIndex) > 0 &&
		rf.log[rf.logic2localIndex(args.PrevLogIndex)].Term != args.PrevLogTerm { // todo panic: runtime error: index out of range [2] with length 2
		//optimize quickly over incorrect
		reply.ConflictTerm = rf.log[rf.logic2localIndex(args.PrevLogIndex)].Term
		for index, entry := range rf.log {
			if entry.Term == reply.ConflictTerm {
				reply.ConflictTermFirstIndex = rf.local2logicIndex(index)
				break
			}
		}

		return
	}

	if args.Entries == nil { //a heartbeat signal BUT THIS PLACE IS WRONG
		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = Min(args.LeaderCommit, rf.logicLogLen()-1)
			DPrintf("%v %v:UPDATE COMMIT INDEX DRIVE BY HB, args.LeaderCommit:%v, len(rf.log)-1:%v,curTerm:%v, commitIndex:%v",
				rf.state, rf.me, args.LeaderCommit, rf.logicLogLen()-1, rf.currentTerm, rf.commitIndex)
			rf.NewCommitInfoChan <- true
		}
		return
	}

	//appendEntries will success
	reply.Success = true
	//If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (§5.3)
	//if match then do nothing else del and append
	matchFlag := true
	for index, entry := range args.Entries {
		if args.PrevLogIndex+index+1 >= rf.logicLogLen() || //illegal index
			rf.log[rf.logic2localIndex(args.PrevLogIndex+index+1)].Term != entry.Term { //term unmatched
			rf.setPersistentState(-1, -2, rf.log[:rf.logic2localIndex(args.PrevLogIndex+1)], -1, nil) //delete logs after args.PrevLogIndex
			//rf.log = rf.log[:args.PrevLogIndex+1] //delete logs after args.PrevLogIndex
			matchFlag = false
			break
		}
	}

	if !matchFlag { //if unmatched then append new/real entries
		newEntryLen := len(args.Entries)
		for i := 0; i < newEntryLen; i++ {
			rf.appendLogWithoutLock(args.Entries[i])
		}
	}
	//update commitIndex
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = Min(args.LeaderCommit, rf.logicLogLen()-1)
		DPrintf("%v %v:UPDATE COMMIT INDEX, args.LeaderCommit:%v, newLastIndex:%v, curTerm:%v, commitIndex:%v",
			rf.state, rf.me, args.LeaderCommit, rf.logicLogLen()-1, rf.currentTerm, rf.commitIndex)
		rf.NewCommitInfoChan <- true //may deadlock due to Chan is full and blocked and lock is un-release
	}
	HPrintf("%v %v:SUCCESS UPDATE LOGS, curTerm:%v, commitIndex:%v", rf.state, rf.me, rf.currentTerm, rf.commitIndex)
	rf.printLogs()
	return
}

// return true indicates function is over
func (rf *Raft) electionTimeoutEventHandler(terminateSignal <-chan bool) bool {
	//better to take a method lock to prevent reentry this method
	voteReply, originTerm := rf.transferToCandidate()
	if voteReply == nil { //server is leader
		return true
	}

	win := rf.handleVotes(voteReply, terminateSignal)
	if !win { //do not win election
		DPrintf("%v %v:LOSE ELECTION", rf.state, rf.me)
		rf.setPersistentState(-1, -1, nil, -1, nil)
		//rf.votedFor = -1 //reset votedFor
		return true
	}

	if rf.currentTerm == originTerm && rf.isCandidate() { //rf did not change (optimistic lock)
		//todo no-op append entry
		rf.transferToLeader()
	}
	return true
}
func (rf *Raft) transferToLeader() {
	//todo no-op append entry
	rf.mu.Lock()
	rf.state = LEADER
	rf.setPersistentState(-1, -2, nil, -1, nil)
	//rf.votedFor = -1 //reset votedFor
	rf.reInitAfterElection()
	//rf.doNoOp()
	rf.sendHeartBeatToPeersWithoutLock()
	rf.NewEntryInfoChan = make(chan int, 64) //reset NewEntryInfoChan
	rf.TransferLeaderInfoChan <- true        //start leader routine
	if rf.updateCommitIndex() {
		rf.NewCommitInfoChan <- true
	}
	rf.mu.Unlock()
	//DPrintf("%v %v:TRANSFER TO LEADER, noop Index:%v,curTerm:%v", rf.state, rf.me, noopIndex, rf.currentTerm)
	HPrintf("%v %v:TRANSFER TO LEADER,curTerm:%v", rf.state, rf.me, rf.currentTerm)
}

//func (rf *Raft) doNoOp() int {
//	noopEntry := rf.makeNoOpEntry()
//	index := rf.appendLogWithoutLock(noopEntry)
//	//info ae here comes a new log
//	rf.NewEntryInfoChan <- index
//	return index
//}

func (rf *Raft) requestVoteEventHandler(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//default false
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	reply.PeerId = rf.me

	if rf.currentTerm > args.Term { //failed to vote
		return
	}
	if rf.currentTerm < args.Term { //discover a bigger term num
		rf.transferToFollower(args.Term)
	}
	votedFor := rf.votedFor
	if !(votedFor == -1 || votedFor == args.CandidateId) { //do not satisfy requirement
		//otherwise: voted for other candidate already
		return
	}
	// never voted or voted for this candidate then check up-to-date and vote
	meLastLogIndex, entry := rf.getLastLogWithoutLock()
	melastLogTerm := entry.Term
	if melastLogTerm > args.LastLogTerm { // candidate not up-to-date
		return
	}
	if melastLogTerm == args.LastLogTerm && meLastLogIndex > args.LastLogIndex { // candidate not up-to-date
		return
	}
	//candidate up-to-date granted vote
	rf.setPersistentState(-1, args.CandidateId, nil, -1, nil)
	//rf.votedFor = args.CandidateId
	reply.VoteGranted = true
	reply.PeerId = rf.me
	return
}

func (rf *Raft) transferToCandidate() (chan *RequestVoteReply, int) { //return reply and term
	rf.mu.Lock()
	if rf.state == LEADER || rf.votedFor != -1 { //if leader or VOTED! then return
		rf.mu.Unlock()
		return nil, -1
	}
	rf.state = CANDIDATE
	//rf.currentTerm++
	//rf.votedFor = rf.me
	rf.setPersistentState(rf.currentTerm+1, rf.me, nil, -1, nil)
	term := rf.currentTerm
	rf.resetTicker(ELEC_TOUT_LOBOUND, ELEC_TOUT_UPBOUND)
	HPrintf("%v %v:TRANSFER TO CANDIDATE. currentTerm:%v", rf.state, rf.me, term)
	replyChan := rf.sendRequestVoteToPeersWithoutLock()
	rf.mu.Unlock()
	return replyChan, term
}
func (rf *Raft) handleVotes(replyChan chan *RequestVoteReply, terminateSignal <-chan bool) bool { //may take some time
	DPrintf("%v %v:BEGIN HANDLE VOTES, curTerm:%v", rf.state, rf.me, rf.currentTerm)
	voteRes := false
	oldTerm := rf.currentTerm
	tempTerm := rf.currentTerm
	voteNum, totalNum := 1, 1 //include me myself
	/////
	forEnd := false
	for !forEnd {
		select {
		case <-terminateSignal: //need to be terminated
			DPrintf("%v %v:COUNT VOTE IS TERMINATED,totalNum:%v,voteNum:%v,len(rf.peers):%v", rf.state, rf.me, totalNum, voteNum, len(rf.peers))
			forEnd = true
		case reply := <-replyChan: //handle votes
			totalNum++
			if reply != nil {
				DPrintf("%v %v:GET 1 VOTE REPLY FROM %v,totalNum:%v,voteNum:%v,majority:%v", rf.state, rf.me, reply.PeerId, totalNum, voteNum, rf.majorityNum)
			}
			if reply == nil {
				DPrintf("%v %v:NIL VOTE(request vote failed),totalNum:%v,voteNum:%v,majority:%v", rf.state, rf.me, totalNum, voteNum, rf.majorityNum)
			} else if reply.VoteGranted { //get vote
				voteNum++
				DPrintf("%v %v:GRANTED VOTE FROM %v,totalNum:%v,voteNum:%v,majority:%v", rf.state, rf.me, reply.PeerId, totalNum, voteNum, rf.majorityNum)
			} else if reply.Term > tempTerm { //left behind then transfer to follower
				rf.mu.Lock()
				if rf.isCandidate() && (reply.Term > rf.currentTerm) { // candidate transfer to follower
					rf.transferToFollower(reply.Term)
					tempTerm = reply.Term
				}
				rf.mu.Unlock()
			}

			if totalNum >= len(rf.peers) || voteNum >= rf.majorityNum { //all calls return or get major vote
				DPrintf("%v %v:COUNT VOTE OVER,totalNum:%v,voteNum:%v,len(rf.peers):%v", rf.state, rf.me, totalNum, voteNum, len(rf.peers))
				forEnd = true
			}

		}
	}

	if rf.isCandidate() && rf.currentTerm == oldTerm && voteNum >= rf.majorityNum { //become leader
		voteRes = true
	}
	//close(replyChan)
	return voteRes
}

func (rf *Raft) transferToFollower(curTerm int) {
	rf.state = FOLLOWER
	rf.setPersistentState(curTerm, -1, nil, -1, nil)
	//rf.currentTerm = curTerm
	//rf.votedFor = -1 //reset votedFor
	rf.resetTicker(ELEC_TOUT_LOBOUND, ELEC_TOUT_UPBOUND)
	HPrintf("%v %v:TRANSFER TO FOLLOWER,curTerm:%v", rf.state, rf.me, rf.currentTerm)
}
func (rf *Raft) makeVoidAppendEntriesArgForLeaderWithoutLock() *AppendEntriesArgs {
	prev := rf.logicLogLen() - 1
	return &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prev,
		PrevLogTerm:  rf.log[rf.logic2localIndex(prev)].Term,
		Entries:      nil,
		LeaderCommit: rf.commitIndex,
	}
}
func (rf *Raft) sendAppendEntriesToPeersWithoutLock(args *AppendEntriesArgs, replyChan chan *AppendEntriesReply) {
	for id := range rf.peers {
		if id != rf.me { //except me
			go func(peerId int) {
				reply := AppendEntriesReply{}
				ok := rf.sendAppendEntries(peerId, args, &reply) //maybe reply after a long time
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
				var finalReply *RequestVoteReply = nil
				reply := RequestVoteReply{}
				//before := time.Now()
				DPrintf("%v %v:RequestVote RPC TO peer:%v peers:%v", rf.state, rf.me, peerId, len(rf.peers))
				ok := make(chan bool, 1)
				//defer close(ok)
				go func(res chan bool) {
					ok := rf.sendRequestVote(peerId, requestVoteArgs, &reply)
					res <- ok
				}(ok)
				select {
				case res := <-ok:
					if !res {
						//DPrintf("%v %v:RequestVote RPC TO peer:%v FILED with time:%v", rf.state, rf.me, peerId, time.Now().Sub(before).Milliseconds())
						break
					}
					finalReply = &reply
					//case <-time.After(RPC_TIMEOUT * time.Millisecond): //rpc timeout limit
					//	DPrintf("%v %v:RequestVote RPC TO peer:%v TIMEOUT with time:%v", rf.state, rf.me, peerId, time.Now().Sub(before).Milliseconds())
				}
				replyChan <- finalReply
			}(id)
		}
	}
	return replyChan
}
func (rf *Raft) applyCommittedRoutine() { //If commitIndex > lastApplied: increment lastApplied, applylog[lastApplied] to state machine (§5.3)
	DPrintf("%v %v:APPLY COMMITTED ROUTINE", rf.state, rf.me)
	for rf.killed() == false {
		select {
		case <-rf.NewCommitInfoChan:
			DPrintf("%v %v:RECEIVE NewCommitInfo", rf.state, rf.me)
			for !rf.killed() {
				rf.mu.Lock()
				if rf.commitIndex > rf.lastApplied {
					rf.lastApplied++
					GPrintf("%v %v:BEGIN TO APPLY commitIndex:%v lastApplied:%v localIndex:%v", rf.state, rf.me, rf.commitIndex, rf.lastApplied, rf.logic2localIndex(rf.lastApplied))
					entry := rf.log[rf.logic2localIndex(rf.lastApplied)] // todo runtime error: index out of range [2] with length 2 maybe have to lock
					msg := ApplyMsg{
						CommandValid: true,
						Command:      entry.Command,
						CommandIndex: rf.lastApplied,
					}
					rf.mu.Unlock()
					rf.applyCh <- msg
					HPrintf("%v %v:APPLY COMPLETE, command:%v, command index:%v", rf.state, rf.me, msg.Command, msg.CommandIndex)
				} else {
					rf.mu.Unlock()
					break
				}
			}
		}
	}
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

func (rf *Raft) appendLogWithoutLock(e Entry) int {
	rf.setPersistentState(-1, -2, append(rf.log, e), -1, nil) //delete logs after args.PrevLogIndex
	//rf.log = append(rf.log, e)
	return rf.logicLogLen() - 1
}
func (rf *Raft) getLastLogWithoutLock() (int, Entry) {
	last := rf.logicLogLen() - 1
	return last, rf.log[rf.logic2localIndex(last)] //return index and lastLog
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
	rf.applyCh = applyCh
	rf.ElectionTimeoutEventChan = rf.electionTimeoutTicker.C
	rf.TransferLeaderInfoChan = make(chan bool, 1)
	rf.TransferFollowerInfoChan = make(chan bool, 1)
	rf.AppendEntriesReplyChan = make(chan *AppendEntriesReply, rf.majorityNum)
	rf.NewEntryInfoChan = make(chan int, 64)
	rf.NewCommitInfoChan = make(chan bool, 64)
	//State:
	rf.state = FOLLOWER
	//Persistent state on all servers:
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]Entry, 0)
	rf.log = append(rf.log, rf.makeNoOpEntry())
	rf.lastIncludedIndex = 0
	//Volatile state on all servers:
	rf.commitIndex = 0
	rf.lastApplied = 0
	//Volatile state on leaders:
	rf.nextIndex = make([]int, len(rf.peers))
	for i := range rf.nextIndex {
		//initialized to leader last log index + 1
		rf.nextIndex[i] = rf.logicLogLen()
	}
	rf.matchIndex = make([]int, len(rf.peers)) //initialized to 0
	//other const
	rf.majorityNum = (len(rf.peers) + 1) / 2

	// initialize from state persisted before a crash
	//rf.persist()
	rf.readPersist(persister.ReadRaftState())

	//todo bugs may be here (nextIndex/Log)
	HPrintf("%v %v:COME TO LIFE, currentTerm:%v, votedFor:%v", rf.state, rf.me, rf.currentTerm, rf.votedFor)
	rf.printLogs()
	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.serverRoutines()
	go rf.appendEntriesReplyHandler(rf.AppendEntriesReplyChan)
	go rf.applyCommittedRoutine()
	return rf
}
func (rf *Raft) reInitAfterElection() {
	for i := range rf.nextIndex {
		//initialized to leader last log index + 1
		rf.nextIndex[i] = rf.logicLogLen()
	}
	rf.matchIndex = make([]int, len(rf.peers)) //initialized to 0
}
func (rf *Raft) printLogs() {
	res := fmt.Sprintf("%v %v:", rf.state, rf.me)
	for _, e := range rf.log {
		//initialized to leader last log index + 1
		res += fmt.Sprintf("|T:%v C:%v|", e.Term, e.Command)
	}
	HPrintf(res)
}
