package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   创建一个新的 Raft 服务器
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   在添加新日志项上达成一致
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   获取 Raft 的当前轮次以及其是否为领导者
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   每当新的记录被添加到日志中时，每一个 Raft 成员都需要
//   向同一个 service 服务器发送 ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"math/rand"
	"sync"
	"time"
)
import "sync/atomic"
import "../labrpc"

// import "bytes"
// import "../labgob"

//
// 当每一个 Raft 节点察觉到下一条日志项被提交之后，该节点就会发送 ApplyMsg 到服务器。
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

// 日志项
type LogEntry struct {
	LogIndex int
	LogTerm  int
	Command  interface{}
}
type RaftState int

const (
	Follower = iota
	Leader
	Candidate
)

type RaftChans struct {
	chanHeartBeat chan int
	chanGrantVote chan int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	funcMu    sync.Mutex          // 函数级别的锁
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	leaderId       int
	votedFor       int
	currentTerm    int
	state          RaftState
	applyCh        chan ApplyMsg
	heartBeatTimer *time.Timer
	electionTimer  *time.Timer
	chs            RaftChans

	nextLogIndex int
	logs         []LogEntry
	commitIndex  int
	lastApplied  int
	// 选举后需要重新初始化
	nextIndex  []int
	matchIndex []int
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

func (rf *Raft) indexArrayInit() {

	nextIndex := rf.getNextLogIndex()
	// _, _ = DBPrintf("Leader %v, index array init, next index: %d, time: %v", rf.me, nextIndex, time.Now())

	for i := range rf.peers {
		rf.setNextIndex(i, nextIndex)

		rf.setMatchIndex(i, 0)
	}
	//for i, _ := range rf.peers {
	//	// 此处不能使用 rf.getNextIndex, 因为前面已经加锁了，这里会出现死锁！！！！
	//	_, _ = DBPrintf("nextIndex[%d]: %v", i, rf.nextIndex[i])
	//}

}

func (rf *Raft) getNextLogIndex() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.nextLogIndex
}

func (rf *Raft) setNextLogIndex(idx int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.nextLogIndex = idx
}
func (rf *Raft) getCommitIndex() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.commitIndex
}
func (rf *Raft) setCommitIndex(idx int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.commitIndex = idx
}
func (rf *Raft) getLastApplied() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.lastApplied
}
func (rf *Raft) getNextIndex(peer int) int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.nextIndex[peer]
}

func (rf *Raft) setNextIndex(peer, idx int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if idx <= 0 {
		idx = 1
	}
	rf.nextIndex[peer] = idx
}

func (rf *Raft) setMatchIndex(peer, idx int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if idx < 0 {
		idx = 0
	}
	rf.matchIndex[peer] = idx
}
func (rf *Raft) getMatchIndex(peer int) int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.matchIndex[peer]
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.getCurrentTerm()
	isleader = rf.getState() == Leader
	return term, isleader
}

func (rf *Raft) getCurrentTerm() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm
}

func (rf *Raft) getState() RaftState {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.state
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

type AppendEntriesArgs struct {
	IsHeartBeat  bool
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int

	LogEntries   []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// 添加日志，心跳检测
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.funcMu.Lock()
	defer rf.funcMu.Unlock()

	curTerm := rf.getCurrentTerm()
	if args.Term < curTerm { // 旧 Term 的信息
		reply.Term = curTerm
		reply.Success = false
		return
	}
	if args.Term > curTerm { // 发现自己 Term 落后了，直接转为 Follower
		_, _ = DBPrintf("%v: term is not up-to-date, new term: %v", rf.me, args.Term)
		rf.becomeFollower(args.Term)
		curTerm = args.Term
	} else { // Term 一致，直接 reset 选举时钟
		rf.electionTimer.Reset(getRandElectionTimeout()) //重新计算选举时间
	}

	reply.Term = rf.getCurrentTerm()
	if args.IsHeartBeat { // 心跳检测
		_, _ = DAPrintf("Follower %v: receive heart from leader %d", rf.me, args.LeaderId)
		reply.Success = true
		// commit？
		oldCommitIndex := rf.getCommitIndex()
		if args.LeaderCommit > oldCommitIndex {
			// Done: set commit index ?
			newCommitIndex := min(rf.logs[len(rf.logs)-1].LogIndex, args.LeaderCommit)

			_, _ = DBPrintf("Follower %v: change commit index to %d", rf.me, newCommitIndex)
			go rf.followerCommit(oldCommitIndex, newCommitIndex)
		}

	} else { // 需要同步日志
		// Done: 完善添加日志的部分
		_, _ = DBPrintf("Follower %v: receive entries from leader %d, args: %+v", rf.me, args.LeaderId, args)
		if args.PrevLogIndex > len(rf.logs)-1 || args.PrevLogIndex < rf.getCommitIndex() { // 需要 leader 回退 follower's PrevLogIndex
			reply.Success = false
		} else if rf.logs[args.PrevLogIndex].LogTerm == args.PrevLogTerm { // match
			// 合并最新的 log entries
			//_, _ = DBPrintf("Follower %v: receive new entry, ", rf.me)
			rf.logs = rf.logs[:args.PrevLogIndex+1]
			rf.logs = append(rf.logs, args.LogEntries[:]...)
			// 修改 raft nextLogIndex，以免投票时出错
			rf.setNextLogIndex(len(rf.logs))
			_, _ = DBPrintf("Follower %v's logs up-to-date, logs' length: %d, args: %+v", rf.me, len(rf.logs), args)
			_, _ = DBPrintf("Follower %v: next command %v", rf.me, rf.logs[rf.getNextLogIndex()-1].LogIndex)
			reply.Success = true
			oldCommitIndex := rf.getCommitIndex()
			if args.LeaderCommit > oldCommitIndex {
				// TODO: set commit index ?
				newCommitIndex := min(rf.logs[len(rf.logs)-1].LogIndex, args.LeaderCommit)

				_, _ = DBPrintf("Follower %v: change commit index to %d", rf.me, newCommitIndex)
				go rf.followerCommit(oldCommitIndex, newCommitIndex)
			}
		} else { // not match
			// follower log 回退，删除后面的不匹配的所有 log entries
			rf.logs = rf.logs[:args.PrevLogIndex]
			// rf.setNextLogIndex(args.PrevLogIndex + 1)
			rf.setNextLogIndex(len(rf.logs))
			reply.Success = false
		}
	}
}

// followers commit logs
func (rf *Raft) followerCommit(startIndex, endIndex int) {
	rf.funcMu.Lock()
	defer rf.funcMu.Unlock()

	rf.setCommitIndex(endIndex)
	for idx := startIndex + 1; idx <= endIndex; idx++ {
		applyMsg := ApplyMsg{
			CommandValid: true,
			Command:      rf.logs[idx].Command,
			CommandIndex: idx,
		}
		// _, _ = DBPrintf("Follower %v: log command: %v", rf.me, applyMsg.Command)
		_, _ = DBPrintf("Follower %v: log(%d) committed", rf.me, applyMsg.CommandIndex)
		rf.applyCh <- applyMsg
	}
}

func min(var1, var2 int) int {
	if var1 < var2 {
		return var1
	}
	return var2
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
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

func isCandidateUpToDate(curTerm, curIndex int, candidateTerm, candidateIndex int) bool {
	_, _ = DBPrintf("myLastLogTerm: %d, myLastLogIndex: %d, candidateLastLogTerm: %d, candidateLastLogIndex: %d", curTerm, curIndex, candidateTerm, candidateIndex)
	if candidateTerm != curTerm {
		return candidateTerm > curTerm
	}
	return candidateIndex >= curIndex
}

//
// example RequestVote RPC handler.\
// Done: 该函数中容易出现 Data Race 的情况，需要加锁改进
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	_, _ = DAPrintf("%v received vote requested form %v at term %v", rf.me, args.CandidateId, rf.getCurrentTerm())
	_, _ = DAPrintf("%+v", args)
	// Your code here (2A, 2B).
	reply.VoteGranted = false
	// 发现自己不是最新的 Term
	if args.Term > rf.getCurrentTerm() {
		rf.becomeFollower(args.Term)
		// rf.setVotedFor(args.CandidateId)
		_, _ = DBPrintf("Follower %v: find higher term from candidate %d, current term: %d", rf.me, args.CandidateId, args.Term)
		// reply.Term = args.Term
		// reply.VoteGranted = true
		// return
	}
	// ！！这句一定要在 becomeFollower 后面，不然会导致成为 follower 后无法选举！！！
	currentState := rf.getState()
	// 候选人 Term 较小，或者当前服务器不是 Follower (Leader/Candidate)
	if args.Term < rf.getCurrentTerm() || currentState != Follower {
		if currentState != Follower {
			_, _ = DBPrintf("Follower %v: became Candidate/Leader before, not vote for candidate(%d)", rf.me, args.CandidateId)
		} else {
			_, _ = DBPrintf("Follower %v: candidate(%d) has old term %d, my term: %d", rf.me, args.CandidateId, args.Term, rf.getCurrentTerm())
		}

		reply.Term = rf.getCurrentTerm()
		reply.VoteGranted = false
		return
	}

	// Trem 一致，服务器为 Follower，且在本轮未参与投票，则为候选人投票
	// 此处要加函数级别的锁，避免多线程同时判断并修改 rf.votedFor !!!
	// rf.getVotedFor() != -1 这个判断语句与 isCandidateUpToDate() 应该放在一起
	rf.funcMu.Lock()
	defer rf.funcMu.Unlock()
	if rf.getVotedFor() != -1 { // 本轮已参与投票
		_, _ = DBPrintf("Follower %v: voted at this term earlier", rf.me)
		reply.Term = args.Term
		reply.VoteGranted = false
	} else { // Done 根据 Candidate log 长度确定是否投票
		// 注意这里的 myLastLogIndex 和 myLastLogTerm，需据此确定是否给 candidate 投票
		// 这里写错了的话，出现 Leader 分区后重新加入的情况会很麻烦
		myLastLogIndex := rf.getNextLogIndex() - 1
		myLastLogTerm := rf.logs[myLastLogIndex].LogTerm
		// 需要判断 candidate 日志是否 up-to-date
		if isCandidateUpToDate(myLastLogTerm, myLastLogIndex, args.LastLogTerm, args.LastLogIndex) {
			_, _ = DBPrintf("Follower %v: candidate %d's log is least as up-to-date, vote him, %+v, myLastLogTerm: %d, myLastLogIndex: %d", rf.me, args.CandidateId, args, myLastLogTerm, myLastLogIndex)
			rf.setVotedFor(args.CandidateId)
			reply.Term = args.Term
			reply.VoteGranted = true
		} else {
			_, _ = DBPrintf("Follower %v: candidate %d's log is not least as up-to-date, %+v, myLastLogTerm: %d, myLastLogIndex: %d", rf.me, args.CandidateId, args, myLastLogTerm, myLastLogIndex)
			reply.Term = args.Term
			reply.VoteGranted = false
		}

	}

	rf.electionTimer.Reset(getRandElectionTimeout())
}

func (rf *Raft) getVotedFor() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.votedFor
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
	// 函数加锁，避免并发时出现问题
	rf.funcMu.Lock()
	defer rf.funcMu.Unlock()

	isLeader := rf.getState() == Leader
	// Your code here (2B).
	if !isLeader {
		return -1, -1, isLeader
	}

	// index 是 command 即将插入的下标
	index := rf.getNextLogIndex()
	term := rf.getCurrentTerm()
	_, _ = DBPrintf("Leader %v: start a command at term %d, log index: %d", rf.me, term, index)
	logEntry := LogEntry{
		LogIndex: index,
		LogTerm:  term,
		Command:  command,
	}
	rf.logs = append(rf.logs[:index], logEntry)
	rf.setNextLogIndex(index + 1)
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
// service 或者 tester 需要创建 Raft 服务器。peers[] 数组包含了
// 所有的 Raft 服务器的端口（包括当前服务器的端口）。当前服务器端口是 peers[me]。
// 所有服务器的 peers 数组都是一样的。Raft 服务器在 persister 中存储其持久状态，
// 如果服务器有最近存储的状态，persister 就会被初始化成这些状态。
// TODO： applyCh 是
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.applyCh = applyCh
	rf.chs.chanGrantVote = make(chan int)
	rf.chs.chanHeartBeat = make(chan int)

	rf.setState(Follower)
	rf.setVotedFor(-1)
	rf.setCurrentTerm(0)
	rf.setNextLogIndex(1)
	rf.setCommitIndex(0)

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.indexArrayInit()
	// 每个 Raft 节点都会初始化一个空日志，使得 commitIndex 从 0 开始
	rf.logs = []LogEntry{{0, 0, nil}}
	go rf.startCommitDetectionLoop()
	go rf.startElectionLoop()
	go rf.startHeartBeatLoop()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rand.Seed(time.Now().UnixNano())
	return rf
}

func (rf *Raft) setVotedFor(vf int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.votedFor = vf
}

func (rf *Raft) setState(state RaftState) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.state = state
}

func (rf *Raft) becomeCandidate() {
	rf.setState(Candidate)
	rf.setCurrentTerm(rf.getCurrentTerm() + 1)
	// 给自己投票
	rf.setVotedFor(rf.me)
	rf.persist() // 持久化存储

	_, _ = DAPrintf("%v change to candidate, current term: %d", rf.me, rf.getCurrentTerm())
}

func (rf *Raft) becomeFollower(term int) {

	rf.setState(Follower)
	rf.setCurrentTerm(term)
	rf.setVotedFor(-1)
	rf.persist()
	rf.electionTimer.Reset(getRandElectionTimeout()) //重新计算选举时间

	_, _ = DAPrintf("%v change to follower at term %d", rf.me, term)
}

func (rf *Raft) setCurrentTerm(term int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.currentTerm = term
}

func (rf *Raft) becomeLeader() {
	rf.setState(Leader)
	rf.leaderId = rf.me
	rf.indexArrayInit()
	rf.persist()

	// TODO
	_, _ = DAPrintf("%v change to leader", rf.me)
}

// 成为 Candidate 后向所有 Follower 拉票
func (rf *Raft) startElection() {
	_, _ = DAPrintf("Candidate %v: start election at term %d", rf.me, rf.getCurrentTerm())
	var mu sync.Mutex

	me := rf.me
	startTerm := rf.getCurrentTerm()
	peerNum := len(rf.peers)

	voteNum := 1 // 自己的一票
	var wg sync.WaitGroup

	peerLength := len(rf.peers)
	for i := 0; i < peerLength; i++ {
		if i != me {
			wg.Add(1)
			go func(i int, wg *sync.WaitGroup) { // 开启协程，单独执行拉票程序
				defer wg.Done()
				rf.funcMu.Lock()
				lastLogIndex := rf.getNextLogIndex() - 1
				args := RequestVoteArgs{
					Term:         rf.getCurrentTerm(),
					CandidateId:  rf.me,
					LastLogIndex: lastLogIndex,
					LastLogTerm:  rf.logs[lastLogIndex].LogTerm,
				}
				reply := RequestVoteReply{}
				rf.funcMu.Unlock()
				// _, _ = DAPrintf("%v: request vote from %d", rf.me, i)
				// _, _ = DAPrintf("args: %+v", args)
				ok := rf.sendRequestVote(i, &args, &reply)
				if !ok {
					_, _ = DAPrintf("Candidate %v: rpc to %d failed", rf.me, i)
					return
				}
				_, _ = DAPrintf("reply from %v: %+v", i, reply)
				if reply.VoteGranted == true {
					_, _ = DAPrintf("Candidate %v: follower %d voted me", rf.me, i)
					mu.Lock()
					voteNum++
					_, _ = DAPrintf("Candidate %v: voteNum: %d", rf.me, voteNum)
					mu.Unlock()
				} else if reply.Term > rf.getCurrentTerm() {
					_, _ = DAPrintf("Candidate %v: current term is not up-to-date, higher term: %d", rf.me, reply.Term)
					rf.becomeFollower(reply.Term)
				}
			}(i, &wg)
		}
	}
	// 如果已经过了轮次，或者自己已经不是 Candidate ，则直接退出程序，
	for rf.isStartTermAndState(startTerm) {
		mu.Lock()
		// _, _ = DAPrintf("test, voteNum: %v, peerNum/2: %v", voteNum, peerNum/2)
		if voteNum > peerNum/2 {
			mu.Unlock()
			rf.becomeLeader()
			break
		}
		mu.Unlock()
		time.Sleep(20 * time.Millisecond)
	}
	// 等待所有协程工作完毕，避免出现意外问题
	_, _ = DAPrintf("Raft %v: term %d election finished", rf.me, startTerm)
	wg.Wait()
}

// 判断当前 Term 是否为选举开始时的 Term，如果不是，表明进入了下一轮，
// 则本次选举作废
func (rf *Raft) isStartTermAndState(startTerm int) bool {
	curTerm := rf.getCurrentTerm()
	curState := rf.getState()

	if startTerm == curTerm && curState == Candidate {
		return true
	}
	return false
}

// 负责选举任务的 GoRoutine
func (rf *Raft) startElectionLoop() {
	rf.mu.Lock()
	rf.electionTimer = time.NewTimer(getRandElectionTimeout())
	rf.mu.Unlock()
	for {
		// 如果 Raft 已经被 killed 掉，就不需要接着选举了
		// TODO：如果 Raft 从错误中恢复了呢？
		if rf.killed() {
			return
		}
		select {
		// 选举时间到期，Raft 变为 Candidate，并重置选举时间
		case <-rf.electionTimer.C:
			// leader 本身不需要选举
			if rf.getState() == Leader {
				rf.electionTimer.Reset(getRandElectionTimeout())
				break
			}

			rf.becomeCandidate()
			rf.electionTimer.Reset(getRandElectionTimeout())
			// 向其他所有 Raft 服务器拉票，使用 GoRoutine
			go rf.startElection()

		// 收到来自其他 candidate 的投票请求
		case <-rf.chs.chanGrantVote:
			rf.electionTimer.Reset(getRandElectionTimeout())
		// 	收到 leader 的心跳
		case <-rf.chs.chanHeartBeat:
			rf.electionTimer.Reset(getRandElectionTimeout())
		}
	}
}

// 开始一轮心跳检测
func (rf *Raft) startHeartBeat() {
	_, _ = DAPrintf("Leader %v: start heartbeat at term %d", rf.me, rf.getCurrentTerm())
	me := rf.me
	curTerm := rf.getCurrentTerm()
	// TODO：append entries？
	var wg sync.WaitGroup
	for i := 0; i < len(rf.peers); i++ {
		if i != me {
			wg.Add(1)
			if rf.getMatchIndex(i) == rf.getNextLogIndex()-1 {
				go rf.justHeartBeat(curTerm, i, &wg)
			} else {
				go rf.sendLogEntries(i, &wg)
			}

		}
	}
	wg.Wait()
}

// 在循环里检测某条信息是否可以 commit
func (rf *Raft) startCommitDetectionLoop() {
	for {
		time.Sleep(10 * time.Millisecond)
		if rf.getState() != Leader { // 不是 Leader 就跳过「本次」循环（千万别用 break！）
			continue
		}
		// 下一个应该被 commit 的 log index
		nextCommitIndex := rf.getCommitIndex() + 1
		// _, _= DBPrintf("nextCommitIndex: %d", nextCommitIndex)
		commitCount := 1
		if nextCommitIndex < rf.getNextLogIndex() {
			peerNum := len(rf.peers)
			for idx := 0; idx < peerNum; idx++ {
				matchIndex := rf.getMatchIndex(idx)
				if idx != rf.me && matchIndex >= nextCommitIndex {
					_, _ = DBPrintf("Leader %v: Follower %d match current index(index: %d)", rf.me, idx, matchIndex)
					commitCount++
				}
			}
		}
		if commitCount > len(rf.peers)/2 {
			rf.funcMu.Lock()
			rf.setCommitIndex(nextCommitIndex)
			_, _ = DBPrintf("Leader %v: log(index: %d) committed, next commit index: %d", rf.me, nextCommitIndex, rf.getCommitIndex())
			applyMsg := ApplyMsg{
				CommandValid: true,
				Command:      rf.logs[nextCommitIndex].Command,
				CommandIndex: nextCommitIndex,
			}
			rf.funcMu.Unlock()
			//_, _ = DBPrintf("Leader %v: log command: %v", rf.me, applyMsg.Command)
			_, _ = DBPrintf("Leader %v: log(%d) committed", rf.me, applyMsg.CommandIndex)
			rf.applyCh <- applyMsg
		}
	}
}

// 对没有到达最新状态的节点，发送日志
func (rf *Raft) sendLogEntries(peer int, wg *sync.WaitGroup) {
	defer wg.Done()
	if rf.getState() != Leader {
		return
	}

	rf.funcMu.Lock()
	prevLogIndex := rf.getNextIndex(peer) - 1
	matchIndex := len(rf.logs) - 1
	_, _ = DBPrintf("sendLogEntries, prevLogIndex: %d, matchIndex: %d", prevLogIndex, matchIndex)
	// use deep copy to avoid race condition
	// when override log in AppendEntries()
	entries := make([]LogEntry, len(rf.logs[prevLogIndex+1:]))
	copy(entries, rf.logs[prevLogIndex+1:])
	args := AppendEntriesArgs{
		IsHeartBeat: false,
		Term:        rf.getCurrentTerm(),
		LeaderId:    rf.me,
		// 让 follower 利用 PrevLogIndex 和 PrevLogTerm 判断之前的 logs 是否已经 up-to-date
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  rf.logs[prevLogIndex].LogTerm,
		LogEntries:   rf.logs[rf.getNextIndex(peer):],
		LeaderCommit: rf.getCommitIndex(),
	}
	rf.funcMu.Unlock()

	reply := AppendEntriesReply{}
	_, _ = DBPrintf("Leader %v: send log entries to %d at term %d, args: %+v", rf.me, peer, args.Term, args)
	ok := rf.sendAppendEntries(peer, &args, &reply)
	if !ok {
		_, _ = DBPrintf("Leader %v: send log entries to %d at term %d filed, timeout, args: %+v", rf.me, peer, args.Term, args)
		return
	}
	if rf.getState() != Leader { // 不是 leader 则直接退出，不管返回值
		return
	}
	if reply.Success == true { // follower 接受了 log entries
		_, _ = DBPrintf("Leader %v: follower %d received log entries from leader %d, match index: %d", rf.me, peer, args.LeaderId, matchIndex)
		rf.setMatchIndex(peer, matchIndex)
		// 及时更新 nextIndex，以降低 RPC 的网络消耗
		rf.setNextIndex(peer, matchIndex+1)
	} else if reply.Term > rf.getCurrentTerm() { // 发现自己落后了，转成 follower
		_, _ = DBPrintf("Leader %v: current term is not up-to-date, higher term: %d", rf.me, reply.Term)
		rf.becomeFollower(reply.Term)

	} else if reply.Term == rf.getCurrentTerm() {
		// 在同一选举周期，PrevLogIndex 或 PrevLogTerm 不一致
		// 或者 leader 的 commit 信息落后于 follower
		_, _ = DBPrintf("Leader %v: PrevLogIndex(%d) and PrevLogTerm(%d) are not consistent", rf.me, args.PrevLogIndex, args.PrevLogTerm)
		// rf.nextIndex[peer] 回退 TODO：回退间隔可以设置的大一些
		rf.setNextIndex(peer, rf.getNextIndex(peer)-1)

	}
}

// 对于已经 up-to-date 的 follower 发送心跳检测即可
func (rf *Raft) justHeartBeat(curTerm int, peer int, wg *sync.WaitGroup) {
	defer wg.Done()
	if rf.getState() != Leader {
		return
	}

	rf.funcMu.Lock()
	prevLogIndex := rf.getNextIndex(peer) - 1
	// _, _ = DBPrintf("Leader %v, prevLogIndex: %d, peer: %d, time: %v", rf.me, prevLogIndex, peer, time.Now())
	args := AppendEntriesArgs{
		IsHeartBeat:  true,
		Term:         curTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  rf.logs[prevLogIndex].LogTerm,
		LeaderCommit: rf.getCommitIndex(),
	}
	rf.funcMu.Unlock()

	reply := AppendEntriesReply{}
	_, _ = DAPrintf("Leader %v: send heartbeat to %d at term %d", rf.me, peer, rf.getCurrentTerm())
	ok := rf.sendAppendEntries(peer, &args, &reply)
	if !ok {
		_, _ = DAPrintf("Leader %v: send heartbeat to %d at term %d filed, timeout", rf.me, peer, rf.getCurrentTerm())
	}
	if rf.getState() != Leader { // 不是 leader 则直接退出，不管返回值
		return
	}
	if reply.Success == true {
		// _, _ = DAPrintf("Leader %v: follower %d received heartbeat from leader %d", rf.me, peer, args.LeaderId)
	} else if reply.Term > rf.getCurrentTerm() {
		_, _ = DAPrintf("Leader %v: current term is not up-to-date, higher term: %d", rf.me, reply.Term)
		rf.becomeFollower(reply.Term)
	}
}

// 负责心跳任务的 GoRoutine
func (rf *Raft) startHeartBeatLoop() {
	rf.heartBeatTimer = time.NewTimer(getHeartBeatTimeout())
	for {
		if rf.killed() {
			return
		}
		select {
		case <-rf.heartBeatTimer.C:
			rf.heartBeatTimer.Reset(getHeartBeatTimeout())
			// 只有 leader 才需要发送心跳
			if rf.getState() != Leader {
				break
			}
			go rf.startHeartBeat()
		}
	}
}

const (
	heartBeatTimeout time.Duration = 70
)

func getHeartBeatTimeout() time.Duration {
	return heartBeatTimeout * time.Millisecond
}

const (
	ElectionTimeoutMin time.Duration = 150
	ElectionTimeoutMax time.Duration = 300
)

// 获取随机的选举 TimeOut
var randMutex = sync.Mutex{}

func getRandElectionTimeout() time.Duration {
	randMutex.Lock()
	defer randMutex.Unlock()
	timeoutInterval := int(ElectionTimeoutMax - ElectionTimeoutMin)
	t := ElectionTimeoutMin + time.Duration(rand.Intn(timeoutInterval+1))
	_, _ = DAPrintf("new election time out: %d Millisecond", t)
	return t * time.Millisecond
}
