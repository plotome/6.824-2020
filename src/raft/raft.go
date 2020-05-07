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
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	leaderId       int
	votedFor       int
	currentTerm    int
	state          RaftState
	heartBeatTimer *time.Timer
	electionTimer  *time.Timer
	chs            RaftChans

	nextIndex []int
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.state == Leader
	return term, isleader
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

	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// 添加日志，心跳检测
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	curTerm := rf.currentTerm
	rf.mu.Unlock()
	if args.IsHeartBeat {
		_, _ = DPrintf("%v: receive heart from leader %d", rf, args.LeaderId)
		if args.Term < curTerm {
			reply.Term = curTerm
			reply.Success = false
		}

		if args.Term >= curTerm {
			rf.becomeFollower(args.Term)
			curTerm = args.Term
			reply.Success = true
		}
	} else {
		// TODO: 完善添加日志的部分
	}
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

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	_, _ = DPrintf("%v received vote requested form %v", rf.me, args.CandidateId)
	// Your code here (2A, 2B).
	currentState := rf.state
	// 发现自己不是最新的 Term
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
		rf.votedFor = args.CandidateId
		reply.Term = args.Term
		reply.VoteGranted = true
		return
	}
	// 候选人 Term 较小，或者当前服务器不是 Follower (Leader/Candidate)
	if args.Term < rf.currentTerm || currentState != Follower {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// Trem 一致，服务器为 Follower，且在本轮未参与投票，则为候选人投票
	if rf.votedFor == -1 { // 本轮已参与投票
		reply.Term = args.Term
		reply.VoteGranted = false
	} else { // TODO 根据 Candidate log 长度确定是否投票
		rf.votedFor = args.CandidateId
		reply.Term = args.Term
		reply.VoteGranted = true
	}
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
	index := -1
	term := -1
	isLeader := rf.state == Leader

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
	rf.chs.chanGrantVote = make(chan int)
	rf.chs.chanHeartBeat = make(chan int)

	rf.state = Follower
	rf.currentTerm = 0

	rf.startElectionLoop()
	rf.startHeartBeatLoop()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

func (rf *Raft) becomeCandidate() {
	rf.mu.Lock()
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.persist() // 持久化存储

	rf.mu.Unlock()
	_, _ = DPrintf("%v change to candidate, current term: %d", rf.me, rf.currentTerm)
}

func (rf *Raft) becomeFollower(term int) {
	rf.mu.Lock()
	rf.state = Follower
	rf.currentTerm = term
	rf.votedFor = -1
	rf.persist()
	rf.electionTimer.Reset(getRandElectionTimeout()) //重新计算选举时间

	rf.mu.Unlock()
	_, _ = DPrintf("%v change to follower at term %d", rf.me, term)
}

func (rf *Raft) becomeLeader() {
	rf.mu.Lock()
	rf.state = Leader
	rf.leaderId = rf.me
	rf.persist()

	// TODO
	rf.mu.Unlock()
	_, _ = DPrintf("%v change to leader", rf.me)
}

// 成为 Candidate 后向所有 Follower 拉票
func (rf *Raft) startElection() {
	_, _ = DPrintf("%v start election at term %d", rf.me, rf.currentTerm)
	var mu sync.Mutex

	rf.mu.Lock()
	me := rf.me
	curTerm := rf.currentTerm
	peerNum := len(rf.peers)
	rf.mu.Unlock()

	voteNum := 1 // 自己的一票
	var wg sync.WaitGroup

	peerLength := len(rf.peers)
	for i := 0; i < peerLength; i++ {
		if i != me {
			wg.Add(1)
			go func(i int, wg *sync.WaitGroup) { // 开启协程，单独执行拉票程序
				defer wg.Done()
				args := RequestVoteArgs{
					Term:        rf.currentTerm,
					CandidateId: rf.me,
				}
				reply := RequestVoteReply{}
				_, _ = DPrintf("%v: request vote from %d", rf.me, i)
				_, _ = DPrintf("args: %+v", args)
				ok := rf.sendRequestVote(i, &args, &reply)
				_, _ = DPrintf("sendRequestVote OK? %v, vote from: %v", ok, i)
				_, _ = DPrintf("reply from %v: %+v", i, reply)
				if reply.VoteGranted == true {
					_, _ = DPrintf("%v: follower %d voted me", rf.me, i)
					mu.Lock()
					voteNum++
					mu.Unlock()
				} else if reply.Term > rf.currentTerm {
					_, _ = DPrintf("%v: current term is not up-to-date, higher term: %d", rf.me, reply.Term)
					rf.becomeFollower(reply.Term)
				}
			}(i, &wg)
		}
	}
	// 如果已经过了轮次，或者自己已经不是 Candidate ，则直接退出程序，
	for curTerm == rf.currentTerm && rf.state == Candidate {
		if voteNum > peerNum/2 {
			rf.becomeLeader()
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	// 等待所有协程工作完毕，避免出现意外问题
	wg.Wait()
	DPrintf("term %d election finished", curTerm)
}

// 负责选举任务的 GoRoutine
func (rf *Raft) startElectionLoop() {
	rf.electionTimer = time.NewTimer(getRandElectionTimeout())
	for {
		select {
		// 选举时间到期，Raft 变为 Candidate，并重置选举时间
		case <-rf.electionTimer.C:
			// leader 本身不需要选举
			if rf.state == Leader {
				rf.electionTimer.Reset(getRandElectionTimeout())
				break
			}

			rf.becomeCandidate()
			rf.electionTimer.Reset(getRandElectionTimeout())
			go rf.startElection()
			// TODO 向其他所有 Raft 服务器拉票，使用 GoRoutine

		// 收到来自其他 candidate 的投票请求
		case <-rf.chs.chanGrantVote:
			rf.electionTimer.Reset(getRandElectionTimeout())
		// 	收到 leader 的心跳
		case <-rf.chs.chanHeartBeat:
			rf.electionTimer.Reset(getRandElectionTimeout())
		}
	}
}

func (rf *Raft) startHeartBeat() {
	_, _ = DPrintf("%v start heartbeat at term %d", rf.me, rf.currentTerm)
	rf.mu.Lock()
	me := rf.me
	curTerm := rf.currentTerm
	rf.mu.Unlock()

	var wg sync.WaitGroup
	for i := 0; i < len(rf.peers); i++ {
		if i != me {
			wg.Add(1)
			go func(i int, wg *sync.WaitGroup) { // 开启协程，单独执行心跳检测
				defer wg.Done()
				args := AppendEntriesArgs{
					IsHeartBeat: true,
					Term:        curTerm,
					LeaderId:    me,
				}
				reply := AppendEntriesReply{}
				rf.sendAppendEntries(i, &args, &reply)
				if rf.state != Leader { // 不是 leader 则直接退出，不管返回值
					return
				}
				if reply.Success == true {
					_, _ = DPrintf("%v: follower %d received heartbeat", rf.me, i)
				} else if reply.Term > rf.currentTerm {
					_, _ = DPrintf("%v: current term is not up-to-date, higher term: %d", rf.me, reply.Term)
					rf.becomeFollower(reply.Term)
				}
			}(i, &wg)
		}
	}
	wg.Wait()
}

func (rf *Raft) startHeartBeatLoop() {
	rf.heartBeatTimer = time.NewTimer(getHeartBeatTimeout())
	for {
		select {
		case <-rf.heartBeatTimer.C:
			rf.heartBeatTimer.Reset(getHeartBeatTimeout())
			// 只有 leader 才需要发送心跳
			if rf.state != Leader {
				break
			}
			rf.startHeartBeat()
		}
	}
}

const (
	heartBeatTimeout time.Duration = 100
)

func getHeartBeatTimeout() time.Duration {
	return heartBeatTimeout
}

const (
	ElectionTimeoutMin time.Duration = 5000
	ElectionTimeoutMax time.Duration = 6000
)

func getRandElectionTimeout() time.Duration {
	rand.Seed(time.Now().UnixNano())
	timeoutInterval := int(ElectionTimeoutMax - ElectionTimeoutMin)
	t := ElectionTimeoutMin + time.Duration(rand.Intn(timeoutInterval+1))
	// DPrintf("new election time out: %d Millisecond", t)
	return time.Millisecond * t
}
