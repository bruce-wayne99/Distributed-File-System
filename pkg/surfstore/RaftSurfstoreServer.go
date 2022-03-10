package surfstore

import (
	context "context"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type RaftSurfstore struct {
	// TODO add any fields you need
	isLeader bool
	term     int64
	log      []*UpdateOperation

	metaStore *MetaStore

	commitIndex         int64
	pendingCommits      []chan bool
	pendingCommitsMutex sync.RWMutex

	lastApplied int64

	// Server Info
	ip       string
	ipList   []string
	serverId int64

	// Leader protection
	isLeaderMutex sync.RWMutex
	isLeaderCond  *sync.Cond

	//Log mutex
	logMutex sync.Mutex

	rpcClients []RaftSurfstoreClient

	/*--------------- Chaos Monkey --------------*/
	isCrashed      bool
	isCrashedMutex *sync.RWMutex
	notCrashedCond *sync.Cond

	UnimplementedRaftSurfstoreServer
}

func (s *RaftSurfstore) GetFileInfoMap(ctx context.Context, empty *emptypb.Empty) (*FileInfoMap, error) {
	s.isLeaderMutex.RLock()
	if !s.isLeader {
		s.isLeaderMutex.RUnlock()
		return nil, ERR_NOT_LEADER
	}
	s.isLeaderMutex.RUnlock()

	s.isCrashedMutex.RLock()
	if s.isCrashed {
		s.isCrashedMutex.RUnlock()
		return nil, ERR_SERVER_CRASHED
	}
	s.isCrashedMutex.RUnlock()

	for {
		msg, err := s.SendHeartbeat(ctx, &emptypb.Empty{})
		if msg != nil && msg.Flag {
			break
		}
		if msg != nil && !msg.Flag && err != nil && strings.Contains(err.Error(), "Server is not the leader") {
			return nil, err
		}
	}
	return s.metaStore.GetFileInfoMap(ctx, empty)
}

func (s *RaftSurfstore) GetBlockStoreAddr(ctx context.Context, empty *emptypb.Empty) (*BlockStoreAddr, error) {
	// fmt.Println("GetBlockStoreAddr called")
	s.isLeaderMutex.RLock()
	if !s.isLeader {
		s.isLeaderMutex.RUnlock()
		return nil, ERR_NOT_LEADER
	}
	s.isLeaderMutex.RUnlock()

	s.isCrashedMutex.RLock()
	if s.isCrashed {
		s.isCrashedMutex.RUnlock()
		return nil, ERR_SERVER_CRASHED
	}
	s.isCrashedMutex.RUnlock()

	for {
		msg, err := s.SendHeartbeat(ctx, &emptypb.Empty{})
		if msg != nil && msg.Flag {
			break
		}
		if msg != nil && !msg.Flag && err != nil && strings.Contains(err.Error(), "Server is not the leader") {
			return nil, err
		}
	}
	return s.metaStore.GetBlockStoreAddr(ctx, empty)
}

func (s *RaftSurfstore) UpdateFile(ctx context.Context, filemeta *FileMetaData) (*Version, error) {
	s.isLeaderMutex.RLock()
	if !s.isLeader {
		s.isLeaderMutex.RUnlock()
		return nil, ERR_NOT_LEADER
	}
	s.isLeaderMutex.RUnlock()

	s.isCrashedMutex.RLock()
	if s.isCrashed {
		s.isCrashedMutex.RUnlock()
		return nil, ERR_SERVER_CRASHED
	}
	s.isCrashedMutex.RUnlock()

	fmt.Println("$$$$$$$$$$$$$$$$$$$$$$$")
	fmt.Println("Update file called: Printing leader details log length:", len(s.log), "serverId:", s.serverId)
	fmt.Println("$$$$$$$$$$$$$$$$$$$$$$$")

	// for {
	// 	msg, err := s.SendHeartbeat(ctx, &emptypb.Empty{})
	// 	if msg != nil && msg.Flag {
	// 		break
	// 	}
	// 	if msg != nil && !msg.Flag && err != nil && strings.Contains(err.Error(), "Server is not the leader") {
	// 		return nil, err
	// 	}
	// }

	op := &UpdateOperation{
		Term:         s.term,
		FileMetaData: filemeta,
	}

	s.logMutex.Lock()
	s.log = append(s.log, op)
	logIndex := int64(len(s.log) - 1)
	s.logMutex.Unlock()

	committed := make(chan bool)
	s.pendingCommitsMutex.Lock()
	s.pendingCommits = append(s.pendingCommits, committed)
	commitIdx := len(s.pendingCommits) - 1
	s.pendingCommitsMutex.Unlock()
	go s.attemptCommit(logIndex, commitIdx)
	success := <-committed

	if success {
		for {
			if s.lastApplied == logIndex-1 {
				break
			}
		}
		// fmt.Println("$$$$$$$$$$$$$$$$$$$$$$$")
		// fmt.Println("$$$$$$$$$$$$$$$$$$$$$$$")
		// fmt.Println("Calling metastore..", filemeta, len(s.log), s.serverId)
		// fmt.Println("$$$$$$$$$$$$$$$$$$$$$$$")
		// fmt.Println("$$$$$$$$$$$$$$$$$$$$$$$")
		ver, err := s.metaStore.UpdateFile(ctx, filemeta)
		s.lastApplied++
		return ver, err
	}

	return nil, fmt.Errorf("failed to execute operation")
}

func (s *RaftSurfstore) attemptCommit(targetIdx int64, commitIdx int) {
	commitChan := make(chan *AppendEntryOutput, len(s.ipList))
	for idx := range s.ipList {
		if int64(idx) == s.serverId {
			continue
		}
		go s.commitEntry(int64(idx), targetIdx, commitChan)
	}

	commitCount := 1
	for {
		//TODO: handle crashed nodes
		commit := <-commitChan
		if commit != nil && commit.Success {
			commitCount++
		}
		if commit != nil && !commit.Success {
			s.pendingCommits[commitIdx] <- false
			break
		}
		if commitCount > len(s.ipList)/2 {
			// for {
			// 	if s.commitIndex == targetIdx-1 {
			// 		break
			// 	}
			// }
			s.commitIndex = int64(math.Max(float64(s.commitIndex), float64(targetIdx)))
			s.pendingCommits[commitIdx] <- true
			break
		}
	}
}

func (s *RaftSurfstore) commitEntry(serverIdx, entryIdx int64, commitChan chan *AppendEntryOutput) {
	for {
		addr := s.ipList[serverIdx]
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			return
		}
		client := NewRaftSurfstoreClient(conn)

		for nextIdx := entryIdx; nextIdx >= -1; nextIdx-- {
			prevLogTerm := int64(-1)
			if nextIdx >= 0 {
				prevLogTerm = s.log[nextIdx].Term
			}
			input := &AppendEntryInput{
				Term:         s.term,
				PrevLogTerm:  prevLogTerm,
				PrevLogIndex: nextIdx,
				Entries:      s.log,
				LeaderCommit: s.commitIndex,
			}

			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			output, err := client.AppendEntries(ctx, input)
			if output != nil && output.Success {
				commitChan <- output
				return
			}
			if err != nil && strings.Contains(err.Error(), "Server is not the leader") {
				s.term = output.Term
				s.isLeaderMutex.Lock()
				s.isLeader = false
				s.isLeaderMutex.Unlock()
				commitChan <- &AppendEntryOutput{
					ServerId:     serverIdx,
					Term:         output.Term,
					Success:      false,
					MatchedIndex: -1,
				}
				return
			}
		}

		conn.Close()
		// TODO update state. s.nextIndex, etc
		// TODO handle crashed/ non success cases
	}
}

//1. Reply false if term < currentTerm (§5.1)
//2. Reply false if log doesn’t contain an entry at prevLogIndex whose term
//matches prevLogTerm (§5.3)
//3. If an existing entry conflicts with a new one (same index but different
//terms), delete the existing entry and all that follow it (§5.3)
//4. Append any new entries not already in the log
//5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index
//of last new entry)
func (s *RaftSurfstore) AppendEntries(ctx context.Context, input *AppendEntryInput) (*AppendEntryOutput, error) {
	// fmt.Println("Append entries called")
	s.isCrashedMutex.RLock()
	if s.isCrashed {
		s.isCrashedMutex.RUnlock()
		return nil, ERR_SERVER_CRASHED
	}
	s.isCrashedMutex.RUnlock()

	if input.Term < s.term {
		output := &AppendEntryOutput{
			ServerId:     s.serverId,
			Term:         s.term,
			Success:      false,
			MatchedIndex: -1,
		}
		return output, ERR_NOT_LEADER
	}

	prevIndex := input.PrevLogIndex
	prevTerm := input.PrevLogTerm

	if int64(len(s.log)) <= prevIndex {
		return &AppendEntryOutput{
			ServerId:     s.serverId,
			Term:         s.term,
			Success:      false,
			MatchedIndex: -1,
		}, nil
	}

	if prevIndex >= 0 {
		if s.log[prevIndex].Term != prevTerm {
			return &AppendEntryOutput{
				ServerId:     s.serverId,
				Term:         s.term,
				Success:      false,
				MatchedIndex: -1,
			}, nil
		}
	}

	if s.term < input.Term {
		s.isLeaderMutex.Lock()
		s.isLeader = false
		s.isLeaderMutex.Unlock()
	}
	s.term = input.Term
	s.log = append(make([]*UpdateOperation, 0), input.Entries...)

	if input.LeaderCommit > s.commitIndex {
		s.commitIndex = int64(math.Min(float64(input.LeaderCommit), float64(len(s.log)-1)))
	}

	for s.lastApplied < s.commitIndex {
		s.lastApplied++
		entry := s.log[s.lastApplied]
		s.metaStore.UpdateFile(ctx, entry.FileMetaData)
	}

	return &AppendEntryOutput{
		ServerId:     s.serverId,
		Term:         s.term,
		Success:      true,
		MatchedIndex: -1,
	}, nil
}

// This should set the leader status and any related variables as if the node has just won an election
func (s *RaftSurfstore) SetLeader(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	// fmt.Println("Set leader called")
	s.isCrashedMutex.RLock()
	if s.isCrashed {
		successNote := &Success{
			Flag: false,
		}
		s.isCrashedMutex.RUnlock()
		return successNote, ERR_SERVER_CRASHED
	}
	s.isCrashedMutex.RUnlock()
	s.isLeaderMutex.Lock()
	s.isLeader = true
	s.term++
	s.isLeaderMutex.Unlock()
	//TODO: do leader operations
	successNote := &Success{
		Flag: true,
	}
	return successNote, nil
}

// Send a 'Heartbeat" (AppendEntries with no log entries) to the other servers
// Only leaders send heartbeats, if the node is not the leader you can return Success = false
func (s *RaftSurfstore) SendHeartbeat(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	// fmt.Println("Sendheartbeat called")
	hbCount := 0
	for idx, addr := range s.ipList {
		if int64(idx) == s.serverId {
			hbCount++
			continue
		}

		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			continue
		}
		client := NewRaftSurfstoreClient(conn)

		input := &AppendEntryInput{
			Term:         s.term,
			PrevLogTerm:  -1,
			PrevLogIndex: -1,
			Entries:      s.log,
			LeaderCommit: s.commitIndex,
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		output, err := client.AppendEntries(ctx, input)
		if (err == nil) || (err != nil && !strings.Contains(err.Error(), "Server is crashed.")) {
			hbCount++
		}
		if err != nil && strings.Contains(err.Error(), "Server is not the leader") {
			if output != nil {
				s.term = output.Term
			}
			s.isLeaderMutex.Lock()
			s.isLeader = false
			s.isLeaderMutex.Unlock()
			return &Success{
				Flag: false,
			}, ERR_NOT_LEADER
		}
		conn.Close()
		if hbCount > len(s.ipList)/2 {
			break
		}
	}

	if hbCount > len(s.ipList)/2 {
		return &Success{
			Flag: true,
		}, nil
	}

	return &Success{
		Flag: false,
	}, nil
}

func (s *RaftSurfstore) Crash(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = true
	s.isCrashedMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) Restore(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = false
	s.notCrashedCond.Broadcast()
	s.isCrashedMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) IsCrashed(ctx context.Context, _ *emptypb.Empty) (*CrashedState, error) {
	return &CrashedState{IsCrashed: s.isCrashed}, nil
}

func (s *RaftSurfstore) GetInternalState(ctx context.Context, empty *emptypb.Empty) (*RaftInternalState, error) {
	fileInfoMap, _ := s.metaStore.GetFileInfoMap(ctx, empty)
	output := &RaftInternalState{
		IsLeader: s.isLeader,
		Term:     s.term,
		Log:      s.log,
		MetaMap:  fileInfoMap,
	}
	fmt.Println("Internal state called, Internal state: ", output, "serverId:", s.serverId)
	return output, nil
}

var _ RaftSurfstoreInterface = new(RaftSurfstore)
