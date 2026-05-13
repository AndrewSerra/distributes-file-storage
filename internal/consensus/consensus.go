package consensus

import (
	"errors"
	"sort"
	"sync"
)

type role int64
type command string

const MinElectionTimeoutMS int64 = 150
const MaxElectionTimeoutMS int64 = 300
const HeartbeatTimeoutMS int64 = 50

const (
	Follower role = iota
	Candidate
	Leader
)

const (
	Create command = "create"
	Delete command = "delete"
)

type LogEntry interface {
	GetTerm() int64
	GetCommand() []byte
}

type logItem struct {
	term    int64
	command []byte
}

func (l logItem) GetTerm() int64 {
	return l.term
}

func (l logItem) GetCommand() []byte {
	return l.command
}

type ServerState struct {
	id   string
	role role

	// other servers
	// key is the nodename and value is the address
	peers map[string]string

	// persistent
	currentTerm int64
	votedFor    string
	log         []logItem

	// volatile
	commitIndex int64
	lastApplied int64

	// volatile - for leaders
	nextIndex  map[string]int64
	matchIndex map[string]int64

	// int64ernal
	mu      sync.RWMutex
	ApplyCh chan []byte
}

func (s *ServerState) GetId() string {
	return s.id
}

func (s *ServerState) GetRole() role {
	return s.role
}

func (s *ServerState) BecomeLeader() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.role = Leader
}

func (s *ServerState) BecomeFollower(term int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.role = Follower
	s.currentTerm = term
	s.votedFor = ""
}

func (s *ServerState) BecomeCandidate() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.role = Candidate
}

func (s *ServerState) AddPeer(peerId string, address string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.peers[peerId] = address
	s.nextIndex[peerId] = int64(len(s.log))
	s.matchIndex[peerId] = 0
}

func (s *ServerState) GetPeerIds() []string {
	ids := make([]string, 0, len(s.peers))
	for id := range s.peers {
		ids = append(ids, id)
	}
	return ids
}

func (s *ServerState) GetPeerAddr(peerId string) (string, error) {
	peerAddr, ok := s.peers[peerId]
	if !ok {
		return "", errors.New("peer does not exist")
	}
	return peerAddr, nil
}

func (s *ServerState) GetPeerNextIndex(peerId string) (int64, error) {
	peerNextIndex, ok := s.nextIndex[peerId]
	if !ok {
		return -1, errors.New("peer does not exist")
	}
	return peerNextIndex, nil
}

func (s *ServerState) GetPeerMatchIndex(peerId string) (int64, error) {
	peerMatchIndex, ok := s.matchIndex[peerId]
	if !ok {
		return -1, errors.New("peer does not exist")
	}
	return peerMatchIndex, nil
}

func (s *ServerState) GetCurrentTerm() int64 {
	return s.currentTerm
}

func (s *ServerState) SetCurrentTerm(term int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.currentTerm = term
}

func (s *ServerState) GetVotedFor() string {
	return s.votedFor
}

func (s *ServerState) SetVotedFor(candidateId string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.votedFor = candidateId
}

func (s *ServerState) GetLog() []logItem {
	return s.log
}

func (s *ServerState) GetLogTermAt(idx int64) (int64, error) {
	if idx < 0 || int(idx) >= len(s.log) {
		return 0, errors.New("index out of range")
	}
	return s.log[idx].term, nil
}

func (s *ServerState) GetLogEntriesFrom(idx int64) []LogEntry {
	if idx >= int64(len(s.log)) {
		return []LogEntry{}
	}
	slice := s.log[idx:]
	out := make([]LogEntry, len(slice))
	for i, item := range slice {
		out[i] = item
	}
	return out
}

func (s *ServerState) GetLastLogIndex() int64 {
	return int64(len(s.log) - 1)
}

func (s *ServerState) GetLastLogTerm() int64 {
	if len(s.log) == 0 {
		return 0
	}
	return s.log[len(s.log)-1].term
}

func (s *ServerState) GetCommitIndex() int64 {
	return s.commitIndex
}

func (s *ServerState) UpdatePeerAfterAppend(peerId string, nextIdx int64, success bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if success {
		s.nextIndex[peerId] = nextIdx
		s.matchIndex[peerId] = nextIdx - 1
	} else {
		if s.nextIndex[peerId] > 1 {
			s.nextIndex[peerId]--
		}
	}
}

func (s *ServerState) TryAdvanceCommitIndex() {
	s.mu.Lock()
	defer s.mu.Unlock()

	// collect matchIndex values from all peers plus self
	indices := make([]int, 0, len(s.matchIndex)+1)
	indices = append(indices, len(s.log)-1)
	for _, idx := range s.matchIndex {
		indices = append(indices, int(idx))
	}

	// sort descending — median is the highest index on a majority
	sort.Sort(sort.Reverse(sort.IntSlice(indices)))
	majority := indices[len(indices)/2]

	// only commit if the entry belongs to the current term
	if int64(majority) > s.commitIndex && majority < len(s.log) && s.log[majority].term == s.currentTerm {
		s.commitIndex = int64(majority)
	}
}

func (s *ServerState) SetCommitIndex(newIndex int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.commitIndex = newIndex
}

func (s *ServerState) LogInconsistentAt(idx int, term int64) bool {
	if idx == -1 {
		return false
	}
	if idx < 0 || idx >= len(s.log) {
		return true
	}
	return s.log[idx].term != term
}

func (s *ServerState) ReplaceEntriesIfTermMismatch(prevIdx int, term int64, entries []LogEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	items := make([]logItem, len(entries))
	for i, e := range entries {
		items[i] = logItem{term: e.GetTerm(), command: e.GetCommand()}
	}

	insertAt := prevIdx + 1
	for i := insertAt; i < len(s.log); i++ {
		itemIdx := i - insertAt
		if itemIdx >= len(items) {
			s.log = s.log[:i]
			break
		}
		if s.log[i].GetTerm() != items[itemIdx].GetTerm() {
			s.log = append(s.log[:i], items[itemIdx:]...)
			return nil
		}
	}

	existing := len(s.log) - insertAt
	if existing < 0 {
		existing = 0
	}
	s.log = append(s.log, items[existing:]...)
	return nil
}

func (s *ServerState) DrainApplied() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for s.lastApplied < s.commitIndex {
		s.lastApplied++
		s.ApplyCh <- s.log[s.lastApplied].command
	}
}

func (s *ServerState) AppendLogEntry(term int64, command []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.log = append(s.log, logItem{term: term, command: command})
}

func (s *ServerState) StartElection() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.role = Candidate
	s.currentTerm += 1
	s.votedFor = s.id
}

func NewServerState(id string, peers map[string]string) *ServerState {

	var nextIndexInit map[string]int64 = map[string]int64{}
	var matchIndexInit map[string]int64 = map[string]int64{}

	for pid := range peers {
		nextIndexInit[pid] = 0
		matchIndexInit[pid] = 0
	}

	return &ServerState{
		id:          id,
		role:        Follower,
		peers:       peers,
		currentTerm: 0,
		votedFor:    "",
		log:         []logItem{},
		commitIndex: -1,
		lastApplied: -1,
		nextIndex:   nextIndexInit,
		matchIndex:  matchIndexInit,
		mu:          sync.RWMutex{},
		ApplyCh:     make(chan []byte, 64),
	}
}
