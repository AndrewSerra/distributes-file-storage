package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"math/rand/v2"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/AndrewSerra/fs-sync/gen/proto/file"
	logpb "github.com/AndrewSerra/fs-sync/gen/proto/log"
	raftpb "github.com/AndrewSerra/fs-sync/gen/proto/raft"
	"github.com/AndrewSerra/fs-sync/internal/chunk"
	"github.com/AndrewSerra/fs-sync/internal/consensus"
	"google.golang.org/protobuf/proto"
)

type peerSlice []string

func (p *peerSlice) String() string {
	return strings.Join(*p, ", ")
}

func (p *peerSlice) Set(value string) error {
	*p = append(*p, value)
	return nil
}

var config struct {
	nodeId      string
	peers       peerSlice
	port        int
	storagePath string
	dbHost      string
	dbPort      int
}

var storage = map[string]chunk.SavedFileMetaData{}
var metaPath string

func init() {
	flag.StringVar(&config.nodeId, "node-id", "", "Node ID (address to reach) to identify server")
	flag.Var(&config.peers, "peer", "Other servers to communicate and create consensus")
	flag.IntVar(&config.port, "port", 3456, "Port to listen for gRPC server")
	flag.StringVar(&config.storagePath, "storage-path", "/var/fs-sync/storage", "A path to store the file chunks")

	flag.Parse()

	metaPath = fmt.Sprintf("%s/.metadata", config.storagePath)

	if err := loadStorageState(); err != nil {
		fmt.Printf("could not load storage state: %s\n", err)
		os.Exit(1)
	}

	for k, v := range storage {
		fmt.Printf("%s      : %+v\n", k, v)
	}
}

type fileSyncServer struct {
	pb.UnimplementedFileSynchronizerServer
	raft *raftServer
}

func saveStateLog(state *consensus.ServerState, outfile string) error {
	var data []byte

	for _, logItem := range state.GetLog() {
		data = append(data, logItem.GetCommand()...)
	}

	return os.WriteFile(outfile, data, os.ModePerm)
}

func getInvalidParamString(message string) string {
	return fmt.Sprintf("invalid parameter: %s", message)
}

func loadStorageState() error {
	entries, err := os.ReadDir(metaPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			if err = os.MkdirAll(metaPath, os.ModePerm); err != nil {
				return err
			}
		}
		return err
	}

	for _, entry := range entries {
		entryPath := fmt.Sprintf("%s/%s", metaPath, entry.Name())
		data, err := os.ReadFile(entryPath)

		if err != nil {
			return err
		}

		var savedMeta chunk.SavedFileMetaData
		if err = json.Unmarshal(data, &savedMeta); err != nil {
			return err
		}

		storage[savedMeta.ChunkId] = savedMeta
	}

	return nil
}

func getChunkDirPath(chunkId string) string {
	return fmt.Sprintf("%s/%s", config.storagePath, chunkId)
}

func getMetaFilePath(chunkId string) string {
	return fmt.Sprintf("%s/%s.json", metaPath, chunkId)
}

func getRandomTickerTime(min int64, max int64) time.Duration {
	return time.Millisecond * time.Duration(int64(rand.IntN(int(max-min)))+min)
}

func toLogEntries(entries []*raftpb.Entry) []consensus.LogEntry {
	out := make([]consensus.LogEntry, len(entries))
	for i, e := range entries {
		out[i] = e
	}
	return out
}

func toPbEntries(entries []consensus.LogEntry) []*raftpb.Entry {
	var retEntries []*raftpb.Entry

	for _, entry := range entries {
		retEntries = append(retEntries, &raftpb.Entry{
			Term:    entry.GetTerm(),
			Command: entry.GetCommand(),
		})
	}

	return retEntries
}

func parsePeerArgs(peerList []string) (map[string]string, error) {
	peers := map[string]string{}

	for _, item := range peerList {
		peer := strings.Split(item, "/")
		if len(peer) != 2 {
			return nil, errors.New("invalid peer format, must be <nodename>/<address>")
		}
		peers[peer[0]] = peer[1]
	}

	return peers, nil
}

type raftServer struct {
	raftpb.UnimplementedConsensusServer
	state           *consensus.ServerState
	electionTicker  *time.Ticker
	heartbeatTicker *time.Ticker

	mu sync.Mutex
}

func (s *raftServer) sendAppendRequest(ctx context.Context, wg *sync.WaitGroup, peerId string) error {
	defer wg.Done()

	var opts = []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}

	peerAddr, err := s.state.GetPeerAddr(peerId)
	if err != nil {
		return err
	}

	grpcClient, err := grpc.NewClient(peerAddr, opts...)
	if err != nil {
		return err
	}

	client := raftpb.NewConsensusClient(grpcClient)

	prevLogIndex, err := s.state.GetPeerNextIndex(peerId)
	if err != nil {
		return err
	}
	prevLogIndex -= 1

	var prevLogTerm int64
	if prevLogIndex >= 0 {
		prevLogTerm, err = s.state.GetLogTermAt(prevLogIndex)
		if err != nil {
			return err
		}
	}

	entries := s.state.GetLogEntriesFrom(prevLogIndex + 1)

	rpcCtx, cancel := context.WithTimeout(ctx, time.Millisecond*time.Duration(consensus.HeartbeatTimeoutMS*2))
	defer cancel()

	res, err := client.AppendEntries(rpcCtx, &raftpb.AppendEntriesRequest{
		Term:         s.state.GetCurrentTerm(),
		LeaderId:     s.state.GetId(),
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      toPbEntries(entries),
		LeaderCommit: s.state.GetCommitIndex(),
	})
	if err != nil {
		return err
	}

	if res.GetTerm() > s.state.GetCurrentTerm() {
		s.state.BecomeFollower(res.GetTerm())
		return nil
	}

	s.state.UpdatePeerAfterAppend(peerId, prevLogIndex+1+int64(len(entries)), res.GetSuccess())
	return nil
}

func (s *raftServer) RunElectionTicker(ctx context.Context) {
	s.electionTicker = time.NewTicker(getRandomTickerTime(consensus.MinElectionTimeoutMS, consensus.MaxElectionTimeoutMS))
	defer s.electionTicker.Stop()

	for {
		select {
		case <-s.electionTicker.C:
			if s.state.GetRole() != consensus.Leader {
				s.state.StartElection()
				go s.runElection(ctx)
			}
		case <-ctx.Done():
			return
		}
	}
}

func (s *raftServer) RunApplyLoop(ctx context.Context) {
	for {
		select {
		case cmd := <-s.state.ApplyCh:
			var command logpb.Command
			if err := proto.Unmarshal(cmd, &command); err != nil {
				log.Printf("failed to unmarshal command: %s", err)
				continue
			}
			switch op := command.Operation.(type) {
			case *logpb.Command_FileAdd:
				applyFileAdd(op.FileAdd)
			case *logpb.Command_FileDelete:
				applyFileDelete(op.FileDelete)
			case *logpb.Command_PeerJoin:
				s.state.AddPeer(op.PeerJoin.GetPeerId(), op.PeerJoin.GetAddress())
			}
		case <-ctx.Done():
			return
		}
	}
}

func applyFileAdd(cmd *logpb.FileAddCommand) {
	chunkDir := getChunkDirPath(cmd.GetChunkId())
	if err := os.MkdirAll(chunkDir, os.ModePerm); err != nil {
		log.Printf("applyFileAdd: could not create dir: %s", err)
		return
	}

	for i, chunkData := range cmd.GetChunks() {
		path := fmt.Sprintf("%s/%s.%d.chunk", chunkDir, cmd.GetChunkId(), i)
		if err := os.WriteFile(path, chunkData, os.ModePerm); err != nil {
			log.Printf("applyFileAdd: could not write chunk %d: %s", i, err)
			return
		}
	}

	meta := chunk.SavedFileMetaData{
		Filename:  cmd.GetFilename(),
		ChunkId:   cmd.GetChunkId(),
		NumChunks: cmd.GetNumChunks(),
		CreatedAt: time.Now(),
	}
	data, err := json.Marshal(meta)
	if err != nil {
		log.Printf("applyFileAdd: could not marshal metadata: %s", err)
		return
	}
	if err := os.WriteFile(getMetaFilePath(cmd.GetChunkId()), data, os.ModePerm); err != nil {
		log.Printf("applyFileAdd: could not write metadata: %s", err)
		return
	}
	storage[cmd.GetChunkId()] = meta
}

func applyFileDelete(cmd *logpb.FileDeleteCommand) {
	if err := os.RemoveAll(getChunkDirPath(cmd.GetChunkId())); err != nil {
		log.Printf("applyFileDelete: could not remove chunks: %s", err)
		return
	}
	if err := os.Remove(getMetaFilePath(cmd.GetChunkId())); err != nil {
		log.Printf("applyFileDelete: could not remove metadata: %s", err)
		return
	}
	delete(storage, cmd.GetChunkId())
}

func (s *raftServer) RunHeartbeatTicker(ctx context.Context) {
	s.heartbeatTicker = time.NewTicker(time.Millisecond * time.Duration(consensus.HeartbeatTimeoutMS))
	defer s.heartbeatTicker.Stop()

	for {
		select {
		case <-s.heartbeatTicker.C:
			if s.state.GetRole() != consensus.Leader {
				return
			}
			var wg sync.WaitGroup
			for _, peerId := range s.state.GetPeerIds() {
				wg.Add(1)
				go s.sendAppendRequest(ctx, &wg, peerId)
			}
			wg.Wait()
			s.state.TryAdvanceCommitIndex()
			s.state.DrainApplied()
		case <-ctx.Done():
			return
		}
	}
}

func (s *raftServer) runElection(ctx context.Context) error {
	var mu sync.Mutex
	var wg sync.WaitGroup
	voteGranted := 1

	var opts = []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}

	sendVoteRequest := func(peerId string) {
		defer wg.Done()

		peerAddr, err := s.state.GetPeerAddr(peerId)
		if err != nil {
			return
		}

		grpcClient, err := grpc.NewClient(peerAddr, opts...)
		if err != nil {
			return
		}

		client := raftpb.NewConsensusClient(grpcClient)

		rpcCtx, cancel := context.WithTimeout(ctx, time.Millisecond*time.Duration(consensus.MinElectionTimeoutMS))
		defer cancel()

		res, err := client.RequestVote(rpcCtx, &raftpb.VoteRequest{
			Term:         s.state.GetCurrentTerm(),
			CandidateId:  s.state.GetId(),
			LastLogIndex: s.state.GetLastLogIndex(),
			LastLogTerm:  s.state.GetLastLogTerm(),
		})

		if err != nil {
			return
		}

		mu.Lock()
		defer mu.Unlock()

		if res.GetTerm() == s.state.GetCurrentTerm() && res.GetVoteGranted() {
			voteGranted += 1
		}
	}

	for _, peer := range s.state.GetPeerIds() {
		wg.Add(1)
		go sendVoteRequest(peer)
	}

	wg.Wait()

	total := len(s.state.GetPeerIds()) + 1
	majority := total / 2

	if voteGranted > majority {
		s.state.BecomeLeader()
		log.Printf("ELECTED LEADER for term %d\n", s.state.GetCurrentTerm())
		s.electionTicker.Stop()
		go s.RunHeartbeatTicker(ctx)
	}

	return nil
}

func main() {
	log.Printf("NODE ID: %s\n", config.nodeId)
	log.Printf("PORT: %d\n", config.port)
	log.Printf("STORAGE PATH: %s\n", config.storagePath)

	log.Printf("Peer list:\n")
	if len(config.peers) == 0 {
		log.Println("No peers listed")
	}

	peers, err := parsePeerArgs(config.peers)

	if err != nil {
		log.Fatalf("failed to parse peers: %v", err)
	}

	for _, peer := range peers {
		log.Printf(" - %s\n", peer)
	}

	lis, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", config.port))

	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	if err = os.MkdirAll(config.storagePath, os.ModePerm); err != nil {
		log.Fatalf("could not create storage dir: %s", err)
	}

	var opts []grpc.ServerOption
	grpcServer := grpc.NewServer(opts...)

	raftServer := &raftServer{
		state: consensus.NewServerState(config.nodeId, peers),
	}

	ctx, cancel := context.WithCancel(context.Background())

	pb.RegisterFileSynchronizerServer(grpcServer, &fileSyncServer{raft: raftServer})
	raftpb.RegisterConsensusServer(grpcServer, raftServer)
	go grpcServer.Serve(lis)
	go raftServer.RunElectionTicker(ctx)
	go raftServer.RunApplyLoop(ctx)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	cancel()

	log.Println("Received termination signal, exiting gracefully.")
	grpcServer.GracefulStop()

	currPath, err := os.Getwd()
	if err != nil {
		log.Fatalln(err)
		return
	}
	outfileDir := fmt.Sprintf("%s/logs", currPath)
	if err := os.MkdirAll(outfileDir, os.ModePerm); err != nil {
		log.Fatalln(err)
		return
	}

	outfilePath := fmt.Sprintf("%s/%s_%s.log", outfileDir, config.nodeId, time.Now().Format("20060102T150405"))
	saveStateLog(raftServer.state, outfilePath)
}
