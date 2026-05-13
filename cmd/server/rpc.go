package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	pb "github.com/AndrewSerra/fs-sync/gen/proto/file"
	logpb "github.com/AndrewSerra/fs-sync/gen/proto/log"
	raftpb "github.com/AndrewSerra/fs-sync/gen/proto/raft"
	"github.com/AndrewSerra/fs-sync/internal/chunk"
	"github.com/AndrewSerra/fs-sync/internal/consensus"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func (s *fileSyncServer) List(context context.Context, req *pb.ListRequest) (*pb.ListResponse, error) {
	files, err := os.ReadDir(metaPath)
	if err != nil {
		return nil, err
	}

	var items []*pb.SavedFileMetaData

	for _, file := range files {
		if file.IsDir() {
			return nil, errors.New("found directory in metadata directory")
		}

		data, err := os.ReadFile(fmt.Sprintf("%s/%s", metaPath, file.Name()))
		if err != nil {
			return nil, err
		}

		var metadata chunk.SavedFileMetaData
		if err := json.Unmarshal(data, &metadata); err != nil {
			return nil, err
		}
		items = append(items, &pb.SavedFileMetaData{
			Filename:  metadata.Filename,
			ChunkId:   metadata.ChunkId,
			NumChunks: metadata.NumChunks,
			CreatedAt: metadata.CreatedAt.Format(time.RFC3339Nano),
		})
	}

	return &pb.ListResponse{
		Items: items,
	}, nil
}

// pendingUploads holds chunks in memory until Upload stream completes
var pendingUploads = map[string]*logpb.FileAddCommand{}

func (s *fileSyncServer) PrepareUpload(ctx context.Context, req *pb.FileMetaData) (*pb.PrepareUploadResponse, error) {
	if s.raft.state.GetRole() != consensus.Leader {
		return nil, status.Errorf(codes.FailedPrecondition, "not the leader, try another node")
	}

	if strings.Trim(req.GetFilename(), " ") == "" {
		return &pb.PrepareUploadResponse{
			Success: false,
			Payload: &pb.PrepareUploadResponse_Message{
				Message: getInvalidParamString("empty filename"),
			},
		}, nil
	}

	if req.GetNumChunks() <= 0 {
		return &pb.PrepareUploadResponse{
			Success: false,
			Payload: &pb.PrepareUploadResponse_Message{
				Message: getInvalidParamString("non-positive number of chunks"),
			},
		}, nil
	}

	chunkId := uuid.NewString()
	pendingUploads[chunkId] = &logpb.FileAddCommand{
		Filename:  req.GetFilename(),
		ChunkId:   chunkId,
		NumChunks: req.GetNumChunks(),
		Chunks:    make([][]byte, req.GetNumChunks()),
	}

	return &pb.PrepareUploadResponse{
		Success: true,
		Payload: &pb.PrepareUploadResponse_ChunkId{
			ChunkId: chunkId,
		},
	}, nil
}

func (s *fileSyncServer) Upload(stream grpc.ClientStreamingServer[pb.FileChunk, pb.UploadResponse]) error {
	var chunkId string

	for {
		c, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		chunkId = c.GetChunkId()
		log.Printf("Received chunk number %d, size %d\n", c.ChunkOrder, len(c.Chunk))

		pending, ok := pendingUploads[chunkId]
		if !ok {
			return fmt.Errorf("no pending upload for chunkId %s", chunkId)
		}
		pending.Chunks[c.GetChunkOrder()] = c.GetChunk()
	}

	if chunkId != "" {
		pending := pendingUploads[chunkId]
		cmd := &logpb.Command{Operation: &logpb.Command_FileAdd{FileAdd: pending}}
		data, err := proto.Marshal(cmd)
		if err != nil {
			return err
		}
		s.raft.state.AppendLogEntry(s.raft.state.GetCurrentTerm(), data)
		delete(pendingUploads, chunkId)
	}

	if err := stream.SendAndClose(&pb.UploadResponse{Success: true}); err != nil {
		log.Printf("could not send upload response: %s", err)
		return err
	}

	return nil
}

func (s *fileSyncServer) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	if s.raft.state.GetRole() != consensus.Leader {
		return nil, status.Errorf(codes.FailedPrecondition, "not the leader, try another node")
	}

	if _, exists := storage[req.GetChunkId()]; !exists {
		msg := "does not exist"
		return &pb.DeleteResponse{Success: false, Message: &msg}, nil
	}

	cmd := &logpb.Command{
		Operation: &logpb.Command_FileDelete{
			FileDelete: &logpb.FileDeleteCommand{ChunkId: req.GetChunkId()},
		},
	}
	data, err := proto.Marshal(cmd)
	if err != nil {
		return nil, err
	}

	s.raft.state.AppendLogEntry(s.raft.state.GetCurrentTerm(), data)

	return &pb.DeleteResponse{Success: true}, nil
}

func (s *fileSyncServer) PrepareDownload(context context.Context, req *pb.DownloadRequest) (*pb.SavedFileMetaData, error) {
	metaFilePath := getMetaFilePath(req.GetChunkId())

	var meta chunk.SavedFileMetaData

	data, err := os.ReadFile(metaFilePath)
	if err != nil {
		return nil, err
	}

	if err := json.Unmarshal(data, &meta); err != nil {
		return nil, err
	}

	return &pb.SavedFileMetaData{
		Filename:  meta.Filename,
		ChunkId:   meta.ChunkId,
		NumChunks: meta.NumChunks,
		CreatedAt: meta.CreatedAt.Format(time.RFC3339Nano),
	}, nil
}

func (s *fileSyncServer) Download(req *pb.DownloadRequest, stream grpc.ServerStreamingServer[pb.FileChunk]) error {
	dirpath := getChunkDirPath(req.GetChunkId())
	files, err := os.ReadDir(dirpath)

	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return errors.New("does not exist")
		} else {
			return errors.New("cannot open directory")
		}
	}

	for _, file := range files {
		if file.IsDir() {
			log.Printf("found '%s' in chunk '%s' directory which is not a file", file.Name(), req.GetChunkId())
			continue
		}

		filenameSections := strings.Split(file.Name(), ".")
		orderNumber, err := strconv.Atoi(filenameSections[len(filenameSections)-2])

		if err != nil {
			return err
		}

		data, err := os.ReadFile(fmt.Sprintf("%s/%s", dirpath, file.Name()))

		if err != nil {
			return err
		}

		fmt.Printf("chunk %s : sending number %d\n", req.GetChunkId(), orderNumber)

		if err := stream.Send(&pb.FileChunk{
			ChunkOrder: int64(orderNumber),
			Chunk:      data,
		}); err != nil {
			return err
		}
	}

	return nil
}

func (s *raftServer) RequestVote(ctx context.Context, req *raftpb.VoteRequest) (*raftpb.VoteResponse, error) {
	if req.GetTerm() < s.state.GetCurrentTerm() {
		return &raftpb.VoteResponse{
			Term:        s.state.GetCurrentTerm(),
			VoteGranted: false,
		}, nil
	}

	if req.GetTerm() > s.state.GetCurrentTerm() {
		s.state.BecomeFollower(req.GetTerm())
	}

	candidateLogOk := req.GetLastLogTerm() > s.state.GetLastLogTerm() ||
		(req.GetLastLogTerm() == s.state.GetLastLogTerm() &&
			req.GetLastLogIndex() >= s.state.GetLastLogIndex())

	if (req.GetCandidateId() == s.state.GetVotedFor() || s.state.GetVotedFor() == "") && candidateLogOk {
		s.state.SetVotedFor(req.GetCandidateId())
		return &raftpb.VoteResponse{
			Term:        s.state.GetCurrentTerm(),
			VoteGranted: true,
		}, nil
	}

	return &raftpb.VoteResponse{
		Term:        s.state.GetCurrentTerm(),
		VoteGranted: false,
	}, nil
}

func (s *raftServer) AppendEntries(ctx context.Context, req *raftpb.AppendEntriesRequest) (*raftpb.AppendEntriesResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if req.GetTerm() < s.state.GetCurrentTerm() {
		return &raftpb.AppendEntriesResponse{
			Term:    s.state.GetCurrentTerm(),
			Success: false,
		}, nil
	}

	if req.GetTerm() > s.state.GetCurrentTerm() ||
		(req.GetTerm() == s.state.GetCurrentTerm() && s.state.GetRole() != consensus.Follower) {
		s.state.BecomeFollower(req.GetTerm())
	}

	s.electionTicker.Reset(getRandomTickerTime(consensus.MinElectionTimeoutMS, consensus.MaxElectionTimeoutMS))

	if s.state.LogInconsistentAt(int(req.GetPrevLogIndex()), req.GetPrevLogTerm()) {
		return &raftpb.AppendEntriesResponse{
			Term:    s.state.GetCurrentTerm(),
			Success: false,
		}, nil
	}

	s.state.ReplaceEntriesIfTermMismatch(int(req.PrevLogIndex), req.GetTerm(), toLogEntries(req.GetEntries()))

	if req.GetLeaderCommit() > s.state.GetCommitIndex() {
		s.state.SetCommitIndex(min(req.GetLeaderCommit(), int64(len(s.state.GetLog())-1)))
		s.state.DrainApplied()
	}

	return &raftpb.AppendEntriesResponse{
		Term:    s.state.GetCurrentTerm(),
		Success: true,
	}, nil
}
