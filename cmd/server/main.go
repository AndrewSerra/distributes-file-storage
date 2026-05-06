package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"

	pb "github.com/AndrewSerra/fs-sync/gen/proto/file"
	"github.com/AndrewSerra/fs-sync/internal/chunk"
	"github.com/google/uuid"
)

var config struct {
	port        int
	storagePath string
	dbHost      string
	dbPort      int
}

var storage = map[string]chunk.SavedFileMetaData{}
var metaPath string

func init() {
	flag.IntVar(&config.port, "port", 3456, "Port to listen for gRPC server")
	flag.StringVar(&config.storagePath, "storage-path", "/var/fs-sync/storage", "A path to store the file chunks")
	flag.StringVar(&config.dbHost, "db-host", "localhost", "Database host address")
	flag.IntVar(&config.dbPort, "db-port", 5432, "Database port")

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

func (s *fileSyncServer) PrepareUpload(context context.Context, req *pb.FileMetaData) (*pb.PrepareUploadResponse, error) {
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

	_, err := os.ReadDir(metaPath)

	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			if err := os.MkdirAll(metaPath, os.ModePerm); err != nil {
				return nil, err
			}
		} else {
			return &pb.PrepareUploadResponse{
				Success: false,
				Payload: &pb.PrepareUploadResponse_Message{
					Message: fmt.Sprintf("could not open file: %s", err),
				},
			}, nil
		}
	}

	chunkId := uuid.NewString()
	metaFilePath := fmt.Sprintf("%s/%s.json", metaPath, chunkId)
	incomingdata := chunk.SavedFileMetaData{
		Filename:  req.Filename,
		ChunkId:   chunkId,
		NumChunks: req.GetNumChunks(),
		CreatedAt: time.Now(),
	}

	data, err := json.Marshal(incomingdata)
	if err != nil {
		return nil, err
	}

	chunkStoragePath := getChunkDirPath(chunkId)

	if err := os.MkdirAll(chunkStoragePath, os.ModePerm); err != nil {
		return nil, err
	}

	if err := os.WriteFile(metaFilePath, data, os.ModePerm); err != nil {
		return nil, err
	}

	storage[chunkId] = incomingdata

	return &pb.PrepareUploadResponse{
		Success: true,
		Payload: &pb.PrepareUploadResponse_ChunkId{
			ChunkId: chunkId,
		},
	}, nil
}

func (s *fileSyncServer) Upload(stream grpc.ClientStreamingServer[pb.FileChunk, pb.UploadResponse]) error {

	for {
		chunk, err := stream.Recv()

		chunkStoragePath := getChunkDirPath(chunk.GetChunkId())

		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		log.Printf("Received chunk number %d, size %d\n", chunk.ChunkOrder, len(chunk.Chunk))

		fullFilePath := fmt.Sprintf("%s/%s.%d.chunk", chunkStoragePath, chunk.GetChunkId(), chunk.GetChunkOrder())
		if err := os.WriteFile(fullFilePath, chunk.GetChunk(), os.ModePerm); err != nil {
			log.Panicf("Could not write chunk file '%s': %s", fullFilePath, err)
		}
	}

	if err := stream.SendAndClose(&pb.UploadResponse{
		Success: true,
	}); err != nil {
		log.Printf("could not send upload response: %s", err)
		return err
	}

	return nil
}

func (s *fileSyncServer) Delete(context context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	dirpath := getChunkDirPath(req.GetChunkId())
	metaFilePath := getMetaFilePath(req.GetChunkId())
	_, err := os.ReadDir(dirpath)

	if err != nil {
		var msg string
		if errors.Is(err, os.ErrNotExist) {
			msg = "does not exist"
		} else {
			msg = "cannot open directory"
		}
		return &pb.DeleteResponse{
			Success: false,
			Message: &msg,
		}, nil
	}

	if err := os.RemoveAll(dirpath); err != nil {
		var msg = "could not delete stored path"
		return &pb.DeleteResponse{
			Success: false,
			Message: &msg,
		}, nil
	}

	if err := os.Remove(metaFilePath); err != nil {
		var msg = "could not delete metadata file"
		return &pb.DeleteResponse{
			Success: false,
			Message: &msg,
		}, nil
	}

	return &pb.DeleteResponse{
		Success: true,
	}, nil
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

func newServer() *fileSyncServer {
	syncServer := &fileSyncServer{}
	return syncServer
}

func main() {
	log.Printf("PORT: %d\n", config.port)
	log.Printf("STORAGE PATH: %s\n", config.storagePath)

	lis, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", config.port))

	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	if err = os.MkdirAll(config.storagePath, os.ModePerm); err != nil {
		log.Fatalf("could not create storage dir: %s", err)
	}

	var opts []grpc.ServerOption
	grpcServer := grpc.NewServer(opts...)

	pb.RegisterFileSynchronizerServer(grpcServer, newServer())
	go grpcServer.Serve(lis)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Received termination signal, exiting gracefully.")
	grpcServer.GracefulStop()
}
