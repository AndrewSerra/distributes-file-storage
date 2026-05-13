package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	pb "github.com/AndrewSerra/fs-sync/gen/proto/file"
	"github.com/AndrewSerra/fs-sync/internal/chunk"
)

var nodes []string

var grpcOpts = []grpc.DialOption{
	grpc.WithTransportCredentials(insecure.NewCredentials()),
}

func init() {
	rootCmd.AddGroup(cmdGroup)
	rootCmd.PersistentFlags().StringArrayVar(&nodes, "node", nil, "Node address (repeatable); first responding leader is used")

	rootCmd.AddCommand(uploadCmd)
	rootCmd.AddCommand(deleteCmd)
	rootCmd.AddCommand(downloadCmd)
	rootCmd.AddCommand(listCmd)
}

var rootCmd = &cobra.Command{
	Use:     "fs-sync <command> <args>",
	Short:   "Command-line tool to store file distributedly.",
	Version: "0.1.0",
}

var cmdGroup = &cobra.Group{
	ID:    "command",
	Title: "Commands: ",
}

var uploadCmd = &cobra.Command{
	Use:     "upload <filename>",
	Short:   "Upload file to storage",
	GroupID: cmdGroup.ID,
	Args:    cobra.ExactArgs(1),
	Run:     uploadCommandHandler,
}

var deleteCmd = &cobra.Command{
	Use:     "delete <chunkId>",
	Short:   "Delete file being stored",
	GroupID: cmdGroup.ID,
	Args:    cobra.ExactArgs(1),
	Run:     deleteCommandHandler,
}

var downloadCmd = &cobra.Command{
	Use:     "download <chunkId>",
	Short:   "Download file being stored",
	GroupID: cmdGroup.ID,
	Args:    cobra.ExactArgs(1),
	Run:     downloadCommandHandler,
}

var listCmd = &cobra.Command{
	Use:     "list",
	Short:   "List files being stored",
	GroupID: cmdGroup.ID,
	Args:    cobra.NoArgs,
	Run:     listCommandHandler,
}

func getNodes() []string {
	if len(nodes) == 0 {
		return []string{"localhost:3456"}
	}
	return nodes
}

func dial(addr string) (pb.FileSynchronizerClient, *grpc.ClientConn, error) {
	conn, err := grpc.NewClient(addr, grpcOpts...)
	if err != nil {
		return nil, nil, err
	}
	return pb.NewFileSynchronizerClient(conn), conn, nil
}

func isNotLeader(err error) bool {
	return status.Code(err) == codes.FailedPrecondition
}

// withLeader tries each node in order, retrying on non-leader errors.
// fn receives a client connected to the leader and must return a non-nil
// error only for real failures (not FailedPrecondition).
func withLeader(fn func(pb.FileSynchronizerClient) error) error {
	for _, addr := range getNodes() {
		c, conn, err := dial(addr)
		if err != nil {
			continue
		}
		err = fn(c)
		conn.Close()
		if err == nil {
			return nil
		}
		if isNotLeader(err) {
			continue
		}
		return err
	}
	return fmt.Errorf("no leader found among nodes: %v", getNodes())
}

func uploadCommandHandler(cmd *cobra.Command, args []string) {
	fp := args[0]
	fn := filepath.Base(fp)

	chunks, err := chunk.CreateFileChunks(fp)
	if err != nil {
		fmt.Printf("could not create chunks: %s\n", fp)
		return
	}

	meta := chunk.PrepareFileMetaData{
		Filename:  fn,
		NumChunks: int64(len(chunks)),
	}

	var chunkId string

	for _, addr := range getNodes() {
		c, conn, err := dial(addr)
		if err != nil {
			continue
		}

		chunkId, err = prepareUpload(c, meta)
		if err != nil {
			conn.Close()
			if isNotLeader(err) {
				continue
			}
			fmt.Printf("could not upload: %s\n", err)
			return
		}

		err = uploadFile(c, chunkId, chunks)
		conn.Close()
		if err != nil {
			fmt.Printf("error uploading file: %s\n", err)
			return
		}

		fmt.Printf("upload complete. chunk ID: %s\n", chunkId)
		return
	}

	fmt.Printf("no leader found among nodes: %v\n", getNodes())
}

func deleteCommandHandler(cmd *cobra.Command, args []string) {
	err := withLeader(func(c pb.FileSynchronizerClient) error {
		return deleteFile(c, args[0])
	})
	if err != nil {
		fmt.Printf("error deleting file: %s\n", err)
		return
	}
	fmt.Println("delete complete.")
}

func downloadCommandHandler(cmd *cobra.Command, args []string) {
	var meta chunk.SavedFileMetaData
	var found bool

	for _, addr := range getNodes() {
		c, conn, err := dial(addr)
		if err != nil {
			continue
		}
		meta, err = prepareDownload(c, args[0])
		conn.Close()
		if err != nil {
			continue
		}
		found = true
		break
	}

	if !found {
		fmt.Println("could not find file on any node")
		return
	}

	for _, addr := range getNodes() {
		c, conn, err := dial(addr)
		if err != nil {
			continue
		}
		err = downloadFile(c, meta)
		conn.Close()
		if err != nil {
			continue
		}
		fmt.Println("download complete.")
		return
	}

	fmt.Println("could not download file from any node")
}

func listCommandHandler(cmd *cobra.Command, args []string) {
	for _, addr := range getNodes() {
		c, conn, err := dial(addr)
		if err != nil {
			continue
		}
		data, err := list(c)
		conn.Close()
		if err != nil {
			continue
		}
		for _, item := range data {
			fmt.Printf("File: %-20s ChunkID: %-20s\n", item.Filename, item.ChunkId)
		}
		return
	}
	fmt.Printf("could not reach any node: %v\n", getNodes())
}

func list(client pb.FileSynchronizerClient) ([]chunk.SavedFileMetaData, error) {
	items, err := client.List(context.Background(), &pb.ListRequest{})
	if err != nil {
		return nil, err
	}

	var saved []chunk.SavedFileMetaData
	for _, item := range items.GetItems() {
		createdAt, err := time.Parse(time.RFC3339Nano, item.GetCreatedAt())
		if err != nil {
			return nil, err
		}
		saved = append(saved, chunk.SavedFileMetaData{
			Filename:  item.GetFilename(),
			ChunkId:   item.GetChunkId(),
			NumChunks: item.GetNumChunks(),
			CreatedAt: createdAt,
		})
	}
	return saved, nil
}

func prepareUpload(client pb.FileSynchronizerClient, metadata chunk.PrepareFileMetaData) (string, error) {
	res, err := client.PrepareUpload(context.Background(), &pb.FileMetaData{
		Filename:  metadata.Filename,
		NumChunks: metadata.NumChunks,
	})
	if err != nil {
		return "", err
	}
	if res.Success {
		return res.GetChunkId(), nil
	}
	return "", errors.New(res.GetMessage())
}

func uploadFile(client pb.FileSynchronizerClient, chunkId string, chunks []chunk.FileChunkData) error {
	stream, err := client.Upload(context.TODO())
	if err != nil {
		return err
	}

	for _, c := range chunks {
		if err := stream.Send(&pb.FileChunk{
			ChunkId:    chunkId,
			ChunkOrder: int64(c.Order),
			Chunk:      c.Chunk,
		}); err != nil {
			return fmt.Errorf("failed to send chunk number %d", c.Order)
		}
	}

	resp, err := stream.CloseAndRecv()
	if err != nil {
		return err
	}
	if !resp.Success {
		return fmt.Errorf("server rejected upload")
	}
	return nil
}

func deleteFile(client pb.FileSynchronizerClient, chunkId string) error {
	resp, err := client.Delete(context.TODO(), &pb.DeleteRequest{ChunkId: chunkId})
	if err != nil {
		return err
	}
	if !resp.Success {
		fmt.Printf("Could not delete chunk '%s': %s\n", chunkId, *resp.Message)
	}
	return nil
}

func prepareDownload(client pb.FileSynchronizerClient, chunkId string) (chunk.SavedFileMetaData, error) {
	savedMeta, err := client.PrepareDownload(context.Background(), &pb.DownloadRequest{ChunkId: chunkId})
	if err != nil {
		return chunk.SavedFileMetaData{}, err
	}

	createdAt, err := time.Parse(time.RFC3339Nano, savedMeta.GetCreatedAt())
	if err != nil {
		return chunk.SavedFileMetaData{}, err
	}

	return chunk.SavedFileMetaData{
		Filename:  savedMeta.GetFilename(),
		NumChunks: savedMeta.GetNumChunks(),
		ChunkId:   savedMeta.GetChunkId(),
		CreatedAt: createdAt,
	}, nil
}

func downloadFile(client pb.FileSynchronizerClient, metadata chunk.SavedFileMetaData) error {
	stream, err := client.Download(context.Background(), &pb.DownloadRequest{ChunkId: metadata.ChunkId})
	if err != nil {
		return err
	}

	data := make([]byte, metadata.NumChunks*chunk.ChunkSizeBytes)
	totalBytes := 0

	for {
		recvChunk, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		offset := recvChunk.ChunkOrder * chunk.ChunkSizeBytes
		endIndex := offset + int64(len(recvChunk.GetChunk()))
		copy(data[offset:endIndex], recvChunk.GetChunk())
		totalBytes += len(recvChunk.Chunk)
	}

	return os.WriteFile(metadata.Filename, data[0:totalBytes], os.ModePerm)
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
