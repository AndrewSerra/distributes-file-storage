package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/AndrewSerra/fs-sync/gen/proto"
	"github.com/AndrewSerra/fs-sync/internal/chunk"
)

func init() {
	rootCmd.AddGroup(cmdGroup)

	// var serverHost string
	// var serverPort int

	// flag.StringVar(&serverHost, "server-host", "localhost", "Host of the server")
	// flag.IntVar(&serverPort, "server-port", 3456, "Port of server listening at")

	// Command
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
	Use:     "delete <filename>",
	Short:   "Delete file being stored",
	GroupID: cmdGroup.ID,
	Args:    cobra.ExactArgs(1),
	Run:     deleteCommandHandler,
}

var downloadCmd = &cobra.Command{
	Use:     "download <filename>",
	Short:   "Download file being stored",
	GroupID: cmdGroup.ID,
	Args:    cobra.ExactArgs(1),
	Run:     downloadCommandHandler,
}

var listCmd = &cobra.Command{
	Use:     "list",
	Short:   "list files being stored",
	GroupID: cmdGroup.ID,
	Args:    cobra.NoArgs,
	Run:     listCommandHandler,
}

var client pb.FileSynchronizerClient

func uploadCommandHandler(cmd *cobra.Command, args []string) {
	fp := args[0]
	fn := filepath.Base(fp)

	chunks, err := chunk.CreateFileChunks(fp)
	if err != nil {
		fmt.Printf("could not create chunks: %s\n", fp)
		return
	}

	chunkId, err := prepareUpload(client, chunk.PrepareFileMetaData{
		Filename:  fn,
		NumChunks: int64(len(chunks)),
	})

	if err != nil {
		fmt.Printf("could not upload: %s\n", err)
		return
	}

	if err := uploadFile(client, chunkId, chunks); err != nil {
		fmt.Printf("error uploading file: %s", err)
		return
	}

	fmt.Printf("upload complete. chunk ID: %s\n", chunkId)
}

func deleteCommandHandler(cmd *cobra.Command, args []string) {
	if err := deleteFile(client, args[0]); err != nil {
		fmt.Printf("error deleting file: %s", err)
		return
	}

	fmt.Println("delete complete.")
}

func downloadCommandHandler(cmd *cobra.Command, args []string) {

	meta, err := prepareDownload(client, args[0])

	if err != nil {
		fmt.Printf("error receiving file metadata: %s", err)
		return
	}

	if err := downloadFile(client, meta); err != nil {
		fmt.Printf("error downloading file: %s", err)
		return
	}
	fmt.Println("download complete.")
}

func listCommandHandler(cmd *cobra.Command, args []string) {
	data, err := list(client)

	if err != nil {
		fmt.Printf("could not get item list: %s", err)
		return
	}

	for _, item := range data {
		fmt.Printf("File: %-20s ChunkID: %-20s\n", item.Filename, item.ChunkId)
	}
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
	} else {
		return "", errors.New(res.GetMessage())
	}
}

func uploadFile(client pb.FileSynchronizerClient, chunkId string, chunks []chunk.FileChunkData) error {
	ctx := context.TODO()
	stream, err := client.Upload(ctx)
	if err != nil {
		return err
	}

	for _, chunk := range chunks {
		if err := stream.Send(&pb.FileChunk{
			ChunkId:    chunkId,
			ChunkOrder: int64(chunk.Order),
			Chunk:      chunk.Chunk,
		}); err != nil {
			return fmt.Errorf("failed to send chunk number %d", chunk.Order)
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
	resp, err := client.Delete(context.TODO(), &pb.DeleteRequest{
		ChunkId: chunkId,
	})
	if err != nil {
		return err
	}

	if !resp.Success {
		fmt.Printf("Could not delete chunk '%s': %s\n", chunkId, *resp.Message)
		return nil
	}

	return nil
}

func prepareDownload(client pb.FileSynchronizerClient, chunkId string) (chunk.SavedFileMetaData, error) {
	savedMeta, err := client.PrepareDownload(context.Background(), &pb.DownloadRequest{
		ChunkId: chunkId,
	})

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
	stream, err := client.Download(context.Background(), &pb.DownloadRequest{
		ChunkId: metadata.ChunkId,
	})
	if err != nil {
		return err
	}

	var data = make([]byte, metadata.NumChunks*chunk.ChunkSizeBytes)
	var totalBytes = 0

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
	var opts = []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}

	const serverHost = "localhost"
	const serverPort = 3456

	grpcClient, err := grpc.NewClient(fmt.Sprintf("%s:%d", serverHost, serverPort), opts...)
	if err != nil {
		log.Fatalf("cannot connect to server: %s", err)
	}
	defer grpcClient.Close()

	client = pb.NewFileSynchronizerClient(grpcClient)

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
