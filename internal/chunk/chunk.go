package chunk

import (
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"
)

const chunkSizeKB = 64
const ChunkSizeBytes = chunkSizeKB * 1024

type SavedFileMetaData struct {
	Filename  string    `json:"filename"`
	ChunkId   string    `json:"chunk_id"`
	NumChunks int64     `json:"chunk_count"`
	CreatedAt time.Time `json:"created_at"`
}

type PrepareFileMetaData struct {
	Filename  string `json:"filename"`
	NumChunks int64  `json:"chunk_count"`
}

type FileChunkData struct {
	Filename string
	Order    int
	Chunk    []byte
}

type CombinedChunk struct {
	ChunkId  string
	Filename string
	data     []byte
}

func writeChunkToFile(outputDir string, filename string, chunk []byte) error {
	writeFilePath := fmt.Sprintf("%s/%s", outputDir, filename)
	file, err := os.Create(writeFilePath)
	if err != nil {
		return err
	}
	defer file.Close()

	n, err := file.Write(chunk)
	log.Printf("written %d bytes out of %d bytes, to file %s", n, len(chunk), writeFilePath)
	return err
}

func verifyFileInput(fp string) (os.FileInfo, error) {
	fileinfo, err := os.Stat(fp)

	if err != nil {
		return nil, err
	}

	if fileinfo.IsDir() {
		return nil, errors.New("file received is a directory!")
	}

	return fileinfo, nil
}

func verifyDirInput(path string) (os.FileInfo, error) {
	outputDirInfo, err := os.Stat(path)

	if err != nil {
		return nil, err
	}

	if !outputDirInfo.IsDir() {
		return nil, errors.New("output directory received is not a directory!")
	}

	return outputDirInfo, nil
}

func CreateFileChunks(fp string) ([]FileChunkData, error) {
	fileinfo, err := verifyFileInput(fp)

	if err != nil {
		return nil, err
	}

	filesizeBytes := fileinfo.Size()
	numChunks := ((int64(filesizeBytes) + ChunkSizeBytes - 1) / ChunkSizeBytes)

	file, err := os.Open(fp)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	fpathSplit := strings.Split(file.Name(), "/")
	// fname, _, _ := strings.Cut(fpathSplit[len(fpathSplit)-1], ".")
	fname := fpathSplit[len(fpathSplit)-1]

	chunkNum := 0
	var chunks []FileChunkData

	for chunkNum <= int(numChunks) {
		buf := make([]byte, ChunkSizeBytes)
		n, err := file.Read(buf)
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Fatalf("%s\n", err)
		}

		chunks = append(chunks, FileChunkData{
			Filename: fmt.Sprintf("%s.%d.chunk", fname, chunkNum),
			Order:    chunkNum,
			Chunk:    buf[:n],
		})

		chunkNum += 1
	}

	return chunks, nil
}

func ChunkSaveFile(fp string, outputDir string) error {

	fileinfo, err := verifyFileInput(fp)

	if err != nil {
		return err
	}

	_, err = verifyDirInput(outputDir)

	if err != nil {
		return err
	}

	filesizeBytes := fileinfo.Size()
	numChunks := ((int64(filesizeBytes) + ChunkSizeBytes - 1) / ChunkSizeBytes)

	file, err := os.Open(fp)
	if err != nil {
		return err
	}
	defer file.Close()

	chunkNum := 0
	buf := make([]byte, ChunkSizeBytes)

	fpathSplit := strings.Split(file.Name(), "/")
	fname, _, _ := strings.Cut(fpathSplit[len(fpathSplit)-1], ".")

	for chunkNum <= int(numChunks) {
		n, err := file.Read(buf)
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Fatalf("%s\n", err)
		}

		chunk := buf[:n]
		chunkFname := fmt.Sprintf("%s.%d.chunk", fname, chunkNum)

		err = writeChunkToFile(outputDir, chunkFname, chunk)
		if err != nil {
			log.Printf("could not write chunk number %d: %s", chunkNum, err)
		}
		chunkNum += 1
	}

	return nil
}
