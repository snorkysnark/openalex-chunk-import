package main

import (
	"flag"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"

	"github.com/cheggaaa/pb/v3"

	"github.com/snorkysnark/openalex-chunk-import/flatten"
)

func findJsonFiles(root string) ([]string, error) {
	var jsonPaths []string

	if err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if !d.IsDir() && filepath.Ext(path) == ".gz" {
			jsonPaths = append(jsonPaths, path)
		}
		return nil
	}); err != nil {
		return nil, err
	}

	return jsonPaths, nil
}

func main() {
	flag.Usage = func() {
		fmt.Fprint(flag.CommandLine.Output(), "Usage: main [-flags] INPUT_DIR OUTPUT DIR\n\n")
		flag.PrintDefaults()
	}
	chunksFlag := flag.Int("chunks", 8, "Number of goroutines")
	flag.Parse()

	if flag.NArg() != 2 {
		flag.Usage()
		os.Exit(1)
	}
	inputPath, outputPath := flag.Arg(0), flag.Arg(1)

	jsonPaths, err := findJsonFiles(filepath.Join(inputPath, "authors"))
	if err != nil {
		panic(err)
	}

	numChunks := *chunksFlag
	chunkSize := len(jsonPaths) / numChunks
	chunkInputs := make([][]string, numChunks)

	for chunk := range numChunks - 1 {
		chunkInputs[chunk] = jsonPaths[chunk*chunkSize : (chunk+1)*chunkSize]
	}
	chunkInputs[numChunks-1] = jsonPaths[(numChunks-1)*chunkSize:]

	pbPool, err := pb.StartPool()
	if err != nil {
		panic(err)
	}

	wg := new(sync.WaitGroup)
	for chunk, chunkInput := range chunkInputs {
		progress := pb.New(len(chunkInput))
		pbPool.Add(progress)
		wg.Add(1)

		go func() {
			defer wg.Done()
			defer progress.Finish()

			flatten.FlattenAuthors(func(yield func(string) bool) {
				for _, inputPath := range chunkInput {
					if !yield(inputPath) {
						return
					}
					progress.Increment()
				}
			}, outputPath, chunk)
		}()
	}

	wg.Wait()
	pbPool.Stop()
}
