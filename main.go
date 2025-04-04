package main

import (
	"flag"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/cheggaaa/pb/v3"

	"github.com/snorkysnark/openalex-chunk-import/converters"
)

func writeImportScript(outputPath string, numChunks int) error {
	if err := os.MkdirAll(filepath.Dir(outputPath), 0755); err != nil {
		return err
	}

	f, err := os.Create(filepath.Join(outputPath, "duckdb_import.sql"))
	if err != nil {
		return err
	}
	defer f.Close()

	for _, entityType := range converters.EntityTypes {
		fmt.Fprintf(f, "--%v\n", entityType.Name)
		entityType.WriteSqlImport(f, outputPath, numChunks)
		fmt.Fprintln(f)
	}
	return nil
}

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

	entityTypesMaskSeq := converters.EntityTypeNames
	flag.Func("entities", "comma-separated entity types", func(s string) error {
		entityTypesMaskSeq = strings.SplitSeq(s, ",")
		return nil
	})

	flag.Parse()

	if flag.NArg() != 2 {
		flag.Usage()
		os.Exit(1)
	}
	inputPath, outputPath := flag.Arg(0), flag.Arg(1)
	numChunks := *chunksFlag

	// Hash set of entity types that need to be converted
	entityTypeMask := map[string]struct{}{}
	for typeName := range entityTypesMaskSeq {
		entityTypeMask[typeName] = struct{}{}
	}

	fmt.Println("Writing import script")
	if err := writeImportScript(outputPath, numChunks); err != nil {
		panic(err)
	}

	for _, entityType := range converters.EntityTypes {
		if _, exists := entityTypeMask[entityType.Name]; !exists {
			continue
		}
		fmt.Println("Converting", entityType.Name)

		jsonPaths, err := findJsonFiles(filepath.Join(inputPath, entityType.Name))
		if err != nil {
			panic(err)
		}

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

				entityType.Convert(func(yield func(string) bool) {
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
}
