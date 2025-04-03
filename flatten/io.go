package flatten

import (
	"bufio"
	"compress/gzip"
	"encoding/csv"
	"encoding/json"
	"iter"
	"os"
	"path/filepath"

	"github.com/jszwec/csvutil"
)

func ReadJsonLines(gzipPath string) (iter.Seq2[map[string]any, error], error) {
	file, err := os.Open(gzipPath)
	if err != nil {
		return nil, err
	}

	gzReader, err := gzip.NewReader(file)
	if err != nil {
		return nil, err
	}

	scanner := bufio.NewScanner(gzReader)

	return func(yield func(map[string]any, error) bool) {
		defer file.Close()
		defer gzReader.Close()

		for scanner.Scan() {
			var data map[string]any
			err := json.Unmarshal(scanner.Bytes(), &data)

			if !yield(data, err) {
				return
			}
		}
		if err := scanner.Err(); err != nil {
			yield(nil, err)
		}
	}, nil
}

func ReadJsonLinesAll(gzipPaths iter.Seq[string]) iter.Seq2[map[string]any, error] {
	return func(yield func(map[string]any, error) bool) {
		for path := range gzipPaths {
			jsonLines, err := ReadJsonLines(path)
			if err != nil && !yield(nil, err) {
				return
			}

			jsonLines(yield)
		}
	}
}

type CsvWriterEncoder struct {
	file    *os.File
	writer  *csv.Writer
	encoder *csvutil.Encoder
}

func (csv *CsvWriterEncoder) Close() error {
	csv.writer.Flush()
	return csv.file.Close()
}

func (csv *CsvWriterEncoder) Encode(v any) error {
	return csv.encoder.Encode(v)
}

func OpenCsvEncoder(path string) (*CsvWriterEncoder, error) {
	err := os.MkdirAll(filepath.Dir(path), 0755)
	if err != nil {
		return nil, err
	}

	file, err := os.Create(path)
	if err != nil {
		return nil, err
	}

	writer := csv.NewWriter(file)
	encoder := csvutil.NewEncoder(writer)
	return &CsvWriterEncoder{file, writer, encoder}, nil
}
