package converters

import (
	"encoding/json"
	"iter"
)

type jsontype struct {
	value any
}

func (j jsontype) MarshalCSV() ([]byte, error) {
	return json.Marshal(j.value)
}

func tryCast[T any](value any) *T {
	castValue, exists := value.(T)
	if exists {
		return &castValue
	} else {
		return nil
	}
}

type EntityType struct {
	name    string
	convert func(gzipPaths iter.Seq[string], outputPath string, chunk int)
}

func (e EntityType) Name() string {
	return e.name
}

func (e EntityType) Convert(gzipPaths iter.Seq[string], outputPath string, chunk int) {
	e.convert(gzipPaths, outputPath, chunk)
}
