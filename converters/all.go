package converters

import (
	"io"
	"iter"
)

type EntityType struct {
	Name           string
	Convert        func(gzipPaths iter.Seq[string], outputPath string, chunk int)
	WriteSqlImport func(w io.Writer, outputPath string, numChunks int)
}

var EntityTypes = []EntityType{TypeAuthors, TypeTopics, TypeConcepts, TypeInstitutions, TypePublishers, TypeSources, TypeWorks}

func EntityTypeNames(yield func(string) bool) {
	for _, entityType := range EntityTypes {
		if !yield(entityType.Name) {
			return
		}
	}
}
