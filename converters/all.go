package converters

import "iter"

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

var EntityTypes = []EntityType{TypeAuthors, TypeTopics, TypeConcepts, TypeInstitutions}

func EntityTypeNames(yield func(string) bool) {
	for _, entityType := range EntityTypes {
		if !yield(entityType.Name()) {
			return
		}
	}
}
