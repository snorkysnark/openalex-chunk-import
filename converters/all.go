package converters

var EntityTypes = []EntityType{TypeAuthors}

func EntityTypeNames(yield func(string) bool) {
	for _, entityType := range EntityTypes {
		if !yield(entityType.Name()) {
			return
		}
	}
}
