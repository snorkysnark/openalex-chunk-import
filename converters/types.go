package converters

import (
	"encoding/json"
)

type jsontype struct {
	value any
}

func (j jsontype) MarshalCSV() ([]byte, error) {
	return json.Marshal(j.value)
}

func getCast[T any](m map[string]any, key string) *T {
	valAny, exists := m[key]
	if !exists {
		return nil
	}

	val, success := valAny.(T)
	if !success {
		return nil
	}

	return &val
}
