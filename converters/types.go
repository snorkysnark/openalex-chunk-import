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
	castValue, ok := value.(T)
	if ok {
		return &castValue
	} else {
		return nil
	}
}

func notNil[T any](value *T) bool {
	return value != nil
}

func getCast[T any](m map[string]any, key string) *T {
	valAny, exists := m[key]
	if !exists {
		return nil
	}

	return tryCast[T](valAny)
}

func iterCast[T any](arr []any) iter.Seq[*T] {
	return func(yield func(*T) bool) {
		for _, item := range arr {
			itemCast := tryCast[T](item)
			if itemCast != nil && !yield(itemCast) {
				return
			}
		}
	}
}
