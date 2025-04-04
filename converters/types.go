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

func getCastAt[T any](m map[string]any, path []string) *T {
	var current any = m
	for _, key := range path {
		currentMap, ok := current.(map[string]any)
		if !ok {
			return nil
		}
		current = currentMap[key]
	}

	return tryCast[T](current)
}
