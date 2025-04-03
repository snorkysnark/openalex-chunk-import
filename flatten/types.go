package flatten

import (
	"encoding/json"
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
