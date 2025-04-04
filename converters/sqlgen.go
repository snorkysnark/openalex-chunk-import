package converters

import (
	"fmt"
	"io"
	"path/filepath"
	"reflect"
	"strings"
)

func writeDuckdbCopy(w io.Writer, schema any, table string, basePath string, numChunks int) {
	t := reflect.TypeOf(schema)

	fieldNames := make([]string, t.NumField())
	for i := range t.NumField() {
		fieldNames[i] = t.Field(i).Tag.Get("csv")
	}
	fieldNamesStr := strings.Join(fieldNames, ", ")

	for chunk := range numChunks {
		fmt.Fprintf(
			w,
			"COPY openalex.%v(%v) FROM '%v';\n",
			table, fieldNamesStr, filepath.Join(basePath, fmt.Sprintf("%v%v.csv.gz", table, chunk)),
		)
	}
}
