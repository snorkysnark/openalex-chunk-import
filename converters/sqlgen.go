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
	fieldTypes := make([]string, t.NumField())
	for i := range t.NumField() {
		field := t.Field(i)

		fieldNames[i] = field.Tag.Get("csv")
		fieldTypes[i] = fmt.Sprintf("'%v': '%v'", field.Tag.Get("csv"), field.Tag.Get("sqltype"))
	}

	for chunk := range numChunks {
		fmt.Fprintf(
			w,
			"INSERT INTO openalex.%v(%v)\nSELECT * FROM read_csv('%v', columns = {%v});\n",
			table, strings.Join(fieldNames, ", "),
			filepath.Join(basePath, fmt.Sprintf("%v%v.csv.gz", table, chunk)),
			strings.Join(fieldTypes, ", "),
		)
	}
}
