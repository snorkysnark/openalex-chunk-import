# OpenAlex to CSV chunked

Usage:

```
go run main.go [-flags] INPUT_DIR OUTPUT DIR
```

Each entity type is processed sequentially, while within each type data is split into parallel-processed chunks

Flags:

- `-chunks`
    Number of goroutines (default 8)
- `-entities` Comma-separated entity types. If present, only these entities will be processed  
    Example: `authors,topics,concepts,institutions,publishers,sources,works`

An import script for the given number of chunks is generated in OUTPUT_DIR,
so you can load the CSVs like this:

```
duckdb openalex-shapshot.duckdb -f openalex-duckdb-schema.sql
```

```
duckdb openalex-shapshot.duckdb -f OUTPUT_DIR/duckdb_import.sql
```
