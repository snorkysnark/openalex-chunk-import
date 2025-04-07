package converters

import (
	"encoding/json"
	"fmt"
	"io"
	"iter"
	"log"
	"path/filepath"
)

// CREATE TABLE openalex.authors (
//     id text NOT NULL,
//     orcid text,
//     display_name text,
//     display_name_alternatives json,
//     works_count integer,
//     cited_by_count integer,
//     last_known_institution text,
//     works_api_url text,
//     updated_date timestamp without time zone
// );

type authorRow struct {
	Id                      *string      `csv:"id" sqltype:"TEXT"`
	Orcid                   *string      `csv:"orcid" sqltype:"TEXT"`
	DisplayName             *string      `csv:"display_name" sqltype:"TEXT"`
	DisplayNameAlternatives jsontype     `csv:"display_name_alternatives" sqltype:"JSON"`
	WorksCount              *json.Number `csv:"works_count" sqltype:"INTEGER"`
	CitedByCount            *json.Number `csv:"cited_by_count" sqltype:"INTEGER"`
	LastKnownInstitution    *string      `csv:"last_known_institution" sqltype:"TEXT"`
	WorksApiUrl             *string      `csv:"works_api_url" sqltype:"TEXT"`
	UpdatedDate             *string      `csv:"updated_date" sqltype:"TIMESTAMP"`
}

// CREATE TABLE openalex.authors_counts_by_year (
//     author_id text NOT NULL,
//     year integer NOT NULL,
//     works_count integer,
//     cited_by_count integer,
//     oa_works_count integer
// );

type authorCountsByYearRow struct {
	AuthorId     *string      `csv:"author_id" sqltype:"TEXT"`
	Year         *json.Number `csv:"year" sqltype:"INTEGER"`
	WorksCount   *json.Number `csv:"works_count" sqltype:"INTEGER"`
	CitedByCount *json.Number `csv:"cited_by_count" sqltype:"INTEGER"`
	OaWorksCount *json.Number `csv:"oa_works_count" sqltype:"INTEGER"`
}

// CREATE TABLE openalex.authors_ids (
//     author_id text NOT NULL,
//     openalex text,
//     orcid text,
//     scopus text,
//     twitter text,
//     wikipedia text,
//     mag bigint
// );

type authorIdsRow struct {
	AuthorId  *string      `csv:"author_id" sqltype:"TEXT"`
	Openalex  *string      `csv:"openalex" sqltype:"TEXT"`
	Orcid     *string      `csv:"orcid" sqltype:"TEXT"`
	Scopus    *string      `csv:"scopus" sqltype:"TEXT"`
	Twitter   *string      `csv:"twitter" sqltype:"TEXT"`
	Wikipedia *string      `csv:"wikipedia" sqltype:"TEXT"`
	Mag       *json.Number `csv:"mag" sqltype:"BIGINT"`
}

func convertAuthors(gzipPaths iter.Seq[string], outputPath string, chunk int) {
	authorsWriter, err := OpenCsvEncoder(filepath.Join(outputPath, "authors", fmt.Sprint("authors", chunk, ".csv.gz")), authorRow{})
	if err != nil {
		log.Println(err)
		return
	}
	defer authorsWriter.Close()
	authorCountsWriter, err := OpenCsvEncoder(filepath.Join(outputPath, "authors", fmt.Sprint("authors_counts_by_year", chunk, ".csv.gz")), authorCountsByYearRow{})
	if err != nil {
		log.Println(err)
		return
	}
	defer authorCountsWriter.Close()
	authorIdsWriter, err := OpenCsvEncoder(filepath.Join(outputPath, "authors", fmt.Sprint("authors_ids", chunk, ".csv.gz")), authorIdsRow{})
	if err != nil {
		log.Println(err)
		return
	}
	defer authorIdsWriter.Close()

	for data, err := range ReadJsonLinesAll(gzipPaths) {
		if err != nil {
			log.Println(err)
			continue
		}

		authorId := getCast[string](data, "id")
		if authorId == nil {
			continue
		}

		var lastKnownInstitutionId *string
		if lastKnownInstitution := getCast[map[string]any](data, "last_known_institution"); lastKnownInstitution != nil {
			lastKnownInstitutionId = getCast[string](*lastKnownInstitution, "id")
		}

		if err := authorsWriter.Encode(authorRow{
			Id:          authorId,
			Orcid:       getCast[string](data, "orcid"),
			DisplayName: getCast[string](data, "display_name"),
			DisplayNameAlternatives: jsontype{
				value: data["display_name_alternatives"],
			},
			WorksCount:           getCast[json.Number](data, "works_count"),
			CitedByCount:         getCast[json.Number](data, "cited_by_count"),
			LastKnownInstitution: lastKnownInstitutionId,
			WorksApiUrl:          getCast[string](data, "works_api_url"),
			UpdatedDate:          getCast[string](data, "updated_date"),
		}); err != nil {
			log.Println(err)
		}

		if authorIds := getCast[map[string]any](data, "ids"); authorIds != nil {
			if err := authorIdsWriter.Encode(authorIdsRow{
				AuthorId:  authorId,
				Openalex:  getCast[string](*authorIds, "openalex"),
				Orcid:     getCast[string](*authorIds, "orcid"),
				Scopus:    getCast[string](*authorIds, "scopus"),
				Twitter:   getCast[string](*authorIds, "twitter"),
				Wikipedia: getCast[string](*authorIds, "wikipedia"),
				Mag:       getCast[json.Number](*authorIds, "mag"),
			}); err != nil {
				log.Println(err)
			}
		}

		if countsByYear := getCast[[]any](data, "counts_by_year"); countsByYear != nil {
			for countByYear := range iterCast[map[string]any](*countsByYear) {
				if err := authorCountsWriter.Encode(authorCountsByYearRow{
					AuthorId:     authorId,
					Year:         getCast[json.Number](*countByYear, "year"),
					WorksCount:   getCast[json.Number](*countByYear, "works_count"),
					CitedByCount: getCast[json.Number](*countByYear, "cited_by_count"),
					OaWorksCount: getCast[json.Number](*countByYear, "oa_works_count"),
				}); err != nil {
					log.Println(err)
				}
			}
		}
	}
}

var TypeAuthors = EntityType{
	Name:    "authors",
	Convert: convertAuthors,
	WriteSqlImport: func(w io.Writer, outputPath string, numChunks int) {
		basePath := filepath.Join(outputPath, "authors")

		writeDuckdbCopy(w, authorRow{}, "authors", basePath, numChunks)
		writeDuckdbCopy(w, authorCountsByYearRow{}, "authors_counts_by_year", basePath, numChunks)
		writeDuckdbCopy(w, authorIdsRow{}, "authors_ids", basePath, numChunks)
	},
}
