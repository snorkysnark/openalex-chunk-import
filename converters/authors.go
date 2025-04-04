package converters

import (
	"encoding/json"
	"fmt"
	"io"
	"iter"
	"log"
	"path/filepath"
)

type authorRow struct {
	Id                      *string      `csv:"id"`
	Orcid                   *string      `csv:"orcid"`
	DisplayName             *string      `csv:"display_name"`
	DisplayNameAlternatives jsontype     `csv:"display_name_alternatives"`
	WorksCount              *json.Number `csv:"works_count"`
	CitedByCount            *json.Number `csv:"cited_by_count"`
	LastKnownInstitution    *string      `csv:"last_known_institution"`
	WorksApiUrl             *string      `csv:"works_api_url"`
	UpdatedDate             *string      `csv:"updated_date"`
}

type authorCountsByYearRow struct {
	AuthorId     *string      `csv:"author_id"`
	Year         *json.Number `csv:"year"`
	WorksCount   *json.Number `csv:"works_count"`
	CitedByCount *json.Number `csv:"cited_by_count"`
	OaWorksCount *json.Number `csv:"oa_works_count"`
}

type authorIdsRow struct {
	AuthorId  *string      `csv:"author_id"`
	Openalex  *string      `csv:"openalex"`
	Orcid     *string      `csv:"orcid"`
	Scopus    *string      `csv:"scopus"`
	Twitter   *string      `csv:"twitter"`
	Wikipedia *string      `csv:"wikipedia"`
	Mag       *json.Number `csv:"mag"`
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
