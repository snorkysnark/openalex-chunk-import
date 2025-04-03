package converters

import (
	"fmt"
	"iter"
	"log"
	"path/filepath"
)

type authorRow struct {
	Id                      *string  `csv:"id"`
	Orcid                   *string  `csv:"orcid"`
	DisplayName             *string  `csv:"display_name"`
	DisplayNameAlternatives jsontype `csv:"display_name_alternatives"`
	WorksCount              *int     `csv:"works_count"`
	CitedByCount            *int     `csv:"cited_by_count"`
	LastKnownInstitution    *string  `csv:"last_known_institution"`
	WorksApiUrl             *string  `csv:"works_api_url"`
	UpdatedDate             *string  `csv:"updated_date"`
}

type authorCountsByYearRow struct {
	AuthorId     *string `csv:"author_id"`
	Year         *int    `csv:"year"`
	WorksCount   *int    `csv:"works_count"`
	CitedByCount *int    `csv:"cited_by_count"`
	OaWorksCount *int    `csv:"oa_works_count"`
}

type authorIdsRow struct {
	AuthorId  *string `csv:"author_id"`
	Openalex  *string `csv:"openalex"`
	Orcid     *string `csv:"orcid"`
	Scopus    *string `csv:"scopus"`
	Twitter   *string `csv:"twitter"`
	Wikipedia *string `csv:"wikipedia"`
	Mag       *int64  `csv:"mag"`
}

func convertAuthors(gzipPaths iter.Seq[string], outputPath string, chunk int) {
	authorsWriter, err := OpenCsvEncoder(filepath.Join(outputPath, "authors", fmt.Sprint("authors", chunk, ".csv.gz")))
	if err != nil {
		log.Println(err)
		return
	}
	defer authorsWriter.Close()
	authorCountsWriter, err := OpenCsvEncoder(filepath.Join(outputPath, "authors", fmt.Sprint("author_counts", chunk, ".csv.gz")))
	if err != nil {
		log.Println(err)
		return
	}
	defer authorCountsWriter.Close()
	authorIdsWriter, err := OpenCsvEncoder(filepath.Join(outputPath, "authors", fmt.Sprint("author_ids", chunk, ".csv.gz")))
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
			WorksCount:           getCast[int](data, "works_count"),
			CitedByCount:         getCast[int](data, "cited_by_count"),
			LastKnownInstitution: lastKnownInstitutionId,
			WorksApiUrl:          getCast[string](data, "works_api_url"),
			UpdatedDate:          getCast[string](data, "updated_date"),
		}); err != nil {
			log.Println(err)
		}

		if authorIds := data["ids"]; authorIds != nil {
			authorIds := authorIds.(map[string]any)

			if err := authorIdsWriter.Encode(authorIdsRow{
				AuthorId:  authorId,
				Openalex:  getCast[string](authorIds, "openalex"),
				Orcid:     getCast[string](authorIds, "orcid"),
				Scopus:    getCast[string](authorIds, "scopus"),
				Twitter:   getCast[string](authorIds, "twitter"),
				Wikipedia: getCast[string](authorIds, "wikipedia"),
				Mag:       getCast[int64](authorIds, "mag"),
			}); err != nil {
				log.Println(err)
			}
		}

		if countsByYear := data["counts_by_year"]; countsByYear != nil {
			countsByYear := countsByYear.([]any)

			for _, countByYear := range countsByYear {
				countByYear := countByYear.(map[string]any)

				if err := authorCountsWriter.Encode(authorCountsByYearRow{
					AuthorId:     authorId,
					Year:         getCast[int](countByYear, "year"),
					WorksCount:   getCast[int](countByYear, "works_count"),
					CitedByCount: getCast[int](countByYear, "cited_by_count"),
					OaWorksCount: getCast[int](countByYear, "oa_works_count"),
				}); err != nil {
					log.Println(err)
				}
			}
		}
	}
}

var TypeAuthors = EntityType{
	name:    "authors",
	convert: convertAuthors,
}
