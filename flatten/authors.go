package flatten

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

func FlattenAuthors(gzipPaths iter.Seq[string], outputPath string, chunk int) {
	authorsWriter, err := OpenCsvEncoder(filepath.Join(outputPath, "authors", fmt.Sprint("authors", chunk, ".csv")))
	if err != nil {
		log.Println(err)
		return
	}
	defer authorsWriter.Close()
	authorCountsWriter, err := OpenCsvEncoder(filepath.Join(outputPath, "authors", fmt.Sprint("author_counts", chunk, ".csv")))
	if err != nil {
		log.Println(err)
		return
	}
	defer authorCountsWriter.Close()
	authorIdsWriter, err := OpenCsvEncoder(filepath.Join(outputPath, "authors", fmt.Sprint("author_ids", chunk, ".csv")))
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

		authorId, hasId := data["id"]
		if !hasId {
			continue
		}

		var lastKnownInstitutionId any
		if lastKnownInstitution := data["last_known_institution"]; lastKnownInstitution != nil {
			lastKnownInstitutionId = lastKnownInstitution.(map[string]any)["id"]
		}

		if err := authorsWriter.Encode(authorRow{
			Id:          tryCast[string](authorId),
			Orcid:       tryCast[string](data["orcid"]),
			DisplayName: tryCast[string](data["display_name"]),
			DisplayNameAlternatives: jsontype{
				value: data["display_name_alternatives"],
			},
			WorksCount:           tryCast[int](data["works_count"]),
			CitedByCount:         tryCast[int](data["cited_by_count"]),
			LastKnownInstitution: tryCast[string](lastKnownInstitutionId),
			WorksApiUrl:          tryCast[string](data["works_api_url"]),
			UpdatedDate:          tryCast[string](data["updated_date"]),
		}); err != nil {
			log.Println(err)
		}

		if authorIds := data["ids"]; authorIds != nil {
			authorIds := authorIds.(map[string]any)

			if err := authorIdsWriter.Encode(authorIdsRow{
				AuthorId:  tryCast[string](authorId),
				Openalex:  tryCast[string](authorIds["openalex"]),
				Orcid:     tryCast[string](authorIds["orcid"]),
				Scopus:    tryCast[string](authorIds["scopus"]),
				Twitter:   tryCast[string](authorIds["twitter"]),
				Wikipedia: tryCast[string](authorIds["wikipedia"]),
				Mag:       tryCast[int64](authorIds["mag"]),
			}); err != nil {
				log.Println(err)
			}
		}

		if countsByYear := data["counts_by_year"]; countsByYear != nil {
			countsByYear := countsByYear.([]any)

			for _, countByYear := range countsByYear {
				countByYear := countByYear.(map[string]any)

				if err := authorCountsWriter.Encode(authorCountsByYearRow{
					AuthorId:     tryCast[string](authorId),
					Year:         tryCast[int](countByYear["year"]),
					WorksCount:   tryCast[int](countByYear["works_count"]),
					CitedByCount: tryCast[int](countByYear["cited_by_count"]),
					OaWorksCount: tryCast[int](countByYear["oa_works_count"]),
				}); err != nil {
					log.Println(err)
				}
			}
		}
	}
}
