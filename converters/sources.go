package converters

import (
	"encoding/json"
	"fmt"
	"iter"
	"log"
	"path/filepath"
)

type sourcesRow struct {
	Id           *string      `csv:"id"`
	IssnL        *string      `csv:"issn_l"`
	Issn         jsontype     `csv:"issn"`
	DisplayName  *string      `csv:"display_name"`
	Publisher    *string      `csv:"publisher"`
	WorksCount   *json.Number `csv:"works_count"`
	CitedByCount *json.Number `csv:"cited_by_count"`
	IsOa         *bool        `csv:"is_oa"`
	IsInDoaj     *bool        `csv:"is_in_doaj"`
	HomepageUrl  *string      `csv:"homepage_url"`
	WorksApiUrl  *string      `csv:"works_api_url"`
	UpdatedDate  *string      `csv:"updated_date"`
}

type sourcesCountsByYearRow struct {
	SourceId     *string      `csv:"source_id"`
	Year         *json.Number `csv:"year"`
	WorksCount   *json.Number `csv:"works_count"`
	CitedByCount *json.Number `csv:"cited_by_count"`
	OaWorksCount *json.Number `csv:"oa_works_count"`
}

type sourcesIdsRow struct {
	SourceId *string      `csv:"source_id"`
	Openalex *string      `csv:"openalex"`
	IssnL    *string      `csv:"issn_l"`
	Issn     jsontype     `csv:"issn"`
	Mag      *json.Number `csv:"mag"`
	Wikidata *string      `csv:"wikidata"`
	Fatcat   *string      `csv:"fatcat"`
}

func convertSources(gzipPaths iter.Seq[string], outputPath string, chunk int) {
	sourcesWriter, err := OpenCsvEncoder(filepath.Join(outputPath, "sources", fmt.Sprint("sources", chunk, ".csv.gz")), sourcesRow{})
	if err != nil {
		log.Println(err)
		return
	}
	defer sourcesWriter.Close()
	sourcesCountsWriter, err := OpenCsvEncoder(filepath.Join(outputPath, "sources", fmt.Sprint("sources_counts", chunk, ".csv.gz")), sourcesCountsByYearRow{})
	if err != nil {
		log.Println(err)
		return
	}
	defer sourcesCountsWriter.Close()
	sourcesIdsWriter, err := OpenCsvEncoder(filepath.Join(outputPath, "sources", fmt.Sprint("sources_ids", chunk, ".csv.gz")), sourcesIdsRow{})
	if err != nil {
		log.Println(err)
		return
	}
	defer sourcesIdsWriter.Close()

	for data, err := range ReadJsonLinesAll(gzipPaths) {
		if err != nil {
			log.Println(err)
			continue
		}

		sourceId := getCast[string](data, "id")
		if sourceId == nil {
			continue
		}

		if err := sourcesWriter.Encode(sourcesRow{
			Id:           sourceId,
			IssnL:        getCast[string](data, "issn_l"),
			Issn:         jsontype{data["issn"]},
			DisplayName:  getCast[string](data, "display_name"),
			Publisher:    getCast[string](data, "publisher"),
			WorksCount:   getCast[json.Number](data, "works_count"),
			CitedByCount: getCast[json.Number](data, "cited_by_count"),
			IsOa:         getCast[bool](data, "is_oa"),
			IsInDoaj:     getCast[bool](data, "is_in_doaj"),
			HomepageUrl:  getCast[string](data, "homepage_url"),
			WorksApiUrl:  getCast[string](data, "works_api_url"),
			UpdatedDate:  getCast[string](data, "updated_date"),
		}); err != nil {
			log.Println(err)
		}

		if sourceIds := getCast[map[string]any](data, "ids"); sourceIds != nil {
			if err := sourcesIdsWriter.Encode(sourcesIdsRow{
				SourceId: sourceId,
				Openalex: getCast[string](*sourceIds, "openalex"),
				IssnL:    getCast[string](*sourceIds, "issn_l"),
				Issn:     jsontype{(*sourceIds)["issn"]},
				Mag:      getCast[json.Number](*sourceIds, "mag"),
				Wikidata: getCast[string](*sourceIds, "wikidata"),
				Fatcat:   getCast[string](*sourceIds, "fatcat"),
			}); err != nil {
				log.Println(err)
			}
		}

		if countsByYear := getCast[[]any](data, "counts_by_year"); countsByYear != nil {
			for countByYear := range iterCast[map[string]any](*countsByYear) {
				if err := sourcesCountsWriter.Encode(sourcesCountsByYearRow{
					SourceId:     sourceId,
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

var TypeSources = EntityType{
	name:    "sources",
	convert: convertSources,
}
