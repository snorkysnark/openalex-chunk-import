package converters

import (
	"encoding/json"
	"fmt"
	"io"
	"iter"
	"log"
	"path/filepath"
)

// CREATE TABLE openalex.sources (
//     id text NOT NULL,
//     issn_l text,
//     issn json,
//     display_name text,
//     publisher text,
//     works_count integer,
//     cited_by_count integer,
//     is_oa boolean,
//     is_in_doaj boolean,
//     homepage_url text,
//     works_api_url text,
//     updated_date timestamp without time zone
// );

type sourcesRow struct {
	Id           *string      `csv:"id" sqltype:"TEXT"`
	IssnL        *string      `csv:"issn_l" sqltype:"TEXT"`
	Issn         jsontype     `csv:"issn" sqltype:"JSON"`
	DisplayName  *string      `csv:"display_name" sqltype:"TEXT"`
	Publisher    *string      `csv:"publisher" sqltype:"TEXT"`
	WorksCount   *json.Number `csv:"works_count" sqltype:"INTEGER"`
	CitedByCount *json.Number `csv:"cited_by_count" sqltype:"INTEGER"`
	IsOa         *bool        `csv:"is_oa" sqltype:"BOOLEAN"`
	IsInDoaj     *bool        `csv:"is_in_doaj" sqltype:"BOOLEAN"`
	HomepageUrl  *string      `csv:"homepage_url" sqltype:"TEXT"`
	WorksApiUrl  *string      `csv:"works_api_url" sqltype:"TEXT"`
	UpdatedDate  *string      `csv:"updated_date" sqltype:"TIMESTAMP"`
}

// CREATE TABLE openalex.sources_counts_by_year (
//     source_id text NOT NULL,
//     year integer NOT NULL,
//     works_count integer,
//     cited_by_count integer,
//     oa_works_count integer
// );

type sourcesCountsByYearRow struct {
	SourceId     *string      `csv:"source_id" sqltype:"TEXT"`
	Year         *json.Number `csv:"year" sqltype:"INTEGER"`
	WorksCount   *json.Number `csv:"works_count" sqtype:"INTEGER"`
	CitedByCount *json.Number `csv:"cited_by_count" sqltype:"INTEGER"`
	OaWorksCount *json.Number `csv:"oa_works_count" sqltype:"INTEGER"`
}

// CREATE TABLE openalex.sources_ids (
//     source_id text,
//     openalex text,
//     issn_l text,
//     issn json,
//     mag bigint,
//     wikidata text,
//     fatcat text
// );

type sourcesIdsRow struct {
	SourceId *string      `csv:"source_id" sqltype:"TEXT"`
	Openalex *string      `csv:"openalex" sqltype:"TEXT"`
	IssnL    *string      `csv:"issn_l" sqltype:"TEXT"`
	Issn     jsontype     `csv:"issn" sqltype:"JSON"`
	Mag      *json.Number `csv:"mag" sqltype:"BIGINT"`
	Wikidata *string      `csv:"wikidata" sqltype:"TEXT"`
	Fatcat   *string      `csv:"fatcat" sqltype:"TEXT"`
}

func convertSources(gzipPaths iter.Seq[string], outputPath string, chunk int) {
	sourcesWriter, err := OpenCsvEncoder(filepath.Join(outputPath, "sources", fmt.Sprint("sources", chunk, ".csv.gz")), sourcesRow{})
	if err != nil {
		log.Println(err)
		return
	}
	defer sourcesWriter.Close()
	sourcesCountsWriter, err := OpenCsvEncoder(filepath.Join(outputPath, "sources", fmt.Sprint("sources_counts_by_year", chunk, ".csv.gz")), sourcesCountsByYearRow{})
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
	Name:    "sources",
	Convert: convertSources,
	WriteSqlImport: func(w io.Writer, outputPath string, numChunks int) {
		basePath := filepath.Join(outputPath, "sources")

		writeDuckdbCopy(w, sourcesRow{}, "sources", basePath, numChunks)
		writeDuckdbCopy(w, sourcesCountsByYearRow{}, "sources_counts_by_year", basePath, numChunks)
		writeDuckdbCopy(w, sourcesIdsRow{}, "sources_ids", basePath, numChunks)
	},
}
