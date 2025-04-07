package converters

import (
	"encoding/json"
	"fmt"
	"io"
	"iter"
	"log"
	"path/filepath"
)

// CREATE TABLE openalex.publishers (
//     id text NOT NULL,
//     display_name text,
//     alternate_titles json,
//     country_codes json,
//     hierarchy_level integer,
//     parent_publisher text,
//     works_count integer,
//     cited_by_count integer,
//     sources_api_url text,
//     updated_date timestamp without time zone
// );

type publisherRow struct {
	Id              *string      `csv:"id" sqltype:"TEXT"`
	DisplayName     *string      `csv:"display_name" sqltype:"TEXT"`
	AlternateTitles jsontype     `csv:"alternate_titles" sqltype:"JSON"`
	CountryCodes    jsontype     `csv:"country_codes" sqltype:"JSON"`
	HierarchyLevel  *json.Number `csv:"hierarchy_level" sqltype:"INTEGER"`
	ParentPublisher *string      `csv:"parent_publisher" sqltype:"TEXT"`
	WorksCount      *json.Number `csv:"works_count" sqltype:"INTEGER"`
	CitedByCount    *json.Number `csv:"cited_by_count" sqltype:"INTEGER"`
	SourcesApiUrl   *string      `csv:"sources_api_url" sqltype:"TEXT"`
	UpdatedDate     *string      `csv:"updated_date" sqltype:"TIMESTAMP"`
}

// CREATE TABLE openalex.publishers_counts_by_year (
//     publisher_id text NOT NULL,
//     year integer NOT NULL,
//     works_count integer,
//     cited_by_count integer,
//     oa_works_count integer
// );

type publishersCountsByYearRow struct {
	PublisherId  *string      `csv:"publisher_id" sqltype:"TEXT"`
	Year         *json.Number `csv:"year" sqltype:"INTEGER"`
	WorksCount   *json.Number `csv:"works_count" sqltype:"INTEGER"`
	CitedByCount *json.Number `csv:"cited_by_count" sqltype:"INTEGER"`
	OaWorksCount *json.Number `csv:"oa_works_count" sqltype:"INTEGER"`
}

// CREATE TABLE openalex.publishers_ids (
//     publisher_id text,
//     openalex text,
//     ror text,
//     wikidata text
// );

type publishersIdsRow struct {
	PublisherId *string `csv:"publisher_id" sqltype:"TEXT"`
	Openalex    *string `csv:"openalex" sqltype:"TEXT"`
	Ror         *string `csv:"ror" sqltype:"TEXT"`
	Wikidata    *string `csv:"wikidata" sqltype:"TEXT"`
}

func convertPublishers(gzipPaths iter.Seq[string], outputPath string, chunk int) {
	publishersWriter, err := OpenCsvEncoder(filepath.Join(outputPath, "publishers", fmt.Sprint("publishers", chunk, ".csv.gz")), publisherRow{})
	if err != nil {
		log.Println(err)
		return
	}
	defer publishersWriter.Close()
	publishersCountsWriter, err := OpenCsvEncoder(filepath.Join(outputPath, "publishers", fmt.Sprint("publishers_counts_by_year", chunk, ".csv.gz")), publishersCountsByYearRow{})
	if err != nil {
		log.Println(err)
		return
	}
	defer publishersCountsWriter.Close()
	publishersIdsWriter, err := OpenCsvEncoder(filepath.Join(outputPath, "publishers", fmt.Sprint("publishers_ids", chunk, ".csv.gz")), publishersIdsRow{})
	if err != nil {
		log.Println(err)
		return
	}
	defer publishersIdsWriter.Close()

	for data, err := range ReadJsonLinesAll(gzipPaths) {
		if err != nil {
			log.Println(err)
			continue
		}

		publisherId := getCast[string](data, "id")
		if publisherId == nil {
			continue
		}

		if err := publishersWriter.Encode(publisherRow{
			Id:              publisherId,
			DisplayName:     getCast[string](data, "display_name"),
			AlternateTitles: jsontype{data["alternate_titles"]},
			CountryCodes:    jsontype{data["country_codes"]},
			HierarchyLevel:  getCast[json.Number](data, "hierarchy_level"),
			ParentPublisher: getCast[string](data, "parent_publisher"),
			WorksCount:      getCast[json.Number](data, "works_count"),
			CitedByCount:    getCast[json.Number](data, "cited_by_count"),
			SourcesApiUrl:   getCast[string](data, "sources_api_url"),
			UpdatedDate:     getCast[string](data, "updated_date"),
		}); err != nil {
			log.Println(err)
		}

		if publisherIds := getCast[map[string]any](data, "ids"); publisherIds != nil {
			if err := publishersIdsWriter.Encode(publishersIdsRow{
				PublisherId: publisherId,
				Openalex:    getCast[string](*publisherIds, "openalex"),
				Ror:         getCast[string](*publisherIds, "ror"),
				Wikidata:    getCast[string](*publisherIds, "wikidata"),
			}); err != nil {
				log.Println(err)
			}
		}

		if countsByYear := getCast[[]any](data, "counts_by_year"); countsByYear != nil {
			for countByYear := range iterCast[map[string]any](*countsByYear) {
				if err := publishersCountsWriter.Encode(publishersCountsByYearRow{
					PublisherId:  publisherId,
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

var TypePublishers = EntityType{
	Name:    "publishers",
	Convert: convertPublishers,
	WriteSqlImport: func(w io.Writer, outputPath string, numChunks int) {
		basePath := filepath.Join(outputPath, "publishers")

		writeDuckdbCopy(w, publisherRow{}, "publishers", basePath, numChunks)
		writeDuckdbCopy(w, publishersCountsByYearRow{}, "publishers_counts_by_year", basePath, numChunks)
		writeDuckdbCopy(w, publishersIdsRow{}, "publishers_ids", basePath, numChunks)
	},
}
