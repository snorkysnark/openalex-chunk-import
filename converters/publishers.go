package converters

import (
	"encoding/json"
	"fmt"
	"iter"
	"log"
	"path/filepath"
)

type publisherRow struct {
	Id              *string      `csv:"id"`
	DisplayName     *string      `csv:"display_name"`
	AlternateTitles jsontype     `csv:"alternate_titles"`
	CountryCodes    jsontype     `csv:"country_codes"`
	HierarchyLevel  *json.Number `csv:"hierarchy_level"`
	ParentPublisher *string      `csv:"parent_publisher"`
	WorksCount      *json.Number `csv:"works_count"`
	CitedByCount    *json.Number `csv:"cited_by_count"`
	SourcesApiUrl   *string      `csv:"sources_api_url"`
	UpdatedDate     *string      `csv:"updated_date"`
}

type publishersCountsByYearRow struct {
	PublisherId  *string      `csv:"publisher_id"`
	Year         *json.Number `csv:"year"`
	WorksCount   *json.Number `csv:"works_count"`
	CitedByCount *json.Number `csv:"cited_by_count"`
	OaWorksCount *json.Number `csv:"oa_works_count"`
}

type publishersIdsRow struct {
	PublisherId *string `csv:"publisher_id"`
	Openalex    *string `csv:"openalex"`
	Ror         *string `csv:"ror"`
	Wikidata    *string `csv:"wikidata"`
}

func convertPublishers(gzipPaths iter.Seq[string], outputPath string, chunk int) {
	publishersWriter, err := OpenCsvEncoder(filepath.Join(outputPath, "publishers", fmt.Sprint("publishers", chunk, ".csv.gz")), publisherRow{})
	if err != nil {
		log.Println(err)
		return
	}
	defer publishersWriter.Close()
	publishersCountsWriter, err := OpenCsvEncoder(filepath.Join(outputPath, "publishers", fmt.Sprint("publishers_counts", chunk, ".csv.gz")), publishersCountsByYearRow{})
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
	name:    "publishers",
	convert: convertPublishers,
}
