package converters

import (
	"fmt"
	"github.com/samber/lo"
	"iter"
	"log"
	"path/filepath"
	"strings"
)

type topicsRow struct {
	Id                  *string  `csv:"id"`
	DisplayName         *string  `csv:"display_name"`
	SubfieldId          *string  `csv:"subfield_id"`
	SubfieldDisplayName *string  `csv:"subfield_display_name"`
	FieldId             *string  `csv:"field_id"`
	FieldDisplayName    *string  `csv:"field_display_name"`
	DomainId            *string  `csv:"domain_id"`
	DomainDisplayName   *string  `csv:"domain_display_name"`
	Description         *string  `csv:"description"`
	Keywords            *string  `csv:"keywords"`
	WorksApiUrl         *string  `csv:"works_api_url"`
	WikipediaId         *string  `csv:"wikipedia_id"`
	WorksCount          *int     `csv:"works_count"`
	CitedByCount        *int     `csv:"cited_by_count"`
	UpdatedDate         *string  `csv:"updated_date"`
	Siblings            jsontype `csv:"siblings"`
}

func getIdAndDisplayName(key string, data map[string]any) (*string, *string) {
	if section := data[key]; section != nil {
		section := section.(map[string]any)
		return tryCast[string](section["id"]), tryCast[string](section["display_name"])
	}
	return nil, nil
}

func convertTopics(gzipPaths iter.Seq[string], outputPath string, chunk int) {
	topicsWriter, err := OpenCsvEncoder(filepath.Join(outputPath, "topics", fmt.Sprint("topics", chunk, ".csv.gz")))
	if err != nil {
		log.Println(err)
		return
	}
	defer topicsWriter.Close()

	for data, err := range ReadJsonLinesAll(gzipPaths) {
		if err != nil {
			log.Println(err)
			return
		}

		topicId, exists := data["id"]
		if !exists {
			continue
		}

		keywords := new(string)
		if keywordsArr, exists := data["keywords"]; exists {
			*keywords = strings.Join(lo.Map(keywordsArr.([]any), func(item any, index int) string {
				return item.(string)
			}), "; ")
		}

		subfieldId, subfieldDisplayName := getIdAndDisplayName("subfield", data)
		fieldId, fieldDisplayName := getIdAndDisplayName("field", data)
		domainId, domainDisplayName := getIdAndDisplayName("domain", data)

		var wikipediaId *string
		if ids := data["ids"]; ids != nil {
			ids := ids.(map[string]any)
			wikipediaId = tryCast[string](ids["wikipedia"])
		}

		updatedDate := tryCast[string](data["updated_date"])
		if updated := data["updated"]; updated != nil {
			updatedDate = tryCast[string](updated)
		}

		topicsWriter.Encode(topicsRow{
			Id:                  tryCast[string](topicId),
			DisplayName:         tryCast[string](data["display_name"]),
			SubfieldId:          subfieldId,
			SubfieldDisplayName: subfieldDisplayName,
			FieldId:             fieldId,
			FieldDisplayName:    fieldDisplayName,
			DomainId:            domainId,
			DomainDisplayName:   domainDisplayName,
			Description:         tryCast[string](data["description"]),
			Keywords:            keywords,
			WorksApiUrl:         tryCast[string](data["works_api_url"]),
			WikipediaId:         wikipediaId,
			WorksCount:          tryCast[int](data["works_count"]),
			CitedByCount:        tryCast[int](data["cited_by_count"]),
			UpdatedDate:         updatedDate,
			Siblings:            jsontype{value: data["siblings"]},
		})
	}
}

var TypeTopics = EntityType{
	name:    "topics",
	convert: convertTopics,
}
