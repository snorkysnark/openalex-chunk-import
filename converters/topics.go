package converters

import (
	"encoding/json"
	"fmt"
	"iter"
	"log"
	"path/filepath"
	"strings"

	"github.com/samber/lo"
)

type topicsRow struct {
	Id                  *string      `csv:"id"`
	DisplayName         *string      `csv:"display_name"`
	SubfieldId          *string      `csv:"subfield_id"`
	SubfieldDisplayName *string      `csv:"subfield_display_name"`
	FieldId             *string      `csv:"field_id"`
	FieldDisplayName    *string      `csv:"field_display_name"`
	DomainId            *string      `csv:"domain_id"`
	DomainDisplayName   *string      `csv:"domain_display_name"`
	Description         *string      `csv:"description"`
	Keywords            *string      `csv:"keywords"`
	WorksApiUrl         *string      `csv:"works_api_url"`
	WikipediaId         *string      `csv:"wikipedia_id"`
	WorksCount          *json.Number `csv:"works_count"`
	CitedByCount        *json.Number `csv:"cited_by_count"`
	UpdatedDate         *string      `csv:"updated_date"`
	Siblings            jsontype     `csv:"siblings"`
}

func getIdAndDisplayName(key string, data map[string]any) (*string, *string) {
	if section := getCast[map[string]any](data, key); section != nil {
		return getCast[string](*section, "id"), getCast[string](*section, "display_name")
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

		topicId := getCast[string](data, "id")
		if topicId == nil {
			continue
		}

		var keywords *string
		if keywordsArr := getCast[[]any](data, "keywords"); keywordsArr != nil {
			k := strings.Join(lo.Map(*keywordsArr, func(item any, index int) string {
				return item.(string)
			}), "; ")
			keywords = &k
		}

		subfieldId, subfieldDisplayName := getIdAndDisplayName("subfield", data)
		fieldId, fieldDisplayName := getIdAndDisplayName("field", data)
		domainId, domainDisplayName := getIdAndDisplayName("domain", data)

		var wikipediaId *string
		if ids := getCast[map[string]any](data, "ids"); ids != nil {
			wikipediaId = getCast[string](*ids, "wikipedia")
		}

		updatedDate := getCast[string](data, "updated_date")
		if updated := getCast[map[string]any](data, "updated"); updated != nil {
			updatedDate = getCast[string](*updated, "date")
		}

		if err := topicsWriter.Encode(topicsRow{
			Id:                  topicId,
			DisplayName:         getCast[string](data, "display_name"),
			SubfieldId:          subfieldId,
			SubfieldDisplayName: subfieldDisplayName,
			FieldId:             fieldId,
			FieldDisplayName:    fieldDisplayName,
			DomainId:            domainId,
			DomainDisplayName:   domainDisplayName,
			Description:         getCast[string](data, "description"),
			Keywords:            keywords,
			WorksApiUrl:         getCast[string](data, "works_api_url"),
			WikipediaId:         wikipediaId,
			WorksCount:          getCast[json.Number](data, "works_count"),
			CitedByCount:        getCast[json.Number](data, "cited_by_count"),
			UpdatedDate:         updatedDate,
			Siblings:            jsontype{value: data["siblings"]},
		}); err != nil {
			log.Println(err)
		}
	}
}

var TypeTopics = EntityType{
	name:    "topics",
	convert: convertTopics,
}
