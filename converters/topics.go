package converters

import (
	"encoding/json"
	"fmt"
	"io"
	"iter"
	"log"
	"path/filepath"
	"strings"

	"github.com/samber/lo"
)

// CREATE TABLE openalex.topics (
//     id text NOT NULL,
//     display_name text,
//     subfield_id text,
//     subfield_display_name text,
//     field_id text,
//     field_display_name text,
//     domain_id text,
//     domain_display_name text,
//     description text,
//     keywords text,
//     works_api_url text,
//     wikipedia_id text,
//     works_count integer,
//     cited_by_count integer,
//     updated_date timestamp without time zone,
//     siblings json
// );

type topicsRow struct {
	Id                  *string      `csv:"id" sqltype:"TEXT"`
	DisplayName         *string      `csv:"display_name" sqltype:"TEXT"`
	SubfieldId          *string      `csv:"subfield_id" sqltype:"TEXT"`
	SubfieldDisplayName *string      `csv:"subfield_display_name" sqltype:"TEXT"`
	FieldId             *string      `csv:"field_id" sqltype:"TEXT"`
	FieldDisplayName    *string      `csv:"field_display_name" sqltype:"TEXT"`
	DomainId            *string      `csv:"domain_id" sqltype:"TEXT"`
	DomainDisplayName   *string      `csv:"domain_display_name" sqltype:"TEXT"`
	Description         *string      `csv:"description" sqltype:"TEXT"`
	Keywords            *string      `csv:"keywords" sqltype:"TEXT"`
	WorksApiUrl         *string      `csv:"works_api_url" sqltype:"TEXT"`
	WikipediaId         *string      `csv:"wikipedia_id" sqltype:"TEXT"`
	WorksCount          *json.Number `csv:"works_count" sqltype:"INTEGER"`
	CitedByCount        *json.Number `csv:"cited_by_count" sqltype:"INTEGER"`
	UpdatedDate         *string      `csv:"updated_date" sqltype:"TIMESTAMP"`
	Siblings            jsontype     `csv:"siblings" sqltype:"JSON"`
}

func getIdAndDisplayName(key string, data map[string]any) (*string, *string) {
	if section := getCast[map[string]any](data, key); section != nil {
		return getCast[string](*section, "id"), getCast[string](*section, "display_name")
	}
	return nil, nil
}

func convertTopics(gzipPaths iter.Seq[string], outputPath string, chunk int) {
	topicsWriter, err := OpenCsvEncoder(filepath.Join(outputPath, "topics", fmt.Sprint("topics", chunk, ".csv.gz")), topicsRow{})
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
	Name:    "topics",
	Convert: convertTopics,
	WriteSqlImport: func(w io.Writer, outputPath string, numChunks int) {
		basePath := filepath.Join(outputPath, "topics")

		writeDuckdbCopy(w, topicsRow{}, "topics", basePath, numChunks)
	},
}
