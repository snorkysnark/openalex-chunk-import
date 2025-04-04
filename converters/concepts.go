package converters

import (
	"encoding/json"
	"fmt"
	"io"
	"iter"
	"log"
	"path/filepath"
)

type conceptsRow struct {
	Id                *string      `csv:"id"`
	Wikidata          *string      `csv:"wikidata"`
	DisplayName       *string      `csv:"display_name"`
	Level             *json.Number `csv:"level"`
	Description       *string      `csv:"description"`
	WorksCount        *json.Number `csv:"works_count"`
	CitedByCount      *json.Number `csv:"cited_by_count"`
	ImageUrl          *string      `csv:"image_url"`
	ImageThumbnailUrl *string      `csv:"image_thumbnail_url"`
	WorksApiUrl       *string      `csv:"works_api_url"`
	UpdatedDate       *string      `csv:"updated_date"`
}

type conceptsAncestorsRow struct {
	ConceptId  *string `csv:"concept_id"`
	AncestorId *string `csv:"ancestor_id"`
}

type conceptsCountsByYearRow struct {
	ConceptId    *string      `csv:"concept_id"`
	Year         *json.Number `csv:"year"`
	WorksCount   *json.Number `csv:"works_count"`
	CitedByCount *json.Number `csv:"cited_by_count"`
	OaWorksCount *json.Number `csv:"oa_works_count"`
}

type conceptsIdsRow struct {
	ConceptId *string      `csv:"concept_id"`
	Openalex  *string      `csv:"openalex"`
	Wikidata  *string      `csv:"wikidata"`
	Wikipedia *string      `csv:"wikipedia"`
	UmlsAui   jsontype     `csv:"umls_aui"`
	UmlsCui   jsontype     `csv:"umls_cui"`
	Mag       *json.Number `csv:"mag"`
}

type conceptsRelatedConceptsRow struct {
	ConceptId        *string      `csv:"concept_id"`
	RelatedConceptId *string      `csv:"related_concept_id"`
	Score            *json.Number `csv:"score"`
}

func convertConcepts(gzipPaths iter.Seq[string], outputPath string, chunk int) {
	conceptsWriter, err := OpenCsvEncoder(filepath.Join(outputPath, "concepts", fmt.Sprint("concepts", chunk, ".csv.gz")), conceptsRow{})
	if err != nil {
		log.Println(err)
		return
	}
	defer conceptsWriter.Close()
	conceptsAncestorsWriter, err := OpenCsvEncoder(filepath.Join(outputPath, "concepts", fmt.Sprint("concepts_ancestors", chunk, ".csv.gz")), conceptsAncestorsRow{})
	if err != nil {
		log.Println(err)
		return
	}
	defer conceptsAncestorsWriter.Close()
	conceptsCountsWriter, err := OpenCsvEncoder(filepath.Join(outputPath, "concepts", fmt.Sprint("concepts_counts_by_year", chunk, ".csv.gz")), conceptsCountsByYearRow{})
	if err != nil {
		log.Println(err)
		return
	}
	defer conceptsCountsWriter.Close()
	conceptsIdsWriter, err := OpenCsvEncoder(filepath.Join(outputPath, "concepts", fmt.Sprint("concepts_ids", chunk, ".csv.gz")), conceptsIdsRow{})
	if err != nil {
		log.Println(err)
		return
	}
	defer conceptsIdsWriter.Close()
	conceptsRelatedConceptsWriter, err := OpenCsvEncoder(filepath.Join(outputPath, "concepts", fmt.Sprint("concepts_related_concepts", chunk, ".csv.gz")), conceptsRelatedConceptsRow{})
	if err != nil {
		log.Println(err)
		return
	}
	defer conceptsRelatedConceptsWriter.Close()

	for data, err := range ReadJsonLinesAll(gzipPaths) {
		if err != nil {
			log.Println(err)
			continue
		}

		conceptId := getCast[string](data, "id")
		if conceptId == nil {
			continue
		}

		if err := conceptsWriter.Encode(conceptsRow{
			Id:                conceptId,
			Wikidata:          getCast[string](data, "wikidata"),
			DisplayName:       getCast[string](data, "display_name"),
			Level:             getCast[json.Number](data, "level"),
			Description:       getCast[string](data, "description"),
			WorksCount:        getCast[json.Number](data, "works_count"),
			CitedByCount:      getCast[json.Number](data, "cited_by_count"),
			ImageUrl:          getCast[string](data, "image_url"),
			ImageThumbnailUrl: getCast[string](data, "image_thumbnail_url"),
			WorksApiUrl:       getCast[string](data, "works_api_url"),
			UpdatedDate:       getCast[string](data, "updated_date"),
		}); err != nil {
			log.Println(err)
		}

		if ids := getCast[map[string]any](data, "ids"); ids != nil {
			if err := conceptsIdsWriter.Encode(conceptsIdsRow{
				ConceptId: conceptId,
				Openalex:  getCast[string](*ids, "openalex"),
				Wikidata:  getCast[string](*ids, "wikidata"),
				Wikipedia: getCast[string](*ids, "wikipedia"),
				UmlsAui:   jsontype{value: (*ids)["umls_aui"]},
				UmlsCui:   jsontype{value: (*ids)["umls_cui"]},
				Mag:       getCast[json.Number](*ids, "mag"),
			}); err != nil {
				log.Println(err)
			}
		}

		if ancestors := getCast[[]any](data, "ancestors"); ancestors != nil {
			for ancestor := range iterCast[map[string]any](*ancestors) {
				if ancestorId := getCast[string](*ancestor, "id"); ancestorId != nil {
					if err := conceptsAncestorsWriter.Encode(conceptsAncestorsRow{
						ConceptId:  conceptId,
						AncestorId: ancestorId,
					}); err != nil {
						log.Println(err)
					}
				}
			}
		}

		if countsByYear := getCast[[]any](data, "counts_by_year"); countsByYear != nil {
			for countByYear := range iterCast[map[string]any](*countsByYear) {
				if err := conceptsCountsWriter.Encode(conceptsCountsByYearRow{
					ConceptId:    conceptId,
					Year:         getCast[json.Number](*countByYear, "year"),
					WorksCount:   getCast[json.Number](*countByYear, "works_count"),
					CitedByCount: getCast[json.Number](*countByYear, "cited_by_count"),
					OaWorksCount: getCast[json.Number](*countByYear, "oa_works_count"),
				}); err != nil {
					log.Println(err)
				}
			}
		}

		if relatedConcepts := getCast[[]any](data, "related_concepts"); relatedConcepts != nil {
			for relatedConcept := range iterCast[map[string]any](*relatedConcepts) {
				if relatedConceptId := getCast[string](*relatedConcept, "id"); relatedConceptId != nil {
					if err := conceptsRelatedConceptsWriter.Encode(conceptsRelatedConceptsRow{
						ConceptId:        conceptId,
						RelatedConceptId: relatedConceptId,
						Score:            getCast[json.Number](*relatedConcept, "score"),
					}); err != nil {
						log.Println(err)
					}
				}
			}
		}
	}
}

var TypeConcepts = EntityType{
	Name:    "concepts",
	Convert: convertConcepts,
	WriteSqlImport: func(w io.Writer, outputPath string, numChunks int) {
		basePath := filepath.Join(outputPath, "concepts")

		writeDuckdbCopy(w, conceptsRow{}, "concepts", basePath, numChunks)
		writeDuckdbCopy(w, conceptsAncestorsRow{}, "concepts_ancestors", basePath, numChunks)
		writeDuckdbCopy(w, conceptsCountsByYearRow{}, "concepts_counts_by_year", basePath, numChunks)
		writeDuckdbCopy(w, conceptsIdsRow{}, "concepts_ids", basePath, numChunks)
		writeDuckdbCopy(w, conceptsRelatedConceptsRow{}, "concepts_related_concepts", basePath, numChunks)
	},
}
