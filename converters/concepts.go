package converters

import (
	"encoding/json"
	"fmt"
	"io"
	"iter"
	"log"
	"path/filepath"
)

// CREATE TABLE openalex.concepts (
//     id text NOT NULL,
//     wikidata text,
//     display_name text,
//     level integer,
//     description text,
//     works_count integer,
//     cited_by_count integer,
//     image_url text,
//     image_thumbnail_url text,
//     works_api_url text,
//     updated_date timestamp without time zone
// );

type conceptsRow struct {
	Id                *string      `csv:"id" sqltype:"TEXT"`
	Wikidata          *string      `csv:"wikidata" sqltype:"TEXT"`
	DisplayName       *string      `csv:"display_name" sqltype:"TEXT"`
	Level             *json.Number `csv:"level" sqltype:"INTEGER"`
	Description       *string      `csv:"description" sqltype:"TEXT"`
	WorksCount        *json.Number `csv:"works_count" sqltype:"INTEGER"`
	CitedByCount      *json.Number `csv:"cited_by_count" sqltype:"INTEGER"`
	ImageUrl          *string      `csv:"image_url" sqltype:"TEXT"`
	ImageThumbnailUrl *string      `csv:"image_thumbnail_url" sqltype:"TEXT"`
	WorksApiUrl       *string      `csv:"works_api_url" sqltype:"TEXT"`
	UpdatedDate       *string      `csv:"updated_date" sqltype:"TIMESTAMP"`
}

// CREATE TABLE openalex.concepts_ancestors (
//     concept_id text,
//     ancestor_id text
// );

type conceptsAncestorsRow struct {
	ConceptId  *string `csv:"concept_id" sqltype:"TEXT"`
	AncestorId *string `csv:"ancestor_id" sqltype:"TEXT"`
}

// CREATE TABLE openalex.concepts_counts_by_year (
//     concept_id text NOT NULL,
//     year integer NOT NULL,
//     works_count integer,
//     cited_by_count integer,
//     oa_works_count integer
// );

type conceptsCountsByYearRow struct {
	ConceptId    *string      `csv:"concept_id" sqltype:"TEXT"`
	Year         *json.Number `csv:"year" sqltype:"INTEGER"`
	WorksCount   *json.Number `csv:"works_count" sqltype:"INTEGER"`
	CitedByCount *json.Number `csv:"cited_by_count" sqltype:"INTEGER"`
	OaWorksCount *json.Number `csv:"oa_works_count" sqltype:"INTEGER"`
}

// CREATE TABLE openalex.concepts_ids (
//     concept_id text NOT NULL,
//     openalex text,
//     wikidata text,
//     wikipedia text,
//     umls_aui json,
//     umls_cui json,
//     mag bigint
// );

type conceptsIdsRow struct {
	ConceptId *string      `csv:"concept_id" sqltype:"TEXT"`
	Openalex  *string      `csv:"openalex" sqltype:"TEXT"`
	Wikidata  *string      `csv:"wikidata" sqltype:"TEXT"`
	Wikipedia *string      `csv:"wikipedia" sqltype:"TEXT"`
	UmlsAui   jsontype     `csv:"umls_aui" sqltype:"JSON"`
	UmlsCui   jsontype     `csv:"umls_cui" sqltype:"JSON"`
	Mag       *json.Number `csv:"mag" sqltype:"BIGINT"`
}

// CREATE TABLE openalex.concepts_related_concepts (
//     concept_id text,
//     related_concept_id text,
//     score real
// );

type conceptsRelatedConceptsRow struct {
	ConceptId        *string      `csv:"concept_id" sqltype:"TEXT"`
	RelatedConceptId *string      `csv:"related_concept_id" sqltype:"TEXT"`
	Score            *json.Number `csv:"score" sqltype:"REAL"`
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
