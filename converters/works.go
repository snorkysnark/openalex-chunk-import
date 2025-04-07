package converters

import (
	"encoding/json"
	"fmt"
	"io"
	"iter"
	"log"
	"path/filepath"
)

// CREATE TABLE openalex.works (
//     id text NOT NULL,
//     doi text,
//     title text,
//     display_name text,
//     publication_year integer,
//     publication_date text,
//     type text,
//     cited_by_count integer,
//     is_retracted boolean,
//     is_paratext boolean,
//     cited_by_api_url text,
//     abstract_inverted_index json,
//     language text
// );

type worksRow struct {
	Id                    *string      `csv:"id" sqltype:"TEXT"`
	Doi                   *string      `csv:"doi" sqltype:"TEXT"`
	Title                 *string      `csv:"title" sqltype:"TEXT"`
	DisplayName           *string      `csv:"display_name" sqltype:"TEXT"`
	PublicationYear       *json.Number `csv:"publication_year" sqltype:"INTEGER"`
	PublicationDate       *string      `csv:"publication_date" sqltype:"TEXT"`
	Type                  *string      `csv:"type" sqltype:"TEXT"`
	CitedByCount          *json.Number `csv:"cited_by_count" sqltype:"INTEGER"`
	IsRetraction          *bool        `csv:"is_retracted" sqltype:"BOOLEAN"`
	IsParatext            *bool        `csv:"is_paratext" sqltype:"BOOLEAN"`
	CitedByApiUrl         *string      `csv:"cited_by_api_url" sqltype:"TEXT"`
	AbstractInvertedIndex jsontype     `csv:"abstract_inverted_index" sqltype:"JSON"`
	Language              *string      `csv:"language" sqltype:"TEXT"`
}

// CREATE TABLE openalex.works_primary_locations (
//     work_id text,
//     source_id text,
//     landing_page_url text,
//     pdf_url text,
//     is_oa boolean,
//     version text,
//     license text
// );

type worksPrimaryLocationsRow struct {
	WorkId         *string `csv:"work_id" sqltype:"TEXT"`
	SourceId       *string `csv:"source_id" sqltype:"TEXT"`
	LandingPageUrl *string `csv:"landing_page_url" sqltype:"TEXT"`
	PdfUrl         *string `csv:"pdf_url" sqltype:"TEXT"`
	IsOa           *bool   `csv:"is_oa" sqltype:"BOOLEAN"`
	Version        *string `csv:"version" sqltype:"TEXT"`
	License        *string `csv:"license" sqltype:"TEXT"`
}

// CREATE TABLE openalex.works_locations (
//     work_id text,
//     source_id text,
//     landing_page_url text,
//     pdf_url text,
//     is_oa boolean,
//     version text,
//     license text
// );

type worksLocationsRow struct {
	WorkId         *string `csv:"work_id" sqltype:"TEXT"`
	SourceId       *string `csv:"source_id" sqltype:"TEXT"`
	LandingPageUrl *string `csv:"landing_page_url" sqltype:"TEXT"`
	PdfUrl         *string `csv:"pdf_url" sqltype:"TEXT"`
	IsOa           *bool   `csv:"is_oa" sqltype:"BOOLEAN"`
	Version        *string `csv:"version" sqltype:"TEXT"`
	License        *string `csv:"license" sqltype:"TEXT"`
}

// CREATE TABLE openalex.works_best_oa_locations (
//     work_id text,
//     source_id text,
//     landing_page_url text,
//     pdf_url text,
//     is_oa boolean,
//     version text,
//     license text
// );

type worksBestOaLocationsRow struct {
	WorkId         *string `csv:"work_id" sqltype:"TEXT"`
	SourceId       *string `csv:"source_id" sqltype:"TEXT"`
	LandingPageUrl *string `csv:"landing_page_url" sqltype:"TEXT"`
	PdfUrl         *string `csv:"pdf_url" sqltype:"TEXT"`
	IsOa           *bool   `csv:"is_oa" sqltype:"BOOLEAN"`
	Version        *string `csv:"version" sqltype:"TEXT"`
	License        *string `csv:"license" sqltype:"TEXT"`
}

// CREATE TABLE openalex.works_authorships (
//     work_id text,
//     author_position text,
//     author_id text,
//     institution_id text,
//     raw_affiliation_string text
// );

type worksAuthorshipsRow struct {
	WorkId               *string `csv:"work_id" sqltype:"TEXT"`
	AuthorPosition       *string `csv:"author_position" sqltype:"TEXT"`
	AuthorId             *string `csv:"author_id" sqltype:"TEXT"`
	InstitutionId        *string `csv:"institution_id" sqltype:"TEXT"`
	RawAffiliationString *string `csv:"raw_affiliation_string" sqltype:"TEXT"`
}

// CREATE TABLE openalex.works_biblio (
//     work_id text NOT NULL,
//     volume text,
//     issue text,
//     first_page text,
//     last_page text
// );

type worksBiblioRow struct {
	WorkId    *string `csv:"work_id" sqltype:"TEXT"`
	Volume    *string `csv:"volume" sqltype:"TEXT"`
	Issue     *string `csv:"issue" sqltype:"TEXT"`
	FirstPage *string `csv:"first_page" sqltype:"TEXT"`
	LastPage  *string `csv:"last_page" sqltype:"TEXT"`
}

// CREATE TABLE openalex.works_topics (
//     work_id text,
//     topic_id text,
//     score real
// );

type worksTopicsRow struct {
	WorkId  *string      `csv:"work_id" sqltype:"TEXT"`
	TopicId *string      `csv:"topic_id" sqltype:"TEXT"`
	Score   *json.Number `csv:"score" sqltype:"REAL"`
}

// CREATE TABLE openalex.works_concepts (
//     work_id text,
//     concept_id text,
//     score real
// );

type worksConceptsRow struct {
	WorkId    *string      `csv:"work_id" sqltype:"TEXT"`
	ConceptId *string      `csv:"concept_id" sqltype:"TEXT"`
	Score     *json.Number `csv:"score" sqltype:"REAL"`
}

// CREATE TABLE openalex.works_ids (
//     work_id text NOT NULL,
//     openalex text,
//     doi text,
//     mag bigint,
//     pmid text,
//     pmcid text
// );

type worksIdsRow struct {
	WorkId   *string      `csv:"work_id" sqltype:"TEXT"`
	Openalex *string      `csv:"openalex" sqltype:"TEXT"`
	Doi      *string      `csv:"doi" sqltype:"TEXT"`
	Mag      *json.Number `csv:"mag" sqltype:"BIGINT"`
	Pmid     *string      `csv:"pmid" sqltype:"TEXT"`
	Pmcid    *string      `csv:"pmcid" sqltype:"TEXT"`
}

// CREATE TABLE openalex.works_mesh (
//     work_id text,
//     descriptor_ui text,
//     descriptor_name text,
//     qualifier_ui text,
//     qualifier_name text,
//     is_major_topic boolean
// );

type worksMeshRow struct {
	WorkId         *string `csv:"work_id" sqltype:"TEXT"`
	DescriptorUi   *string `csv:"descriptor_ui" sqltype:"TEXT"`
	DescriptorName *string `csv:"descriptor_name" sqltype:"TEXT"`
	QualifierUi    *string `csv:"qualifier_ui" sqltype:"TEXT"`
	QualifierName  *string `csv:"qualifier_name" sqltype:"TEXT"`
	IsMajorTopic   *bool   `csv:"is_major_topic" sqltype:"BOOLEAN"`
}

// CREATE TABLE openalex.works_open_access (
//     work_id text NOT NULL,
//     is_oa boolean,
//     oa_status text,
//     oa_url text,
//     any_repository_has_fulltext boolean
// );

type worksOpenAccessRow struct {
	WorkId                   *string `csv:"work_id" sqltype:"TEXT"`
	IsOa                     *bool   `csv:"is_oa" sqltype:"BOOLEAN"`
	OaStatus                 *string `csv:"oa_status" sqltype:"TEXT"`
	OaUrl                    *string `csv:"oa_url" sqltype:"TEXT"`
	AnyRepositoryHasFulltext *bool   `csv:"any_repository_has_fulltext" sqltype:"BOOLEAN"`
}

// CREATE TABLE openalex.works_referenced_works (
//     work_id text,
//     referenced_work_id text
// );

type worksReferencedWorksRow struct {
	WorkId           *string `csv:"work_id" sqltype:"TEXT"`
	ReferencedWorkId *string `csv:"referenced_work_id" sqltype:"TEXT"`
}

// CREATE TABLE openalex.works_related_works (
//     work_id text,
//     related_work_id text
// );

type worksRelatedWorksRow struct {
	WorkId        *string `csv:"work_id" sqltype:"TEXT"`
	RelatedWorkId *string `csv:"related_work_id" sqltype:"TEXT"`
}

func convertWorks(gzipPaths iter.Seq[string], outputPath string, chunk int) {
	worksWriter, err := OpenCsvEncoder(filepath.Join(outputPath, "works", fmt.Sprint("works", chunk, ".csv.gz")), worksRow{})
	if err != nil {
		log.Println(err)
		return
	}
	defer worksWriter.Close()
	worksPrimaryLocationsWriter, err := OpenCsvEncoder(filepath.Join(outputPath, "works", fmt.Sprint("works_primary_locations", chunk, ".csv.gz")), worksPrimaryLocationsRow{})
	if err != nil {
		log.Println(err)
		return
	}
	defer worksPrimaryLocationsWriter.Close()
	worksLocationsWriter, err := OpenCsvEncoder(filepath.Join(outputPath, "works", fmt.Sprint("works_locations", chunk, ".csv.gz")), worksLocationsRow{})
	if err != nil {
		log.Println(err)
		return
	}
	defer worksLocationsWriter.Close()
	worksBestOaLocationsWriter, err := OpenCsvEncoder(filepath.Join(outputPath, "works", fmt.Sprint("works_best_oa_locations", chunk, ".csv.gz")), worksBestOaLocationsRow{})
	if err != nil {
		log.Println(err)
		return
	}
	defer worksBestOaLocationsWriter.Close()
	worksAuthorshipsWriter, err := OpenCsvEncoder(filepath.Join(outputPath, "works", fmt.Sprint("works_authorships", chunk, ".csv.gz")), worksAuthorshipsRow{})
	if err != nil {
		log.Println(err)
		return
	}
	defer worksAuthorshipsWriter.Close()
	worksBiblioWriter, err := OpenCsvEncoder(filepath.Join(outputPath, "works", fmt.Sprint("works_biblio", chunk, ".csv.gz")), worksBiblioRow{})
	if err != nil {
		log.Println(err)
		return
	}
	defer worksBiblioWriter.Close()
	worksTopicsWriter, err := OpenCsvEncoder(filepath.Join(outputPath, "works", fmt.Sprint("works_topics", chunk, ".csv.gz")), worksTopicsRow{})
	if err != nil {
		log.Println(err)
		return
	}
	defer worksTopicsWriter.Close()
	worksConceptsWriter, err := OpenCsvEncoder(filepath.Join(outputPath, "works", fmt.Sprint("works_concepts", chunk, ".csv.gz")), worksConceptsRow{})
	if err != nil {
		log.Println(err)
		return
	}
	defer worksConceptsWriter.Close()
	worksIdsWriter, err := OpenCsvEncoder(filepath.Join(outputPath, "works", fmt.Sprint("works_ids", chunk, ".csv.gz")), worksIdsRow{})
	if err != nil {
		log.Println(err)
		return
	}
	defer worksIdsWriter.Close()
	worksMeshWriter, err := OpenCsvEncoder(filepath.Join(outputPath, "works", fmt.Sprint("works_mesh", chunk, ".csv.gz")), worksMeshRow{})
	if err != nil {
		log.Println(err)
		return
	}
	defer worksMeshWriter.Close()
	worksOpenAccessWriter, err := OpenCsvEncoder(filepath.Join(outputPath, "works", fmt.Sprint("works_open_access", chunk, ".csv.gz")), worksOpenAccessRow{})
	if err != nil {
		log.Println(err)
		return
	}
	defer worksOpenAccessWriter.Close()
	worksReferencedWorksWriter, err := OpenCsvEncoder(filepath.Join(outputPath, "works", fmt.Sprint("works_referenced_works", chunk, ".csv.gz")), worksReferencedWorksRow{})
	if err != nil {
		log.Println(err)
		return
	}
	defer worksReferencedWorksWriter.Close()
	worksRelatedWorksWriter, err := OpenCsvEncoder(filepath.Join(outputPath, "works", fmt.Sprint("works_related_works", chunk, ".csv.gz")), worksRelatedWorksRow{})
	if err != nil {
		log.Println(err)
		return
	}
	defer worksRelatedWorksWriter.Close()

	for data, err := range ReadJsonLinesAll(gzipPaths) {
		if err != nil {
			log.Println(err)
			continue
		}

		workId := getCast[string](data, "id")
		if workId == nil {
			continue
		}

		if err := worksWriter.Encode(worksRow{
			Id:                    workId,
			Doi:                   getCast[string](data, "doi"),
			Title:                 getCast[string](data, "title"),
			DisplayName:           getCast[string](data, "display_name"),
			PublicationYear:       getCast[json.Number](data, "publication_year"),
			PublicationDate:       getCast[string](data, "publication_date"),
			Type:                  getCast[string](data, "type"),
			CitedByCount:          getCast[json.Number](data, "cited_by_count"),
			IsRetraction:          getCast[bool](data, "is_retracted"),
			IsParatext:            getCast[bool](data, "is_paratext"),
			CitedByApiUrl:         getCast[string](data, "cited_by_api_url"),
			AbstractInvertedIndex: jsontype{data["abstract_inverted_index"]},
			Language:              getCast[string](data, "language"),
		}); err != nil {
			log.Println(err)
		}

		if primaryLocation := getCast[map[string]any](data, "primary_location"); primaryLocation != nil {
			if sourceId := getCastAt[string](*primaryLocation, []string{"source", "id"}); sourceId != nil {
				if err := worksPrimaryLocationsWriter.Encode(worksPrimaryLocationsRow{
					WorkId:         workId,
					SourceId:       sourceId,
					LandingPageUrl: getCast[string](*primaryLocation, "landing_page_url"),
					PdfUrl:         getCast[string](*primaryLocation, "pdf_url"),
					IsOa:           getCast[bool](*primaryLocation, "is_oa"),
					Version:        getCast[string](*primaryLocation, "version"),
					License:        getCast[string](*primaryLocation, "license"),
				}); err != nil {
					log.Println(err)
				}
			}
		}

		if locations := getCast[[]any](data, "locations"); locations != nil {
			for location := range iterCast[map[string]any](*locations) {
				if sourceId := getCastAt[string](*location, []string{"source", "id"}); sourceId != nil {
					if err := worksLocationsWriter.Encode(worksLocationsRow{
						WorkId:         workId,
						SourceId:       sourceId,
						LandingPageUrl: getCast[string](*location, "landing_page_url"),
						PdfUrl:         getCast[string](*location, "pdf_url"),
						IsOa:           getCast[bool](*location, "is_oa"),
						Version:        getCast[string](*location, "version"),
						License:        getCast[string](*location, "license"),
					}); err != nil {
						log.Println(err)
					}
				}
			}
		}

		if bestOaLocation := getCast[map[string]any](data, "best_oa_location"); bestOaLocation != nil {
			if sourceId := getCastAt[string](*bestOaLocation, []string{"source", "id"}); sourceId != nil {
				if err := worksBestOaLocationsWriter.Encode(worksBestOaLocationsRow{
					WorkId:         workId,
					SourceId:       sourceId,
					LandingPageUrl: getCast[string](*bestOaLocation, "landing_page_url"),
					PdfUrl:         getCast[string](*bestOaLocation, "pdf_url"),
					IsOa:           getCast[bool](*bestOaLocation, "is_oa"),
					Version:        getCast[string](*bestOaLocation, "version"),
					License:        getCast[string](*bestOaLocation, "license"),
				}); err != nil {
					log.Println(err)
				}
			}
		}

		if authorships := getCast[[]any](data, "authorships"); authorships != nil {
			for authorship := range iterCast[map[string]any](*authorships) {
				if authorId := getCastAt[string](*authorship, []string{"author", "id"}); authorId != nil {
					institutions := getCast[[]any](*authorship, "institutions")

					institutionIds := []*string{}
					if institutions != nil {
						for institution := range iterCast[map[string]any](*institutions) {
							if institutionId := getCast[string](*institution, "id"); institutionId != nil {
								institutionIds = append(institutionIds, institutionId)
							}
						}
					}

					if len(institutionIds) == 0 {
						institutionIds = append(institutionIds, nil)
					}

					for _, institutionId := range institutionIds {
						if err := worksAuthorshipsWriter.Encode(worksAuthorshipsRow{
							WorkId:               workId,
							AuthorPosition:       getCast[string](*authorship, "author_position"),
							AuthorId:             authorId,
							InstitutionId:        institutionId,
							RawAffiliationString: getCast[string](*authorship, "raw_affiliation_string"),
						}); err != nil {
							log.Println(err)
						}
					}
				}
			}
		}

		if biblio := getCast[map[string]any](data, "biblio"); biblio != nil {
			if err := worksBiblioWriter.Encode(worksBiblioRow{
				WorkId:    workId,
				Volume:    getCast[string](*biblio, "volume"),
				Issue:     getCast[string](*biblio, "issue"),
				FirstPage: getCast[string](*biblio, "first_page"),
				LastPage:  getCast[string](*biblio, "last_page"),
			}); err != nil {
				log.Println(err)
			}
		}

		if topics := getCast[[]any](data, "topics"); topics != nil {
			for topic := range iterCast[map[string]any](*topics) {
				if topicId := getCast[string](*topic, "id"); topicId != nil {
					if err := worksTopicsWriter.Encode(worksTopicsRow{
						WorkId:  workId,
						TopicId: topicId,
						Score:   getCast[json.Number](*topic, "score"),
					}); err != nil {
						log.Println(err)
					}
				}
			}
		}

		if concepts := getCast[[]any](data, "concepts"); concepts != nil {
			for concept := range iterCast[map[string]any](*concepts) {
				if err := worksConceptsWriter.Encode(worksConceptsRow{
					WorkId:    workId,
					ConceptId: getCast[string](*concept, "id"),
					Score:     getCast[json.Number](*concept, "score"),
				}); err != nil {
					log.Println(err)
				}
			}
		}

		if ids := getCast[map[string]any](data, "ids"); ids != nil {
			if err := worksIdsWriter.Encode(worksIdsRow{
				WorkId:   workId,
				Openalex: getCast[string](*ids, "openalex"),
				Doi:      getCast[string](*ids, "doi"),
				Mag:      getCast[json.Number](*ids, "mag"),
				Pmid:     getCast[string](*ids, "pmid"),
				Pmcid:    getCast[string](*ids, "pmcid"),
			}); err != nil {
				log.Println(err)
			}
		}

		if meshes := getCast[[]any](data, "mesh"); meshes != nil {
			for mesh := range iterCast[map[string]any](*meshes) {
				if err := worksMeshWriter.Encode(worksMeshRow{
					WorkId:         workId,
					DescriptorUi:   getCast[string](*mesh, "descriptor_ui"),
					DescriptorName: getCast[string](*mesh, "descriptor_name"),
					QualifierUi:    getCast[string](*mesh, "qualifier_ui"),
					QualifierName:  getCast[string](*mesh, "qualifier_name"),
					IsMajorTopic:   getCast[bool](*mesh, "is_major_topic"),
				}); err != nil {
					log.Println(err)
				}
			}
		}

		if openAccess := getCast[map[string]any](data, "open_access"); openAccess != nil {
			if err := worksOpenAccessWriter.Encode(worksOpenAccessRow{
				WorkId:                   workId,
				IsOa:                     getCast[bool](*openAccess, "is_oa"),
				OaStatus:                 getCast[string](*openAccess, "oa_status"),
				OaUrl:                    getCast[string](*openAccess, "oa_url"),
				AnyRepositoryHasFulltext: getCast[bool](*openAccess, "any_repository_has_fulltext"),
			}); err != nil {
				log.Println(err)
			}
		}

		if referencedWorks := getCast[[]any](data, "referenced_works"); referencedWorks != nil {
			for referencedWork := range iterCast[string](*referencedWorks) {
				if err := worksReferencedWorksWriter.Encode(worksReferencedWorksRow{
					WorkId:           workId,
					ReferencedWorkId: referencedWork,
				}); err != nil {
					log.Println(err)
				}
			}
		}

		if relatedWorks := getCast[[]any](data, "related_works"); relatedWorks != nil {
			for relatedWork := range iterCast[string](*relatedWorks) {
				if err := worksRelatedWorksWriter.Encode(worksRelatedWorksRow{
					WorkId:        workId,
					RelatedWorkId: relatedWork,
				}); err != nil {
					log.Println(err)
				}
			}
		}
	}
}

var TypeWorks = EntityType{
	Name:    "works",
	Convert: convertWorks,
	WriteSqlImport: func(w io.Writer, outputPath string, numChunks int) {
		basePath := filepath.Join(outputPath, "works")

		writeDuckdbCopy(w, worksRow{}, "works", basePath, numChunks)
		writeDuckdbCopy(w, worksPrimaryLocationsRow{}, "works_primary_locations", basePath, numChunks)
		writeDuckdbCopy(w, worksLocationsRow{}, "works_locations", basePath, numChunks)
		writeDuckdbCopy(w, worksBestOaLocationsRow{}, "works_best_oa_locations", basePath, numChunks)
		writeDuckdbCopy(w, worksAuthorshipsRow{}, "works_authorships", basePath, numChunks)
		writeDuckdbCopy(w, worksBiblioRow{}, "works_biblio", basePath, numChunks)
		writeDuckdbCopy(w, worksTopicsRow{}, "works_topics", basePath, numChunks)
		writeDuckdbCopy(w, worksConceptsRow{}, "works_concepts", basePath, numChunks)
		writeDuckdbCopy(w, worksIdsRow{}, "works_ids", basePath, numChunks)
		writeDuckdbCopy(w, worksMeshRow{}, "works_mesh", basePath, numChunks)
		writeDuckdbCopy(w, worksOpenAccessRow{}, "works_open_access", basePath, numChunks)
	},
}
