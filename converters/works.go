package converters

import (
	"encoding/json"
	"fmt"
	"iter"
	"log"
	"path/filepath"
)

type worksRow struct {
	Id                    *string      `csv:"id"`
	Doi                   *string      `csv:"doi"`
	Title                 *string      `csv:"title"`
	DisplayName           *string      `csv:"display_name"`
	PublicationYear       *json.Number `csv:"publication_year"`
	PublicationDate       *string      `csv:"publication_date"`
	Type                  *string      `csv:"type"`
	CitedByCount          *json.Number `csv:"cited_by_count"`
	IsRetraction          *bool        `csv:"is_retracted"`
	IsParatext            *bool        `csv:"is_paratext"`
	CitedByApiUrl         *string      `csv:"cited_by_api_url"`
	AbstractInvertedIndex jsontype     `csv:"abstract_inverted_index"`
	Language              *string      `csv:"language"`
}

type worksPrimaryLocationsRow struct {
	WorkId         *string `csv:"work_id"`
	SourceId       *string `csv:"source_id"`
	LandingPageUrl *string `csv:"landing_page_url"`
	PdfUrl         *string `csv:"pdf_url"`
	IsOa           *bool   `csv:"is_oa"`
	Version        *string `csv:"version"`
	License        *string `csv:"license"`
}

type worksLocationsRow struct {
	WorkId         *string `csv:"work_id"`
	SourceId       *string `csv:"source_id"`
	LandingPageUrl *string `csv:"landing_page_url"`
	PdfUrl         *string `csv:"pdf_url"`
	IsOa           *bool   `csv:"is_oa"`
	Version        *string `csv:"version"`
	License        *string `csv:"license"`
}

type worksBestOaLocationsRow struct {
	WorkId         *string `csv:"work_id"`
	SourceId       *string `csv:"source_id"`
	LandingPageUrl *string `csv:"landing_page_url"`
	PdfUrl         *string `csv:"pdf_url"`
	IsOa           *bool   `csv:"is_oa"`
	Version        *string `csv:"version"`
	License        *string `csv:"license"`
}

type worksAuthorshipsRow struct {
	WorkId               *string `csv:"work_id"`
	AuthorPosition       *string `csv:"author_position"`
	AuthorId             *string `csv:"author_id"`
	InstitutionId        *string `csv:"institution_id"`
	RawAffiliationString *string `csv:"raw_affiliation_string"`
}

type worksBiblioRow struct {
	WorkId    *string `csv:"work_id"`
	Volume    *string `csv:"volume"`
	Issue     *string `csv:"issue"`
	FirstPage *string `csv:"first_page"`
	LastPage  *string `csv:"last_page"`
}

type worksTopicsRow struct {
	WorkId  *string      `csv:"work_id"`
	TopicId *string      `csv:"topic_id"`
	Score   *json.Number `csv:"score"`
}

type worksConceptsRow struct {
	WorkId    *string      `csv:"work_id"`
	ConceptId *string      `csv:"concept_id"`
	Score     *json.Number `csv:"score"`
}

type worksIdsRow struct {
	WorkId   *string      `csv:"work_id"`
	Openalex *string      `csv:"openalex"`
	Doi      *string      `csv:"doi"`
	Mag      *json.Number `csv:"mag"`
	Pmid     *string      `csv:"pmid"`
	Pmcid    *string      `csv:"pmcid"`
}

type worksMeshRow struct {
	WorkId         *string `csv:"work_id"`
	DescriptorUi   *string `csv:"descriptor_ui"`
	DescriptorName *string `csv:"descriptor_name"`
	QualifierUi    *string `csv:"qualifier_ui"`
	QualifierName  *string `csv:"qualifier_name"`
	IsMajorTopic   *bool   `csv:"is_major_topic"`
}

type worksOpenAccessRow struct {
	WorkId                   *string `csv:"work_id"`
	IsOa                     *bool   `csv:"is_oa"`
	OaStatus                 *string `csv:"oa_status"`
	OaUrl                    *string `csv:"oa_url"`
	AnyRepositoryHasFulltext *bool   `csv:"any_repository_has_fulltext"`
}

type worksReferencedWorksRow struct {
	WorkId           *string `csv:"work_id"`
	ReferencedWorkId *string `csv:"referenced_work_id"`
}

type worksRelatedWorksRow struct {
	WorkId        *string `csv:"work_id"`
	RelatedWorkId *string `csv:"related_work_id"`
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
	name:    "works",
	convert: convertWorks,
}
