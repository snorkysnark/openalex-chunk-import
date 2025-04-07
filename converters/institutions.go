package converters

import (
	"encoding/json"
	"fmt"
	"io"
	"iter"
	"log"
	"path/filepath"
)

// CREATE TABLE openalex.institutions (
//     id text NOT NULL,
//     ror text,
//     display_name text,
//     country_code text,
//     type text,
//     homepage_url text,
//     image_url text,
//     image_thumbnail_url text,
//     display_name_acronyms json,
//     display_name_alternatives json,
//     works_count integer,
//     cited_by_count integer,
//     works_api_url text,
//     updated_date timestamp without time zone
// );

type institutionsRow struct {
	Id                      *string      `csv:"id" sqltype:"TEXT"`
	Ror                     *string      `csv:"ror" sqltype:"TEXT"`
	DisplayName             *string      `csv:"display_name" sqltype:"TEXT"`
	CountryCode             *string      `csv:"country_code" sqltype:"TEXT"`
	Type                    *string      `csv:"type" sqltype:"TEXT"`
	HomepageUrl             *string      `csv:"homepage_url" sqltype:"TEXT"`
	ImageUrl                *string      `csv:"image_url" sqltype:"TEXT"`
	ImageThumbnailUrl       *string      `csv:"image_thumbnail_url" sqltype:"TEXT"`
	DisplayNameAcronyms     jsontype     `csv:"display_name_acronyms" sqltype:"JSON"`
	DisplayNameAlternatives jsontype     `csv:"display_name_alternatives" sqltype:"JSON"`
	WorksCount              *json.Number `csv:"works_count" sqltype:"INTEGER"`
	CitedByCount            *json.Number `csv:"cited_by_count" sqltype:"INTEGER"`
	WorksApiUrl             *string      `csv:"works_api_url" sqltype:"TEXT"`
	UpdatedDate             *string      `csv:"updated_date" sqltype:"TIMESTAMP"`
}

// CREATE TABLE openalex.institutions_associated_institutions (
//     institution_id text,
//     associated_institution_id text,
//     relationship text
// );

type institutionsAssociatedInstitutionsRow struct {
	InstitutionId           *string `csv:"institution_id" sqltype:"TEXT"`
	AssociatedInstitutionId *string `csv:"associated_institution_id" sqltype:"TEXT"`
	Relationship            *string `csv:"relationship" sqltype:"TEXT"`
}

// CREATE TABLE openalex.institutions_counts_by_year (
//     institution_id text NOT NULL,
//     year integer NOT NULL,
//     works_count integer,
//     cited_by_count integer,
//     oa_works_count integer
// );

type institutionsCountsByYearRow struct {
	InstitutionId *string      `csv:"institution_id" sqltype:"TEXT"`
	Year          *json.Number `csv:"year" sqltype:"INTEGER"`
	WorksCount    *json.Number `csv:"works_count" sqltype:"INTEGER"`
	CitedByCount  *json.Number `csv:"cited_by_count" sqltype:"INTEGER"`
	OaWorksCount  *json.Number `csv:"oa_works_count" sqltype:"INTEGER"`
}

// CREATE TABLE openalex.institutions_geo (
//     institution_id text NOT NULL,
//     city text,
//     geonames_city_id text,
//     region text,
//     country_code text,
//     country text,
//     latitude real,
//     longitude real
// );

type institutionsGeoRow struct {
	InstitutionId  *string      `csv:"institution_id" sqltype:"TEXT"`
	City           *string      `csv:"city" sqltype:"TEXT"`
	GeonamesCityId *string      `csv:"geonames_city_id" sqltype:"TEXT"`
	Region         *string      `csv:"region" sqltype:"TEXT"`
	CountryCode    *string      `csv:"country_code" sqltype:"TEXT"`
	Country        *string      `csv:"country" sqltype:"TEXT"`
	Latitude       *json.Number `csv:"latitude" sqltype:"REAL"`
	Longitude      *json.Number `csv:"longitude" sqltype:"REAL"`
}

// CREATE TABLE openalex.institutions_ids (
//     institution_id text NOT NULL,
//     openalex text,
//     ror text,
//     grid text,
//     wikipedia text,
//     wikidata text,
//     mag bigint
// );

type institutionsIdsRow struct {
	InstitutionId *string      `csv:"institution_id" sqltype:"TEXT"`
	Openalex      *string      `csv:"openalex" sqltype:"TEXT"`
	Ror           *string      `csv:"ror" sqltype:"TEXT"`
	Grid          *string      `csv:"grid" sqltype:"TEXT"`
	Wikipedia     *string      `csv:"wikipedia" sqltype:"TEXT"`
	Wikidata      *string      `csv:"wikidata" sqltype:"TEXT"`
	Mag           *json.Number `csv:"mag" sqltype:"BIGINT"`
}

func convertInstitutions(gzipPaths iter.Seq[string], outputPath string, chunk int) {
	institutionsWriter, err := OpenCsvEncoder(filepath.Join(outputPath, "institutions", fmt.Sprint("institutions", chunk, ".csv.gz")), institutionsRow{})
	if err != nil {
		log.Println(err)
		return
	}
	defer institutionsWriter.Close()
	institutionsAssociatedInstitutionsWriter, err := OpenCsvEncoder(filepath.Join(outputPath, "institutions", fmt.Sprint("institutions_associated_institutions", chunk, ".csv.gz")), institutionsAssociatedInstitutionsRow{})
	if err != nil {
		log.Println(err)
		return
	}
	defer institutionsAssociatedInstitutionsWriter.Close()
	institutionsCountsWriter, err := OpenCsvEncoder(filepath.Join(outputPath, "institutions", fmt.Sprint("institutions_counts_by_year", chunk, ".csv.gz")), institutionsCountsByYearRow{})
	if err != nil {
		log.Println(err)
		return
	}
	defer institutionsCountsWriter.Close()
	institutionsGeoWriter, err := OpenCsvEncoder(filepath.Join(outputPath, "institutions", fmt.Sprint("institutions_geo", chunk, ".csv.gz")), institutionsGeoRow{})
	if err != nil {
		log.Println(err)
		return
	}
	defer institutionsGeoWriter.Close()
	institutionsIdsWriter, err := OpenCsvEncoder(filepath.Join(outputPath, "institutions", fmt.Sprint("institutions_ids", chunk, ".csv.gz")), institutionsIdsRow{})
	if err != nil {
		log.Println(err)
		return
	}
	defer institutionsIdsWriter.Close()

	for data, err := range ReadJsonLinesAll(gzipPaths) {
		if err != nil {
			log.Println(err)
			continue
		}

		institutionId := getCast[string](data, "id")
		if institutionId == nil {
			continue
		}

		if err := institutionsWriter.Encode(institutionsRow{
			Id:                      institutionId,
			Ror:                     getCast[string](data, "ror"),
			DisplayName:             getCast[string](data, "display_name"),
			CountryCode:             getCast[string](data, "country_code"),
			Type:                    getCast[string](data, "type"),
			HomepageUrl:             getCast[string](data, "homepage_url"),
			ImageUrl:                getCast[string](data, "image_url"),
			ImageThumbnailUrl:       getCast[string](data, "image_thumbnail_url"),
			DisplayNameAcronyms:     jsontype{data["display_name_acronyms"]},
			DisplayNameAlternatives: jsontype{data["display_name_alternatives"]},
			WorksCount:              getCast[json.Number](data, "works_count"),
			CitedByCount:            getCast[json.Number](data, "cited_by_count"),
			WorksApiUrl:             getCast[string](data, "works_api_url"),
			UpdatedDate:             getCast[string](data, "updated_date"),
		}); err != nil {
			log.Println(err)
		}

		if institutionIds := getCast[map[string]any](data, "ids"); institutionIds != nil {
			if err := institutionsIdsWriter.Encode(institutionsIdsRow{
				InstitutionId: institutionId,
				Openalex:      getCast[string](*institutionIds, "openalex"),
				Ror:           getCast[string](*institutionIds, "ror"),
				Grid:          getCast[string](*institutionIds, "grid"),
				Wikipedia:     getCast[string](*institutionIds, "wikipedia"),
				Wikidata:      getCast[string](*institutionIds, "wikidata"),
				Mag:           getCast[json.Number](*institutionIds, "mag"),
			}); err != nil {
				log.Println(err)
			}
		}

		if institutionsGeo := getCast[map[string]any](data, "geo"); institutionsGeo != nil {
			if err := institutionsGeoWriter.Encode(institutionsGeoRow{
				InstitutionId:  institutionId,
				City:           getCast[string](*institutionsGeo, "city"),
				GeonamesCityId: getCast[string](*institutionsGeo, "geonames_city_id"),
				Region:         getCast[string](*institutionsGeo, "region"),
				CountryCode:    getCast[string](*institutionsGeo, "country_code"),
				Country:        getCast[string](*institutionsGeo, "country"),
				Latitude:       getCast[json.Number](*institutionsGeo, "latitude"),
				Longitude:      getCast[json.Number](*institutionsGeo, "longitude"),
			}); err != nil {
				log.Println(err)
			}
		}

		if associatedInstitutions := getCast[[]any](data, "associated_institutions"); associatedInstitutions != nil {
			for associatedInstitution := range iterCast[map[string]any](*associatedInstitutions) {
				if associatedInstitutionId := getCast[string](*associatedInstitution, "id"); associatedInstitutionId != nil {
					if err := institutionsAssociatedInstitutionsWriter.Encode(institutionsAssociatedInstitutionsRow{
						InstitutionId:           institutionId,
						AssociatedInstitutionId: associatedInstitutionId,
						Relationship:            getCast[string](*associatedInstitution, "relationship"),
					}); err != nil {
						log.Println(err)
					}
				}
			}
		}

		if countsByYear := getCast[[]any](data, "counts_by_year"); countsByYear != nil {
			for countByYear := range iterCast[map[string]any](*countsByYear) {
				if err := institutionsCountsWriter.Encode(institutionsCountsByYearRow{
					InstitutionId: institutionId,
					Year:          getCast[json.Number](*countByYear, "year"),
					WorksCount:    getCast[json.Number](*countByYear, "works_count"),
					CitedByCount:  getCast[json.Number](*countByYear, "cited_by_count"),
					OaWorksCount:  getCast[json.Number](*countByYear, "oa_works_count"),
				}); err != nil {
					log.Println(err)
				}
			}
		}
	}
}

var TypeInstitutions = EntityType{
	Name:    "institutions",
	Convert: convertInstitutions,
	WriteSqlImport: func(w io.Writer, outputPath string, numChunks int) {
		basePath := filepath.Join(outputPath, "institutions")

		writeDuckdbCopy(w, institutionsRow{}, "institutions", basePath, numChunks)
		writeDuckdbCopy(w, institutionsAssociatedInstitutionsRow{}, "institutions_associated_institutions", basePath, numChunks)
		writeDuckdbCopy(w, institutionsCountsByYearRow{}, "institutions_counts_by_year", basePath, numChunks)
		writeDuckdbCopy(w, institutionsGeoRow{}, "institutions_geo", basePath, numChunks)
		writeDuckdbCopy(w, institutionsIdsRow{}, "institutions_ids", basePath, numChunks)
	},
}
