package converters

import (
	"encoding/json"
	"fmt"
	"io"
	"iter"
	"log"
	"path/filepath"
)

type institutionsRow struct {
	Id                      *string      `csv:"id"`
	Ror                     *string      `csv:"ror"`
	DisplayName             *string      `csv:"display_name"`
	CountryCode             *string      `csv:"country_code"`
	Type                    *string      `csv:"type"`
	HomepageUrl             *string      `csv:"homepage_url"`
	ImageUrl                *string      `csv:"image_url"`
	ImageThumbnailUrl       *string      `csv:"image_thumbnail_url"`
	DisplayNameAcronyms     jsontype     `csv:"display_name_acronyms"`
	DisplayNameAlternatives jsontype     `csv:"display_name_alternatives"`
	WorksCount              *json.Number `csv:"works_count"`
	CitedByCount            *json.Number `csv:"cited_by_count"`
	WorksApiUrl             *string      `csv:"works_api_url"`
	UpdatedDate             *string      `csv:"updated_date"`
}

type institutionsAssociatedInstitutionsRow struct {
	InstitutionId           *string `csv:"institution_id"`
	AssociatedInstitutionId *string `csv:"associated_institution_id"`
	Relationship            *string `csv:"relationship"`
}

type institutionsCountsByYearRow struct {
	InstitutionId *string      `csv:"institution_id"`
	Year          *json.Number `csv:"year"`
	WorksCount    *json.Number `csv:"works_count"`
	CitedByCount  *json.Number `csv:"cited_by_count"`
	OaWorksCount  *json.Number `csv:"oa_works_count"`
}

type institutionsGeoRow struct {
	InstitutionId  *string      `csv:"institution_id"`
	City           *string      `csv:"city"`
	GeonamesCityId *string      `csv:"geonames_city_id"`
	Region         *string      `csv:"region"`
	CountryCode    *string      `csv:"country_code"`
	Country        *string      `csv:"country"`
	Latitude       *json.Number `csv:"latitude"`
	Longitude      *json.Number `csv:"longitude"`
}

type institutionsIdsRow struct {
	InstitutionId *string      `csv:"institution_id"`
	Openalex      *string      `csv:"openalex"`
	Ror           *string      `csv:"ror"`
	Grid          *string      `csv:"grid"`
	Wikipedia     *string      `csv:"wikipedia"`
	Wikidata      *string      `csv:"wikidata"`
	Mag           *json.Number `csv:"mag"`
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
