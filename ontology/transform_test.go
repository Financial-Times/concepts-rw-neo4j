package ontology

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestTransformAggregateConceptProperties(t *testing.T) {
	expected := AggregatedConcept{
		PrefUUID:               "prefUUID value",
		PrefLabel:              "prefLabel value",
		Type:                   "type value",
		Aliases:                []string{"alias1", "alias2"},
		Strapline:              "strapline value",
		DescriptionXML:         "descriptionXML value",
		ImageURL:               "_imageUrl value",
		EmailAddress:           "emailAddress value",
		FacebookPage:           "facebookPage value",
		TwitterHandle:          "twitterHandle value",
		ScopeNote:              "scopeNote value",
		ShortLabel:             "shortLabel value",
		AggregatedHash:         "aggregateHash value",
		InceptionDate:          "inceptionDate value",
		TerminationDate:        "terminationDate value",
		InceptionDateEpoch:     1,
		TerminationDateEpoch:   2,
		FigiCode:               "figiCode value",
		ProperName:             "properName value",
		ShortName:              "shortName value",
		TradeNames:             []string{"trade name 1", "trade name 2"},
		FormerNames:            []string{"former name 1", "former name 2"},
		CountryCode:            "countryCode value",
		CountryOfRisk:          "countryOfRisk value",
		CountryOfIncorporation: "countryOfIncorporation value",
		CountryOfOperations:    "countryOfOperations value",
		PostalCode:             "postalCode value",
		YearFounded:            1,
		LeiCode:                "leiCode value",
		IsDeprecated:           true,
		ISO31661:               "iso31661 value",
		Salutation:             "salutation value",
		BirthYear:              1,
		IndustryIdentifier:     "industryIdentifier value",
	}

	newAggregateConcept := TransformToNewAggregateConcept(expected)
	got := TransformToOldAggregateConcept(newAggregateConcept)
	if !cmp.Equal(got, expected) {
		t.Errorf("transforming between old and new model has failed:\n%s", cmp.Diff(got, expected))
	}
}
