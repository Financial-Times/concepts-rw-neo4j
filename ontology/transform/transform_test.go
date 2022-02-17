package transform

import (
	"sort"
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

	newAggregateConcept, err := TransformToNewAggregateConcept(expected)
	if err != nil {
		t.Fatal(err)
	}
	got, err := TransformToOldAggregateConcept(newAggregateConcept)
	if err != nil {
		t.Fatal(err)
	}
	if !cmp.Equal(got, expected) {
		t.Errorf("transforming between old and new model has failed:\n%s", cmp.Diff(got, expected))
	}
}

func TestTransformSourceConceptRelationships(t *testing.T) {
	expected := Concept{
		ParentUUIDs:                []string{"2ef39c2a-da9c-4263-8209-ebfd490d3101"},
		BroaderUUIDs:               []string{"f7e3fe2d-7496-4d42-b19f-378094efd263", "b5d7c6b5-db7d-4bce-9d6a-f62195571f92"},
		RelatedUUIDs:               []string{"f7e3fe2d-7496-4d42-b19f-378094efd263", "b5d7c6b5-db7d-4bce-9d6a-f62195571f92"},
		SupersededByUUIDs:          []string{"1a96ee7a-a4af-3a56-852c-60420b0b8da6", "b5d7c6b5-db7d-4bce-9d6a-f62195571f92"},
		ImpliedByUUIDs:             []string{"740c604b-8d97-443e-be70-33de6f1d6e67", "b5d7c6b5-db7d-4bce-9d6a-f62195571f92"},
		HasFocusUUIDs:              []string{"2e7429bd-7a84-41cb-a619-2c702893e359", "740c604b-8d97-443e-be70-33de6f1d6e67", "c28fa0b4-4245-11e8-842f-0ed5f89f718b"},
		OrganisationUUID:           "7f40d291-b3cb-47c4-9bce-18413e9350cf",
		PersonUUID:                 "35946807-0205-4fc1-8516-bb1ae141659b",
		CountryOfRiskUUID:          "coreb1c1-7ecd-4600-8cbb-c02ba53ced4b",
		CountryOfIncorporationUUID: "coieb1c1-7ecd-4600-8cbb-c02ba53ced4b",
		CountryOfOperationsUUID:    "cooeb1c1-7ecd-4600-8cbb-c02ba53ced4b",
		ParentOrganisation:         "c001ee9c-94c5-11e8-8f42-da24cd01f044",
		MembershipRoles: []MembershipRole{
			{
				RoleUUID:             "f7063d80-0f52-418f-874c-f2968a9ffe9b",
				InceptionDate:        "2022-02-02",
				TerminationDate:      "",
				InceptionDateEpoch:   1643767200,
				TerminationDateEpoch: 0,
			},
		},
		NAICSIndustryClassifications: []NAICSIndustryClassification{
			{
				UUID: "67c42188-d4fe-47bf-b952-83600d01b6bf",
				Rank: 2,
			},
			{
				UUID: "786d6cee-800f-4028-98ed-6f19a6d2a701",
				Rank: 1,
			},
		},
	}

	newSourceConcept, err := TransformToNewSourceConcept(expected)
	if err != nil {
		t.Fatal(err)
	}
	got, err := TransformToOldSourceConcept(newSourceConcept)
	if err != nil {
		t.Fatal(err)
	}

	sort.Strings(expected.ParentUUIDs)
	sort.Strings(expected.BroaderUUIDs)
	sort.Strings(expected.RelatedUUIDs)
	sort.Strings(expected.SupersededByUUIDs)
	sort.Strings(expected.ImpliedByUUIDs)
	sort.Strings(expected.HasFocusUUIDs)

	sort.SliceStable(expected.NAICSIndustryClassifications, func(i, j int) bool {
		return expected.NAICSIndustryClassifications[i].Rank < expected.NAICSIndustryClassifications[j].Rank
	})
	sort.SliceStable(expected.MembershipRoles, func(i, j int) bool {
		left := expected.MembershipRoles[i]
		right := expected.MembershipRoles[j]
		if left.RoleUUID < right.RoleUUID {
			return true
		}
		if left.InceptionDateEpoch < right.InceptionDateEpoch {
			return true
		}
		if left.TerminationDate < right.TerminationDate {
			return true
		}
		return false
	})

	sort.Strings(got.ParentUUIDs)
	sort.Strings(got.BroaderUUIDs)
	sort.Strings(got.RelatedUUIDs)
	sort.Strings(got.SupersededByUUIDs)
	sort.Strings(got.ImpliedByUUIDs)
	sort.Strings(got.HasFocusUUIDs)

	sort.SliceStable(got.NAICSIndustryClassifications, func(i, j int) bool {
		return got.NAICSIndustryClassifications[i].Rank < got.NAICSIndustryClassifications[j].Rank
	})
	sort.SliceStable(got.MembershipRoles, func(i, j int) bool {
		left := got.MembershipRoles[i]
		right := got.MembershipRoles[j]
		if left.RoleUUID < right.RoleUUID {
			return true
		}
		if left.InceptionDateEpoch < right.InceptionDateEpoch {
			return true
		}
		if left.TerminationDate < right.TerminationDate {
			return true
		}
		return false
	})

	if !cmp.Equal(got, expected) {
		t.Errorf("transforming between old and new source model has failed:\n%s", cmp.Diff(got, expected))
	}
}
