package neo4j

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"path/filepath"
	"strings"
	"testing"

	cmneo4j "github.com/Financial-Times/cm-neo4j-driver"
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"

	"github.com/Financial-Times/concepts-rw-neo4j/ontology"
	"github.com/Financial-Times/concepts-rw-neo4j/ontology/transform"
)

func TestPopulateConceptQueries(t *testing.T) {
	tests := []struct {
		name           string
		conceptFile    string
		goldenFileName string
	}{
		{
			name:           "Aggregate concept with default values",
			conceptFile:    "concept-queries-default.json",
			goldenFileName: "testdata/concept-queries-default.golden",
		},
		{
			name:           "Aggregate concept with default values and single default source",
			conceptFile:    "concept-queries-default-source.json",
			goldenFileName: "testdata/concept-queries-default-source.golden",
		},
		{
			name:           "Aggregate concept with HAS_PARENT relationship",
			conceptFile:    "full-concorded-aggregated-concept.json",
			goldenFileName: "testdata/concept-queries-has-parent-rel.golden",
		},
		{
			name:           "Aggregate concept with HAS_BROADER relationship",
			conceptFile:    "concept-with-multiple-has-broader.json",
			goldenFileName: "testdata/concept-queries-has-broader-rel.golden",
		},
		{
			name:           "Aggregate concept with IS_RELATED_TO relationship",
			conceptFile:    "concept-with-multiple-related-to.json",
			goldenFileName: "testdata/concept-queries-is-related-to-rel.golden",
		},
		{
			name:           "Aggregate concept with SUPERSEDED_BY relationship",
			conceptFile:    "concept-with-multiple-superseded-by.json",
			goldenFileName: "testdata/concept-queries-superseded-by-rel.golden",
		},
		{
			name:           "Aggregate concept with IMPLIED_BY relationship",
			conceptFile:    "brand-with-multiple-implied-by.json",
			goldenFileName: "testdata/concept-queries-implied-by-rel.golden",
		},
		{
			name:           "Aggregate concept with HAS_FOCUS relationship",
			conceptFile:    "concept-with-multiple-has-focus.json",
			goldenFileName: "testdata/concept-queries-has-focus-rel.golden",
		},
		{
			name:           "Aggregate concept with HAS_MEMBER, HAS_ORGANISATION & HAS_ROLE relationships",
			conceptFile:    "membership-with-roles-and-org.json",
			goldenFileName: "testdata/concept-queries-membership-rels.golden",
		},
		{
			name:           "Aggregate concept with COUNTRY_OF & NAICS relationships",
			conceptFile:    "organisation-with-naics.json",
			goldenFileName: "testdata/concept-queries-country-of-naics-rels.golden",
		},
		{
			name:           "Aggregate concept with SUB_ORGANISATION_OF relationship",
			conceptFile:    "organisation.json",
			goldenFileName: "testdata/concept-queries-sub-organisation-of-rel.golden",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var concept ontology.NewAggregatedConcept
			if test.conceptFile != "" {
				concept = getAggregatedConcept(t, test.conceptFile)
			}
			queries := WriteCanonicalConceptQueries(concept)
			got := cypherBatchToString(queries)

			expectedStatement := getFromGoldenFile(t, test.goldenFileName, got, *update)
			if !cmp.Equal(expectedStatement, got) {
				t.Errorf("Got unexpected Cypher query batch:\n%s", cmp.Diff(expectedStatement, got))
			}
		})
	}
}

func TestWriteUnconcordedConcept(t *testing.T) {
	concept := getSourceConcept(t, "WriteCanonicalForUnconcordedConcept/concept.json")
	query := WriteCanonicalForUnconcordedConcept(concept)
	got := cypherBatchToString([]*cmneo4j.Query{query})
	expected := getFromGoldenFile(t, "testdata/WriteCanonicalForUnconcordedConcept/query.golden", got, *update)
	if !cmp.Equal(expected, got) {
		t.Errorf("Got unexpected Cypher query batch:\n%s", cmp.Diff(expected, got))
	}
}

func TestFilterSlice(t *testing.T) {
	tests := []struct {
		name     string
		input    []string
		expected []string
	}{
		{
			name:     "nil should return nil",
			input:    nil,
			expected: nil,
		},
		{
			name:     "empty slice should return nil",
			input:    []string{},
			expected: nil,
		},
		{
			name:     "one element empty string slice should return nil",
			input:    []string{""},
			expected: nil,
		},
		{
			name:     "one element non-empty string slice should return itself",
			input:    []string{"non-empty-string"},
			expected: []string{"non-empty-string"},
		},
		{
			name:     "multiple empty strings slice should return nil",
			input:    []string{"", "", "", "", ""},
			expected: nil,
		},
		{
			name:     "multiple non-empty strings slice should return itself",
			input:    []string{"multiple", "non-empty", "strings", "slice"},
			expected: []string{"multiple", "non-empty", "strings", "slice"},
		},
		{
			name:     "multiple strings slice should return slice with non-empty strings",
			input:    []string{"multiple", "", "strings", "", "slice"},
			expected: []string{"multiple", "strings", "slice"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := filterSlice(test.input)
			if !cmp.Equal(test.expected, got) {
				t.Error(cmp.Diff(test.expected, got))
			}
		})
	}
}

func TestSetCanonicalProps(t *testing.T) {
	tests := []struct {
		name     string
		concept  ontology.NewAggregatedConcept
		prefUUID string
		expected map[string]interface{}
	}{
		{
			name:    "Concept with default values and no prefUUID should return default props",
			concept: ontology.NewAggregatedConcept{},
			expected: map[string]interface{}{
				"prefUUID":      "",
				"aggregateHash": "",
			},
		},
		{
			name:     "Concept with default values with prefUUID should return props with prefUUID",
			concept:  ontology.NewAggregatedConcept{},
			prefUUID: "6649aeda-0cd0-4a65-a310-77f28e88b620",
			expected: map[string]interface{}{
				"prefUUID":      "6649aeda-0cd0-4a65-a310-77f28e88b620",
				"aggregateHash": "",
			},
		},
		{
			name: "Concept with empty values for properties should return default props",
			concept: ontology.NewAggregatedConcept{
				Properties: map[string]interface{}{
					"strapline":              "",
					"descriptionXML":         "",
					"imageUrl":               "",
					"emailAddress":           "",
					"facebookPage":           "",
					"twitterHandle":          "",
					"scopeNote":              "",
					"shortLabel":             "",
					"properName":             "",
					"shortName":              "",
					"countryCode":            "",
					"countryOfRisk":          "",
					"countryOfIncorporation": "",
					"countryOfOperations":    "",
					"postalCode":             "",
					"leiCode":                "",
					"iso31661":               "",
					"salutation":             "",
					"industryIdentifier":     "",
					"aliases":                []string{},
					"formerNames":            []string{},
					"tradeNames":             []string{},
					"yearFounded":            0,
					"birthYear":              0,
				},
			},
			prefUUID: "bbc4f575-edb3-4f51-92f0-5ce6c708d1ea",
			expected: map[string]interface{}{
				"prefUUID":      "bbc4f575-edb3-4f51-92f0-5ce6c708d1ea",
				"aggregateHash": "",
			},
		},
		{
			name: "Concept with non-empty valid values should return valid props",
			concept: ontology.NewAggregatedConcept{
				PrefLabel:       "prefLabel value",
				AggregatedHash:  "aggregateHash value",
				InceptionDate:   "inceptionDate value",
				TerminationDate: "terminationDate value",
				FigiCode:        "figiCode value",
				IsDeprecated:    true,
				Properties: map[string]interface{}{
					"strapline":              "strapline value",
					"descriptionXML":         "descriptionXML value",
					"_imageUrl":              "imageUrl value",
					"emailAddress":           "emailAddress value",
					"facebookPage":           "facebookPage value",
					"twitterHandle":          "twitterHandle value",
					"scopeNote":              "scopeNote value",
					"shortLabel":             "shortLabel value",
					"properName":             "properName value",
					"shortName":              "shortName value",
					"countryCode":            "countryCode value",
					"countryOfRisk":          "countryOfRisk value",
					"countryOfIncorporation": "countryOfIncorporation value",
					"countryOfOperations":    "countryOfOperations value",
					"postalCode":             "postalCode value",
					"leiCode":                "leiCode value",
					"iso31661":               "iso31661 value",
					"salutation":             "salutation value",
					"industryIdentifier":     "industryIdentifier value",
					"aliases":                []interface{}{"alias1", "alias2"},
					"formerNames":            []interface{}{"former name 1", "former name 2"},
					"tradeNames":             []interface{}{"trade name 1", "trade name 2"},
					"yearFounded":            1,
					"birthYear":              2,
				},
			},
			prefUUID: "bbc4f575-edb3-4f51-92f0-5ce6c708d1ea",
			expected: map[string]interface{}{
				"prefUUID":               "bbc4f575-edb3-4f51-92f0-5ce6c708d1ea",
				"prefLabel":              "prefLabel value",
				"aggregateHash":          "aggregateHash value",
				"inceptionDate":          "inceptionDate value",
				"terminationDate":        "terminationDate value",
				"figiCode":               "figiCode value",
				"isDeprecated":           true,
				"strapline":              "strapline value",
				"descriptionXML":         "descriptionXML value",
				"imageUrl":               "imageUrl value",
				"emailAddress":           "emailAddress value",
				"facebookPage":           "facebookPage value",
				"twitterHandle":          "twitterHandle value",
				"scopeNote":              "scopeNote value",
				"shortLabel":             "shortLabel value",
				"properName":             "properName value",
				"shortName":              "shortName value",
				"countryCode":            "countryCode value",
				"countryOfRisk":          "countryOfRisk value",
				"countryOfIncorporation": "countryOfIncorporation value",
				"countryOfOperations":    "countryOfOperations value",
				"postalCode":             "postalCode value",
				"leiCode":                "leiCode value",
				"iso31661":               "iso31661 value",
				"salutation":             "salutation value",
				"industryIdentifier":     "industryIdentifier value",
				"aliases":                []interface{}{"alias1", "alias2"},
				"formerNames":            []interface{}{"former name 1", "former name 2"},
				"tradeNames":             []interface{}{"trade name 1", "trade name 2"},
				"yearFounded":            1,
				"birthYear":              2,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := setCanonicalProps(test.concept, test.prefUUID)

			// check that "lastModifiedEpoch" is always set and ignore it
			_, ok := got["lastModifiedEpoch"]
			assert.True(t, ok, "expected lastModifiedEpoch to be set")
			delete(got, "lastModifiedEpoch")

			if !cmp.Equal(got, test.expected) {
				t.Errorf("Node props differ from expected:\n%s", cmp.Diff(got, test.expected))
			}
		})
	}
}

func cypherBatchToString(queryBatch []*cmneo4j.Query) string {
	var queries []string
	for _, query := range queryBatch {
		// ignore lastModifiedEpoch from allprops
		if _, ok := query.Params["allprops"]; ok {
			props := query.Params["allprops"].(map[string]interface{})
			delete(props, "lastModifiedEpoch")
			query.Params["allprops"] = props
		}

		params, _ := json.MarshalIndent(query.Params, "", "  ")
		queries = append(queries, fmt.Sprintf("Statement: %v,\nParemeters: %v", query.Cypher, string(params)))
	}

	return strings.Join(queries, "\n==============================================================================\n")
}

func helperLoadBytes(t *testing.T, name string) []byte {
	path := filepath.Join("testdata", name)
	bytes, err := ioutil.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return bytes
}

// A lone concept should always have matching pref labels and uuid at the src system level and the top level - We are
// currently missing validation around this
func getAggregatedConcept(t *testing.T, name string) ontology.NewAggregatedConcept {
	t.Helper()
	ac := transform.OldAggregatedConcept{}
	err := json.Unmarshal(helperLoadBytes(t, name), &ac)
	if err != nil {
		t.Fatal(err)
	}
	result, err := transform.ToNewAggregateConcept(ac)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func getSourceConcept(t *testing.T, name string) ontology.NewConcept {
	t.Helper()
	ac := transform.OldConcept{}
	err := json.Unmarshal(helperLoadBytes(t, name), &ac)
	if err != nil {
		t.Fatal(err)
	}
	result, err := transform.ToNewSourceConcept(ac)
	if err != nil {
		t.Fatal(err)
	}
	return result
}
