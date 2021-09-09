package concepts

import (
	"testing"

	"github.com/Financial-Times/concepts-rw-neo4j/ontology"
	"github.com/google/go-cmp/cmp"
)

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

func TestFilterRelationships(t *testing.T) {
	tests := []struct {
		name     string
		input    []ontology.Relationship
		expected []ontology.Relationship
	}{
		{
			name:     "nil should return nil",
			input:    nil,
			expected: nil,
		},
		{
			name:     "empty slice should return nil",
			input:    []ontology.Relationship{},
			expected: nil,
		},
		{
			name:     "single relationship with empty UUID should return nil",
			input:    []ontology.Relationship{{UUID: ""}},
			expected: nil,
		},
		{
			name:     "single relationship with non-empty UUID should return itself",
			input:    []ontology.Relationship{{UUID: "non-empty-uuid"}},
			expected: []ontology.Relationship{{UUID: "non-empty-uuid"}},
		},
		{
			name:     "multiple relationships with empty UUIDs should return nil",
			input:    []ontology.Relationship{{UUID: ""}, {UUID: ""}, {UUID: ""}},
			expected: nil,
		},
		{
			name:     "multiple relationships with non-empty UUIDs should return itself",
			input:    []ontology.Relationship{{UUID: "uuid1"}, {UUID: "uuid2"}, {UUID: "uuid3"}},
			expected: []ontology.Relationship{{UUID: "uuid1"}, {UUID: "uuid2"}, {UUID: "uuid3"}},
		},
		{
			name:     "multiple relationships slice should return relationships with non-empty UUIDs",
			input:    []ontology.Relationship{{UUID: "uuid1"}, {UUID: ""}, {UUID: "uuid2"}, {UUID: ""}, {UUID: "uuid3"}},
			expected: []ontology.Relationship{{UUID: "uuid1"}, {UUID: "uuid2"}, {UUID: "uuid3"}},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := filterRelationships(test.input)
			if !cmp.Equal(test.expected, got) {
				t.Error(cmp.Diff(test.expected, got))
			}
		})
	}
}

func TestToOntologyNewAggregateConcept(t *testing.T) {
	tests := []struct {
		name        string
		neoConcept  neoAggregatedConcept
		ontologyCfg ontology.Config
		expected    ontology.NewAggregatedConcept
	}{
		{
			name: "string props",
			neoConcept: neoAggregatedConcept{
				Types:        []string{"Brand"},
				EmailAddress: "test@example.com",
				ImageURL:     "image url",
			},
			ontologyCfg: ontology.Config{
				Fields: map[string]ontology.FieldConfig{
					"emailAddress": {NeoProp: "emailAddress", FieldType: "string"},
					"_imageUrl":    {NeoProp: "imageUrl", FieldType: "string"},
				},
			},
			expected: ontology.NewAggregatedConcept{
				Type: "Brand",
				Properties: map[string]interface{}{
					"emailAddress": "test@example.com",
					"_imageUrl":    "image url",
				},
			},
		},
		{
			name: "slice of strings props",
			neoConcept: neoAggregatedConcept{
				Types:       []string{"Brand"},
				Aliases:     []string{"alias1", "alias2"},
				TradeNames:  []string{"trade name 1", "trade name 2"},
				FormerNames: []string{"former name 1", "former name 2"},
			},
			ontologyCfg: ontology.Config{
				Fields: map[string]ontology.FieldConfig{
					"aliases":     {NeoProp: "aliases", FieldType: "[]string"},
					"formerNames": {NeoProp: "formerNames", FieldType: "[]string"},
					"tradeNames":  {NeoProp: "tradeNames", FieldType: "[]string"},
				},
			},
			expected: ontology.NewAggregatedConcept{
				Type: "Brand",
				Properties: map[string]interface{}{
					"aliases":     []interface{}{"alias1", "alias2"},
					"formerNames": []interface{}{"former name 1", "former name 2"},
					"tradeNames":  []interface{}{"trade name 1", "trade name 2"},
				},
			},
		},
		{
			name: "int props",
			neoConcept: neoAggregatedConcept{
				Types:       []string{"Brand"},
				YearFounded: 1,
				BirthYear:   2,
			},
			ontologyCfg: ontology.Config{
				Fields: map[string]ontology.FieldConfig{
					"yearFounded": {NeoProp: "yearFounded", FieldType: "int"},
					"birthYear":   {NeoProp: "birthYear", FieldType: "int"},
				},
			},
			expected: ontology.NewAggregatedConcept{
				Type: "Brand",
				Properties: map[string]interface{}{
					"yearFounded": float64(1),
					"birthYear":   float64(2),
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, _, _ := test.neoConcept.ToOntologyNewAggregateConcept(test.ontologyCfg)
			if !cmp.Equal(got, test.expected) {
				t.Error(cmp.Diff(got, test.expected))
			}
		})
	}
}

func TestТоOntologyNewConcept(t *testing.T) {
	tests := []struct {
		name            string
		neoConcept      neoConcept
		ontologyRelsCfg map[string]ontology.RelationshipConfig
		expected        ontology.NewConcept
	}{
		{
			name: "simple one-to-one relationship",
			neoConcept: neoConcept{
				Types:              []string{"Brand"},
				ParentOrganisation: "c001ee9c-94c5-11e8-8f42-da24cd01f044",
			},
			ontologyRelsCfg: map[string]ontology.RelationshipConfig{
				"SUB_ORGANISATION_OF": {
					ConceptField: "parentOrganisation",
					OneToOne:     true,
				},
			},
			expected: ontology.NewConcept{
				Type: "Brand",
				Relationships: []ontology.Relationship{
					{
						UUID:  "c001ee9c-94c5-11e8-8f42-da24cd01f044",
						Label: "SUB_ORGANISATION_OF",
					},
				},
			},
		},
		{
			name: "simple one-to-many relationship",
			neoConcept: neoConcept{
				Types:         []string{"Brand"},
				HasFocusUUIDs: []string{"2e7429bd-7a84-41cb-a619-2c702893e359", "740c604b-8d97-443e-be70-33de6f1d6e67", "c28fa0b4-4245-11e8-842f-0ed5f89f718b"},
			},
			ontologyRelsCfg: map[string]ontology.RelationshipConfig{
				"HAS_FOCUS": {
					ConceptField: "hasFocusUUIDs",
				},
			},
			expected: ontology.NewConcept{
				Type: "Brand",
				Relationships: []ontology.Relationship{
					{
						UUID:  "2e7429bd-7a84-41cb-a619-2c702893e359",
						Label: "HAS_FOCUS",
					},
					{
						UUID:  "740c604b-8d97-443e-be70-33de6f1d6e67",
						Label: "HAS_FOCUS",
					},
					{
						UUID:  "c28fa0b4-4245-11e8-842f-0ed5f89f718b",
						Label: "HAS_FOCUS",
					},
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, _ := test.neoConcept.ТоOntologyNewConcept(test.ontologyRelsCfg)
			if !cmp.Equal(got, test.expected) {
				t.Error(cmp.Diff(got, test.expected))
			}
		})
	}
}
