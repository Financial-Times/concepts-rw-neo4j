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
				FieldToNeoProps: map[string]string{
					"emailAddress": "emailAddress",
					"_imageUrl":    "imageUrl",
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
				FieldToNeoProps: map[string]string{
					"aliases":     "aliases",
					"formerNames": "formerNames",
					"tradeNames":  "tradeNames",
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
				FieldToNeoProps: map[string]string{
					"yearFounded": "yearFounded",
					"birthYear":   "birthYear",
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
