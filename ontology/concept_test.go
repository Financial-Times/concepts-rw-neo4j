package ontology

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewAggregatedConceptGetPropertyValue(t *testing.T) {
	tests := []struct {
		name        string
		concept     NewAggregatedConcept
		propName    string
		expectedVal interface{}
		expectedOk  bool
	}{
		{
			name: "non-existing prop",
			concept: NewAggregatedConcept{
				Properties: map[string]interface{}{
					"strapline": "strapline value",
				},
			},
			propName:    "nonExisting",
			expectedVal: nil,
			expectedOk:  false,
		},
		{
			name: "existing prop unknown type",
			concept: NewAggregatedConcept{
				Properties: map[string]interface{}{
					"strapline": true,
				},
			},
			propName:    "strapline",
			expectedVal: true,
			expectedOk:  false,
		},
		{
			name: "existing string prop",
			concept: NewAggregatedConcept{
				Properties: map[string]interface{}{
					"strapline": "strapline value",
				},
			},
			propName:    "strapline",
			expectedVal: "strapline value",
			expectedOk:  true,
		},
		{
			name: "existing empty string prop",
			concept: NewAggregatedConcept{
				Properties: map[string]interface{}{
					"strapline": "",
				},
			},
			propName:    "strapline",
			expectedVal: "",
			expectedOk:  false,
		},
		{
			name: "existing slice prop",
			concept: NewAggregatedConcept{
				Properties: map[string]interface{}{
					"aliases": []interface{}{"alias1", "alias2"},
				},
			},
			propName:    "aliases",
			expectedVal: []interface{}{"alias1", "alias2"},
			expectedOk:  true,
		},
		{
			name: "existing empty slice prop",
			concept: NewAggregatedConcept{
				Properties: map[string]interface{}{
					"aliases": []interface{}{},
				},
			},
			propName:    "aliases",
			expectedVal: []interface{}{},
			expectedOk:  false,
		},
		{
			name: "existing number prop",
			concept: NewAggregatedConcept{
				Properties: map[string]interface{}{
					"birthYear": float64(1),
				},
			},
			propName:    "birthYear",
			expectedVal: float64(1),
			expectedOk:  true,
		},
		{
			name: "existing number prop zero",
			concept: NewAggregatedConcept{
				Properties: map[string]interface{}{
					"birthYear": float64(0),
				},
			},
			propName:    "birthYear",
			expectedVal: float64(0),
			expectedOk:  false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			val, ok := test.concept.GetPropertyValue(test.propName)
			assert.Equal(t, val, test.expectedVal)
			assert.Equal(t, ok, test.expectedOk)
		})
	}
}
