package ontology

import (
	"errors"
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

func TestValidateProperties(t *testing.T) {
	tests := []struct {
		name        string
		props       map[string]interface{}
		expectedErr error
	}{
		{
			name:        "nil props are valid",
			props:       nil,
			expectedErr: nil,
		},
		{
			name:        "empty props are valid",
			props:       map[string]interface{}{},
			expectedErr: nil,
		},
		{
			name: "string props are valid string fields",
			props: map[string]interface{}{
				"emailAddress": "test@example.com",
				"_imageUrl":    "image url",
			},
			expectedErr: nil,
		},
		{
			name: "non-string props are invalid string fields",
			props: map[string]interface{}{
				"emailAddress": 5,
				"_imageUrl":    true,
			},
			expectedErr: ErrInvalidPropertyValue,
		},
		{
			name: "int props are valid int fields",
			props: map[string]interface{}{
				"yearFounded": 1,
				"birthYear":   2,
			},
			expectedErr: nil,
		},
		{
			name: "float64 props are valid int fields",
			props: map[string]interface{}{
				"yearFounded": float64(1),
				"birthYear":   float64(2),
			},
			expectedErr: nil,
		},
		{
			name: "non-number props are invalid int fields",
			props: map[string]interface{}{
				"yearFounded": "1",
				"birthYear":   false,
			},
			expectedErr: ErrInvalidPropertyValue,
		},
		{
			name: "[]string props are valid []string fields",
			props: map[string]interface{}{
				"aliases":     []string{"alias1", "alias2"},
				"formerNames": []string{"former name 1", "former name 2"},
				"tradeNames":  []string{"trade name 1", "trade name 2"},
			},
			expectedErr: nil,
		},
		{
			name: "[]interface{} props containing strings are valid []string fields",
			props: map[string]interface{}{
				"aliases":     []interface{}{"alias1", "alias2"},
				"formerNames": []interface{}{"former name 1", "former name 2"},
				"tradeNames":  []interface{}{"trade name 1", "trade name 2"},
			},
			expectedErr: nil,
		},
		{
			name: "[]interface{} props not containing strings are invalid []string fields",
			props: map[string]interface{}{
				"aliases": []interface{}{1, true},
			},
			expectedErr: ErrInvalidPropertyValue,
		},
		{
			name: "non-slice props are invalid []string fields",
			props: map[string]interface{}{
				"aliases":     "alias1",
				"formerNames": 1,
				"tradeNames":  true,
			},
			expectedErr: ErrInvalidPropertyValue,
		},
		{
			name: "props are invalid []string fields",
			props: map[string]interface{}{
				"aliases":     "alias1",
				"formerNames": 1,
				"tradeNames":  true,
			},
			expectedErr: ErrInvalidPropertyValue,
		},
		{
			name: "non-existent props are invalid fields",
			props: map[string]interface{}{
				"non-existent": "prop",
			},
			expectedErr: ErrUnkownProperty,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := GetConfig().ValidateProperties(test.props)
			if test.expectedErr != nil {
				assert.Error(t, err)
				assert.True(t, errors.Is(err, test.expectedErr))
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
