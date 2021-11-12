package ontology

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConfigIsPropValueValid(t *testing.T) {
	cfg := Config{
		Fields: map[string]FieldConfig{
			"strapline":          {NeoProp: "strapline", FieldType: "string"},
			"aliases":            {NeoProp: "aliases", FieldType: "[]string"},
			"yearFounded":        {NeoProp: "yearFounded", FieldType: "int"},
			"nonImplementedType": {NeoProp: "isDeprecated", FieldType: "bool"},
		},
	}

	assert.True(t, cfg.IsPropValueValid("strapline", "val"))
	assert.False(t, cfg.IsPropValueValid("strapline", 1))
	assert.False(t, cfg.IsPropValueValid("strapline", true))

	assert.True(t, cfg.IsPropValueValid("aliases", []string{"alias1"}))
	assert.True(t, cfg.IsPropValueValid("aliases", []interface{}{"alias2"}))
	assert.False(t, cfg.IsPropValueValid("aliases", []interface{}{1, false}))
	assert.False(t, cfg.IsPropValueValid("aliases", "alias2"))
	assert.False(t, cfg.IsPropValueValid("aliases", 1))
	assert.False(t, cfg.IsPropValueValid("aliases", true))

	assert.True(t, cfg.IsPropValueValid("yearFounded", 1234))
	assert.False(t, cfg.IsPropValueValid("yearFounded", 1234.0))
	assert.False(t, cfg.IsPropValueValid("yearFounded", 1234.5))
	assert.False(t, cfg.IsPropValueValid("yearFounded", "1234"))

	assert.False(t, cfg.IsPropValueValid("non-existent", "prop"))
	assert.False(t, cfg.IsPropValueValid("nonImplementedType", true))
}
