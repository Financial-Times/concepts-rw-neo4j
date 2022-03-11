package ontology

import (
	"embed"
	"errors"
	"fmt"

	"gopkg.in/yaml.v2"
)

type FieldConfig struct {
	NeoProp   string `yaml:"neoProp"`
	FieldType string `yaml:"type"`
}

const PropertyTypeDate = "date"

type RelationshipConfig struct {
	ConceptField    string            `yaml:"conceptField"`
	OneToOne        bool              `yaml:"oneToOne"`
	NeoCreate       bool              `yaml:"neoCreate"`
	Properties      map[string]string `yaml:"properties"` // map of property label: type
	ToNodeWithLabel string            `yaml:"toNodeWithLabel"`
}

type Config struct {
	Fields        map[string]FieldConfig        `yaml:"fields"`
	Relationships map[string]RelationshipConfig `yaml:"relationships"`
	Authorities   []string                      `yaml:"authorities"`
	ConceptTypes  []string                      `yaml:"conceptTypes"`
}

var ErrUnknownProperty = errors.New("unknown concept property")
var ErrInvalidPropertyValue = errors.New("invalid property value")

func (cfg Config) ValidateProperties(props map[string]interface{}) error {
	for propName, propVal := range props {
		if !cfg.HasField(propName) {
			return fmt.Errorf("propName=%s: %w", propName, ErrUnknownProperty)
		}

		if !cfg.IsPropValueValid(propName, propVal) {
			return InvalidPropValueError(propName, propVal)
		}
	}

	return nil
}

func (cfg Config) HasField(propName string) bool {
	_, has := cfg.Fields[propName]
	return has
}

func (cfg Config) IsPropValueValid(propName string, val interface{}) bool {
	fieldType := cfg.Fields[propName].FieldType
	switch fieldType {
	case "string":
		_, ok := val.(string)
		return ok
	case "[]string":
		_, ok := val.([]string)
		if ok {
			return true
		}

		vs, ok := val.([]interface{}) // []interface{}, for JSON arrays
		if !ok {
			return false
		}

		for _, v := range vs {
			_, ok := v.(string)
			if !ok {
				return false
			}
		}

		return true
	case "int":
		_, ok := val.(int)
		return ok
	default:
		return false
	}
}

func InvalidPropValueError(name string, val interface{}) error {
	return fmt.Errorf("propName=%s, value=%v: %w", name, val, ErrInvalidPropertyValue)
}

var config Config

//go:embed config.yml
var f embed.FS

func init() {
	bytes, err := f.ReadFile("config.yml")
	if err != nil {
		panic(err)
	}

	err = yaml.Unmarshal(bytes, &config)
	if err != nil {
		panic(err)
	}
}

func GetConfig() Config {
	return config
}

func (cfg Config) GetConceptTypes() []string {
	return cfg.ConceptTypes
}
