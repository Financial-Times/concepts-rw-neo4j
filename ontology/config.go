package ontology

import (
	"embed"

	"gopkg.in/yaml.v2"
)

type FieldConfig struct {
	NeoProp   string `yaml:"neoProp"`
	FieldType string `yaml:"type"`
}

type RelationshipConfig struct {
	ConceptField    string   `yaml:"conceptField"`
	OneToOne        bool     `yaml:"oneToOne"`
	NeoCreate       bool     `yaml:"neoCreate"`
	Properties      []string `yaml:"properties"`
	ToNodeWithLabel string   `yaml:"toNodeWithLabel"`
}

type Config struct {
	Fields        map[string]FieldConfig        `yaml:"fields"`
	Relationships map[string]RelationshipConfig `yaml:"relationships"`
	Authorities   []string                      `yaml:"authorities"`
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
