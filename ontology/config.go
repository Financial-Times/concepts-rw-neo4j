package ontology

import (
	"io/ioutil"
	"path"
	"runtime"

	"gopkg.in/yaml.v2"
)

type RelationshipConfig struct {
	ConceptField    string   `yaml:"conceptField"`
	OneToOne        bool     `yaml:"oneToOne"`
	NeoCreate       bool     `yaml:"neoCreate"`
	Properties      []string `yaml:"properties"`
	ToNodeWithLabel string   `yaml:"toNodeWithLabel"`
}

type Config struct {
	FieldToNeoProps map[string]string             `yaml:"fieldToNeoProps"`
	Relationships   map[string]RelationshipConfig `yaml:"relationships"`
}

var config Config

func init() {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		panic("no caller information")
	}

	bytes, err := ioutil.ReadFile(path.Dir(file) + "/config.yml")
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
