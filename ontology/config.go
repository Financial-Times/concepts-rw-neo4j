package ontology

import (
	"io/ioutil"
	"path"
	"runtime"

	"gopkg.in/yaml.v2"
)

var Relationships = map[string]struct {
	ConceptField    string
	OneToOne        bool
	NeoCreate       bool
	Properties      []string
	ToNodeWithLabel string
}{}

type Config struct {
	FieldToNeoProps map[string]string `yaml:"fieldToNeoProps"`
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
