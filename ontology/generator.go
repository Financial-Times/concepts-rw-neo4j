package main

import (
	"io/ioutil"
	"path/filepath"

	"github.com/99designs/gqlgen/codegen"
	"github.com/99designs/gqlgen/codegen/config"
	"github.com/99designs/gqlgen/codegen/templates"
	"github.com/99designs/gqlgen/plugin"
	"github.com/99designs/gqlgen/plugin/modelgen"
	"github.com/vektah/gqlparser/v2/ast"
)

func main() {
	cfg := &config.Config{
		SchemaFilename: config.StringList{"schema.graphql"},
		Model:          config.PackageConfig{Filename: "generated/models.go"},
		Exec:           config.PackageConfig{Filename: "generated/ontology.go"},
		Directives:     map[string]config.DirectiveConfig{},
		Models:         config.TypeMap{},
	}

	for _, filename := range cfg.SchemaFilename {
		filename = filepath.ToSlash(filename)
		var err error
		var schemaRaw []byte
		schemaRaw, err = ioutil.ReadFile(filename)
		if err != nil {
			panic(err)
		}

		cfg.Sources = append(cfg.Sources, &ast.Source{Name: filename, Input: string(schemaRaw)})
	}

	plugins := []plugin.Plugin{}
	if cfg.Model.IsDefined() {
		plugins = append(plugins, modelgen.New())
	}

	for _, p := range plugins {
		if inj, ok := p.(plugin.EarlySourceInjector); ok {
			if s := inj.InjectSourceEarly(); s != nil {
				cfg.Sources = append(cfg.Sources, s)
			}
		}
	}

	if err := cfg.LoadSchema(); err != nil {
		panic(err)
	}

	if err := cfg.Init(); err != nil {
		panic(err)
	}

	for _, p := range plugins {
		if mut, ok := p.(plugin.ConfigMutator); ok {
			err := mut.MutateConfig(cfg)
			if err != nil {
				panic(err)
			}
		}
	}

	data, err := codegen.BuildData(cfg)
	if err != nil {
		panic(err)
	}

	template, err := ioutil.ReadFile("ontology.go.tpl")
	if err != nil {
		panic(err)
	}

	err = templates.Render(templates.Options{
		Template:        string(template),
		PackageName:     data.Config.Exec.Package,
		Filename:        data.Config.Exec.Filename,
		Data:            data,
		RegionTags:      true,
		GeneratedHeader: true,
		Packages:        data.Config.Packages,
	})

	if err != nil {
		panic(err)
	}
}
