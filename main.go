package main

import (
	"fmt"
	_ "net/http/pprof"
	"os"

	"time"

	"github.com/Financial-Times/base-ft-rw-app-go/baseftrwapp"
	"github.com/Financial-Times/concepts-rw-neo4j/concepts"
	"github.com/Financial-Times/go-fthealth"
	"github.com/Financial-Times/neo-utils-go/neoutils"
	log "github.com/Sirupsen/logrus"
	"github.com/jawher/mow.cli"
)

func main() {
	app := cli.App("concepts-rw-neo4j", "A RESTful API for managing Concepts in neo4j")
	neoURL := app.String(cli.StringOpt{
		Name:   "neo-url",
		Value:  "http://localhost:7474/db/data",
		Desc:   "neo4j endpoint URL",
		EnvVar: "NEO_URL",
	})
	graphiteTCPAddress := app.String(cli.StringOpt{
		Name:   "graphiteTCPAddress",
		Value:  "",
		Desc:   "Graphite TCP address, e.g. graphite.ft.com:2003. Leave as default if you do NOT want to output to graphite (e.g. if running locally",
		EnvVar: "GRAPHITE_ADDRESS",
	})
	graphitePrefix := app.String(cli.StringOpt{
		Name:   "graphitePrefix",
		Value:  "",
		Desc:   "Prefix to use. Should start with content, include the environment, and the host name. e.g. coco.pre-prod.roles-rw-neo4j.1 or content.test.concepts.rw.neo4j.ftaps58938-law1a-eu-t",
		EnvVar: "GRAPHITE_PREFIX",
	})
	port := app.Int(cli.IntOpt{
		Name:   "port",
		Value:  8080,
		Desc:   "Port to listen on",
		EnvVar: "APP_PORT",
	})
	batchSize := app.Int(cli.IntOpt{
		Name:   "batchSize",
		Value:  1024,
		Desc:   "Maximum number of statements to execute per batch",
		EnvVar: "BATCH_SIZE",
	})
	logMetrics := app.Bool(cli.BoolOpt{
		Name:   "logMetrics",
		Value:  false,
		Desc:   "Whether to log metrics. Set to true if running locally and you want metrics output",
		EnvVar: "LOG_METRICS",
	})
	env := app.String(cli.StringOpt{
		Name:  "env",
		Value: "local",
		Desc:  "environment this app is running in",
	})
	logLevel := app.String(cli.StringOpt{
		Name:   "logLevel",
		Desc:   "Level of logging to be shown",
		EnvVar: "LOG_LEVEL",
	})
	healthcheckInterval := app.String(cli.StringOpt{
		Name:   "healthcheckInterval",
		Desc:   "How often the Neo healthcheck should refresh.",
		EnvVar: "HEALTHCHECK_INTERVAL",
		Value:  "30s",
	})

	log.SetFormatter(&log.JSONFormatter{})
	lvl, err := log.ParseLevel(*logLevel)
	if err != nil {
		log.Warnf("Log level %s could not be parsed, defaulting to info")
		lvl = log.InfoLevel
	}
	log.SetLevel(lvl)
	log.Info(lvl.String() + ": log level set")

	app.Action = func() {
		conf := neoutils.DefaultConnectionConfig()
		conf.BatchSize = *batchSize
		db, err := neoutils.Connect(*neoURL, conf)

		if err != nil {
			log.Errorf("Could not connect to neo4j, error=[%s]\n", err)
		}

		duration, err := time.ParseDuration(*healthcheckInterval)
		if err != nil {
			log.Warnf("Could not parse healthcheck duration, defaulting to 30s.")
			duration = time.Duration(30 * time.Second)
		}

		asyncHealthcheck := neoutils.NewAsyncHealthcheck(duration)

		conceptsDriver := concepts.NewConceptService(db, asyncHealthcheck)
		conceptsDriver.Initialise()

		baseftrwapp.OutputMetricsIfRequired(*graphiteTCPAddress, *graphitePrefix, *logMetrics)

		services := map[string]baseftrwapp.Service{}

		for _, path := range concepts.BasicTmePaths {
			services[path] = conceptsDriver
		}

		var checks []fthealth.Check

		// We are only checking Neo4J so only need to check with the implementation for one type
		checks = append(checks, makeCheck(services[concepts.BasicTmePaths[0]], db))

		baseftrwapp.RunServerWithConf(baseftrwapp.RWConf{
			Services:      services,
			HealthHandler: fthealth.Handler("ft-concepts_rw_neo4j ServiceModule", "Writes 'concepts' to Neo4j, usually as part of a bulk upload done on a schedule", checks...),
			Port:          *port,
			ServiceName:   "concepts-rw-neo4j",
			Env:           *env,
			EnableReqLog:  true,
		})
	}
	log.Infof("Application started with args %s", os.Args)
	app.Run(os.Args)
}

func makeCheck(service baseftrwapp.Service, cr neoutils.CypherRunner) fthealth.Check {
	return fthealth.Check{
		BusinessImpact:   "Cannot read/write concepts via this writer",
		Name:             "Check connectivity to Neo4j - neoUrl is a parameter in hieradata for this service",
		PanicGuide:       "https://dewey.ft.com/concepts-rw-neo4j.html",
		Severity:         1,
		TechnicalSummary: fmt.Sprintf("Cannot connect to Neo4j instance %s with at least one concept loaded in it", cr),
		Checker:          func() error { return service.Check() },
	}
}
