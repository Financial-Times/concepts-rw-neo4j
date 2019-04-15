package main

import (
	"net/http"
	_ "net/http/pprof"
	"os"
	"strconv"

	"github.com/Financial-Times/concepts-rw-neo4j/concepts"
	logger "github.com/Financial-Times/go-logger"
	"github.com/Financial-Times/neo-utils-go/neoutils"
	"github.com/gorilla/mux"
	cli "github.com/jawher/mow.cli"
)

const appDescription = "A RESTful API for managing Concepts in Neo4j"
const serviceName = "concepts-rw-neo4j"

type ServerConf struct {
	AppSystemCode    string
	AppName          string
	Port             int
	RequestLoggingOn bool
}

func main() {
	app := cli.App(serviceName, appDescription)
	appSystemCode := app.String(cli.StringOpt{
		Name:   "app-system-code",
		Value:  "concept-rw-neo4j",
		Desc:   "System Code of the application",
		EnvVar: "APP_SYSTEM_CODE",
	})
	appName := app.String(cli.StringOpt{
		Name:   "app-name",
		Value:  "Concept Rw Neo4j",
		Desc:   "Application name",
		EnvVar: "APP_NAME",
	})
	neoURL := app.String(cli.StringOpt{
		Name:   "neo-url",
		Value:  "http://localhost:7474/db/data",
		Desc:   "neo4j endpoint URL",
		EnvVar: "NEO_URL",
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
	requestLoggingOn := app.Bool(cli.BoolOpt{
		Name:   "requestLoggingOn",
		Value:  true,
		Desc:   "Whether to log requests or not",
		EnvVar: "REQUEST_LOGGING_ON",
	})
	logLevel := app.String(cli.StringOpt{
		Name:   "logLevel",
		Value:  "info",
		Desc:   "Level of logging to be shown",
		EnvVar: "LOG_LEVEL",
	})

	logger.InitLogger(*appName, *logLevel)
	app.Action = func() {
		conf := neoutils.DefaultConnectionConfig()
		conf.BatchSize = *batchSize
		db, err := neoutils.Connect(*neoURL, conf)

		if err != nil {
			logger.Errorf("Could not connect to neo4j, error=[%s]\n", err)
		}

		appConf := ServerConf{
			AppSystemCode:    *appSystemCode,
			AppName:          *appName,
			Port:             *port,
			RequestLoggingOn: *requestLoggingOn,
		}

		conceptsService := concepts.NewConceptService(db)
		conceptsService.Initialise()

		handler := concepts.ConceptsHandler{ConceptsService: &conceptsService}
		runServerWithParams(handler, appConf)
	}
	logger.Infof("Application started with args %s", os.Args)
	app.Run(os.Args)
}

func runServerWithParams(handler concepts.ConceptsHandler, appConf ServerConf) {
	router := mux.NewRouter()
	logger.Info("Registering handlers")
	handler.RegisterHandlers(router)

	mr := handler.RegisterAdminHandlers(router, appConf.AppSystemCode, appConf.AppName, appDescription, appConf.RequestLoggingOn)

	http.Handle("/", mr)

	logger.Printf("listening on %d", appConf.Port)

	if err := http.ListenAndServe(":"+strconv.Itoa(appConf.Port), mr); err != nil {
		logger.Fatalf("Unable to start: %v", err)
	}
	logger.Printf("exiting on %s", serviceName)
}
