package main

import (
	standardLog "log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"strconv"
	"time"

	"github.com/Financial-Times/concepts-rw-neo4j/concepts"
	"github.com/Financial-Times/go-logger"
	"github.com/Financial-Times/neo-utils-go/neoutils"
	"github.com/cyberdelia/go-metrics-graphite"
	"github.com/gorilla/mux"
	"github.com/jawher/mow.cli"
	_ "github.com/joho/godotenv/autoload"
	"github.com/rcrowley/go-metrics"
)

const appDescription = "A RESTful API for managing Concepts in neo4j"
const serviceName = "concepts-rw-neo4j"

type ServerConf struct {
	AppSystemCode      string
	AppName            string
	GraphiteTCPAddress string
	GraphitePrefix     string
	Port               int
	LogMetrics         bool
	RequestLoggingOn   bool
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
			AppSystemCode:      *appSystemCode,
			AppName:            *appName,
			GraphiteTCPAddress: *graphiteTCPAddress,
			GraphitePrefix:     *graphitePrefix,
			Port:               *port,
			LogMetrics:         *logMetrics,
			RequestLoggingOn:   *requestLoggingOn,
		}

		conceptsService := concepts.NewConceptService(db)
		conceptsService.Initialise()

		handler := concepts.ConceptsHandler{ConceptsService: &conceptsService}

		services := map[string]concepts.ConceptService{}
		for _, path := range concepts.ConceptTypePaths {
			services[path] = conceptsService
		}

		runServerWithParams(handler, services, appConf)
	}
	logger.Infof("Application started with args %s", os.Args)
	app.Run(os.Args)
}

func runServerWithParams(handler concepts.ConceptsHandler, services map[string]concepts.ConceptService, appConf ServerConf) {
	outputMetricsIfRequired(appConf.GraphiteTCPAddress, appConf.GraphitePrefix, appConf.LogMetrics)

	router := mux.NewRouter()
	logger.Info("Registering handlers")
	for path, service := range services {
		err := service.Initialise()
		if err != nil {
			logger.Fatalf("Service for path %s could not startup, err=%s", path, err)
		}

		router = handler.RegisterHandlers(router, path)
	}

	mr := handler.RegisterAdminHandlers(router, appConf.AppSystemCode, appConf.AppName, appDescription, appConf.RequestLoggingOn)

	http.Handle("/", mr)

	logger.Printf("listening on %d", appConf.Port)

	if err := http.ListenAndServe(":"+strconv.Itoa(appConf.Port), mr); err != nil {
		logger.Fatalf("Unable to start: %v", err)
	}
	logger.Printf("exiting on %s", serviceName)
}

func outputMetricsIfRequired(graphiteTCPAddress string, graphitePrefix string, logMetrics bool) {
	if graphiteTCPAddress != "" {
		addr, _ := net.ResolveTCPAddr("tcp", graphiteTCPAddress)
		go graphite.Graphite(metrics.DefaultRegistry, 5*time.Second, graphitePrefix, addr)
	}
	if logMetrics { //useful locally
		//messy use of the 'standard' log package here as this method takes the log struct, not an interface, so can't use logrus.Logger
		go metrics.Log(metrics.DefaultRegistry, 60*time.Second, standardLog.New(os.Stdout, "metrics", standardLog.Lmicroseconds))
	}
}
