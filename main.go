package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	cmneo4j "github.com/Financial-Times/cm-neo4j-driver"
	"github.com/Financial-Times/concepts-rw-neo4j/concepts"
	logger "github.com/Financial-Times/go-logger/v2"
	"github.com/gorilla/mux"
	cli "github.com/jawher/mow.cli"
	"github.com/neo4j/neo4j-go-driver/v4/neo4j"
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
		Value:  "bolt://localhost:7687",
		Desc:   "neoURL must point to a leader node or to use neo4j:// scheme, otherwise writes will fail",
		EnvVar: "NEO_URL",
	})
	port := app.Int(cli.IntOpt{
		Name:   "port",
		Value:  8080,
		Desc:   "Port to listen on",
		EnvVar: "APP_PORT",
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
		Desc:   "Level of logging to be shown (debug, info, warn, error)",
		EnvVar: "LOG_LEVEL",
	})
	dbDriverLogLevel := app.String(cli.StringOpt{
		Name:   "dbDriverLogLevel",
		Value:  "warn",
		Desc:   "Db's driver logging level (debug, info, warn, error)",
		EnvVar: "DB_DRIVER_LOG_LEVEL",
	})
	maxTxRetryTime := app.Int(cli.IntOpt{
		Name:   "maxTxRetryTime",
		Value:  30,
		Desc:   "Maximum amount of time a to retry executing a transaction(in seconds)",
		EnvVar: "MAX_TX_RETRY_TIME",
	})

	log := logger.NewUPPLogger(*appSystemCode, *logLevel)
	dbDriverLog := logger.NewUPPLogger(*appName+"-cmneo4j-driver", *dbDriverLogLevel)
	app.Action = func() {
		driver, err := cmneo4j.NewDriver(*neoURL, neo4j.NoAuth(), func(c *neo4j.Config) {
			c.MaxTransactionRetryTime = time.Duration(*maxTxRetryTime) * time.Second
			c.Log = cmneo4j.NewLogger(dbDriverLog)
		})
		if err != nil {
			log.WithError(err).WithField("neoURL", *neoURL).Fatal("Could not create a cmneo4j driver")
		}

		conceptsService := concepts.NewConceptService(driver, log)
		err = conceptsService.Initialise()
		if err != nil {
			log.WithError(err).Fatal("Failed to initialise ConceptService")
		}

		appConf := ServerConf{
			AppSystemCode:    *appSystemCode,
			AppName:          *appName,
			Port:             *port,
			RequestLoggingOn: *requestLoggingOn,
		}
		handler := concepts.ConceptsHandler{ConceptsService: &conceptsService}
		runServerWithParams(handler, appConf, log)
	}
	log.WithField("args", os.Args).Info("Application started")
	app.Run(os.Args)
}

func runServerWithParams(handler concepts.ConceptsHandler, appConf ServerConf, log *logger.UPPLogger) {
	log.Info("Registering handlers")
	router := mux.NewRouter()
	handler.RegisterHandlers(router)
	serveMux := handler.RegisterAdminHandlers(router, log, appConf.AppSystemCode, appConf.AppName, appDescription, appConf.RequestLoggingOn)

	server := &http.Server{
		Addr:    ":" + strconv.Itoa(appConf.Port),
		Handler: serveMux,
	}

	go func() {
		log.Infof("Starting HTTP server listening on %d", appConf.Port)
		if err := server.ListenAndServe(); err != http.ErrServerClosed {
			log.WithError(err).Fatal("Unable to start HTTP server")
		}
	}()

	waitForSignal()
	log.Info("Received termination signal: shutting down HTTP server")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		log.WithError(err).Fatalf("Failed to gracefully shutdown the server")
	}
}

func waitForSignal() {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	<-ch
}
