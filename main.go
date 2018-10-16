package main

import (
	"github.com/Financial-Times/concepts-rw-neo4j/alphaville-series"
	"github.com/Financial-Times/concepts-rw-neo4j/brands"
	"github.com/Financial-Times/concepts-rw-neo4j/concepts"
	"github.com/Financial-Times/concepts-rw-neo4j/genres"
	"github.com/Financial-Times/concepts-rw-neo4j/locations"
	"github.com/Financial-Times/concepts-rw-neo4j/membership-roles"
	"github.com/Financial-Times/concepts-rw-neo4j/memberships"
	"github.com/Financial-Times/concepts-rw-neo4j/organisations"
	"github.com/Financial-Times/concepts-rw-neo4j/people"
	"github.com/Financial-Times/concepts-rw-neo4j/sections"
	"github.com/Financial-Times/concepts-rw-neo4j/special-reports"
	"github.com/Financial-Times/concepts-rw-neo4j/subjects"
	"github.com/Financial-Times/concepts-rw-neo4j/topics"
	"github.com/Financial-Times/go-logger"
	"github.com/Financial-Times/neo-utils-go/neoutils"
	"github.com/gorilla/mux"
	"github.com/jawher/mow.cli"
	_ "github.com/joho/godotenv/autoload"
	"net/http"
	_ "net/http/pprof"
	"os"
	"strconv"
)

const appDescription = "A RESTful API for managing Concepts in neo4j"
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
			logger.Fatalf("Could not connect to neo4j, error=[%s]\n", err.Error())
		}

		appConf := ServerConf{
			AppSystemCode:    *appSystemCode,
			AppName:          *appName,
			Port:             *port,
			RequestLoggingOn: *requestLoggingOn,
		}

		if err := concepts.Initialise(db); err != nil {
			logger.Fatalf("Could not initialize constraints on db, error=[%s]\n", err.Error())
		}

		services := createServices(db)
		handler := concepts.ConceptsHandler{ConceptsService: services, Connection: db}
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

func createServices(db neoutils.NeoConnection) map[string]concepts.ConceptServicer {
	serviceMap := make(map[string]concepts.ConceptServicer)
	serviceMap["alphaville-series"] = alphaville_series.NewAlphavilleSeriesService(db)
	serviceMap["brands"] = brands.NewBrandService(db)
	serviceMap["genres"] = genres.NewGenreService(db)
	serviceMap["locations"] = locations.NewLocationService(db)
	serviceMap["memberships"] = memberships.NewMembershipService(db)
	serviceMap["membership-roles"] = membership_roles.NewMembershipRoleService(db)
	serviceMap["organisations"] = organisations.NewOrganisationService(db)
	serviceMap["people"] = people.NewPeopleService(db)
	serviceMap["sections"] = sections.NewSectionService(db)
	serviceMap["subjects"] = subjects.NewSubjectService(db)
	serviceMap["special-reports"] = special_reports.NewSpecialReportService(db)
	serviceMap["topics"] = topics.NewTopicService(db)
	return serviceMap
}
