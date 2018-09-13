package concepts

import (
	"github.com/Financial-Times/neo-utils-go/neoutils"
	"net/http"
	"time"

	fthealth "github.com/Financial-Times/go-fthealth/v1_1"
	"github.com/Financial-Times/go-logger"
	"github.com/Financial-Times/http-handlers-go/httphandlers"
	"github.com/Financial-Times/service-status-go/gtg"
	st "github.com/Financial-Times/service-status-go/httphandlers"
	"github.com/gorilla/mux"
	"github.com/rcrowley/go-metrics"
	log "github.com/sirupsen/logrus"
)

func (h *ConceptsHandler) RegisterAdminHandlers(router *mux.Router, appSystemCode string, appName string, appDescription string, enableRequestLogging bool) http.Handler {
	logger.Info("Registering healthcheck handlers")

	hc := fthealth.TimedHealthCheck{
		HealthCheck: fthealth.HealthCheck{
			SystemCode:  appSystemCode,
			Name:        appName,
			Description: appDescription,
			Checks:      h.checks(),
		},
		Timeout: 10 * time.Second,
	}

	router.HandleFunc("/__health", fthealth.Handler(hc))
	router.HandleFunc(st.BuildInfoPath, st.BuildInfoHandler)
	router.HandleFunc(st.GTGPath, st.NewGoodToGoHandler(h.GTG))

	var monitoringRouter http.Handler = router
	if enableRequestLogging {
		monitoringRouter = httphandlers.TransactionAwareRequestLoggingHandler(log.StandardLogger(), monitoringRouter)
	}
	monitoringRouter = httphandlers.HTTPMetricsHandler(metrics.DefaultRegistry, monitoringRouter)

	return monitoringRouter
}

func (h *ConceptsHandler) GTG() gtg.Status {
	var statusChecker []gtg.StatusChecker
	for _, c := range h.checks() {
		checkFunc := func() gtg.Status {
			return gtgCheck(c.Checker)
		}
		statusChecker = append(statusChecker, checkFunc)
	}
	return gtg.FailFastParallelCheck(statusChecker)()
}

func (h *ConceptsHandler) checks() []fthealth.Check {
	return []fthealth.Check{h.makeNeo4jAvailabilityCheck()}
}

func (h *ConceptsHandler) makeNeo4jAvailabilityCheck() fthealth.Check {
	return fthealth.Check{
		BusinessImpact:   "Cannot read/write concepts via this writer",
		Name:             "Check connectivity to Neo4j - neoUrl is a parameter in hieradata for this service",
		PanicGuide:       "https://dewey.ft.com/concepts-rw-neo4j.html",
		Severity:         2,
		TechnicalSummary: "Cannot connect to Neo4j instance with at least one concept loaded in it",
		Checker:          h.checkNeo4jAvailability,
	}
}

func (h *ConceptsHandler) checkNeo4jAvailability() (string, error) {
	err := neoutils.Check(h.Connection)
	if err != nil {
		return "Could not connect to database!", err
	}
	return "", nil
}

func gtgCheck(handler func() (string, error)) gtg.Status {
	if _, err := handler(); err != nil {
		return gtg.Status{GoodToGo: false, Message: err.Error()}
	}
	return gtg.Status{GoodToGo: true}
}
