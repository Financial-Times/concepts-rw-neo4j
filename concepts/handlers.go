package concepts

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"

	"github.com/Financial-Times/concepts-rw-neo4j/ontology"
	transactionidutils "github.com/Financial-Times/transactionid-utils-go"
	"github.com/Financial-Times/up-rw-app-api-go/rwapi"
	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
)

var irregularConceptTypePaths = map[string]string{
	"AlphavilleSeries":            "alphaville-series",
	"BoardRole":                   "membership-roles",
	"Dummy":                       "dummies",
	"Person":                      "people",
	"PublicCompany":               "organisations",
	"NAICSIndustryClassification": "industry-classifications",
}

type ConceptsHandler struct {
	ConceptsService ConceptServicer
}

func (h *ConceptsHandler) RegisterHandlers(router *mux.Router) {
	router.Handle("/{concept_type}/{uuid}", handlers.MethodHandler{
		"GET": http.HandlerFunc(h.GetConcept),
		"PUT": http.HandlerFunc(h.PutConcept),
	})
}

func (h *ConceptsHandler) PutConcept(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	uuid := vars["uuid"]
	conceptType := vars["concept_type"]

	transID := transactionidutils.GetTransactionIDFromRequest(r)
	w.Header().Add("Content-Type", "application/json")
	w.Header().Set("X-Request-Id", transID)

	var body io.Reader = r.Body
	dec := json.NewDecoder(body)
	inst, docUUID, err := h.ConceptsService.DecodeJSON(dec)

	if err != nil {
		writeJSONError(w, err.Error(), http.StatusBadRequest)
		return
	}

	if docUUID != uuid {
		writeJSONError(w, fmt.Sprintf("Uuids from payload and request, respectively, do not match: '%v' '%v'", docUUID, uuid), http.StatusBadRequest)
		return
	}

	agConcept := inst.(ontology.AggregatedConcept)
	if err := checkConceptTypeAgainstPath(agConcept.Type, conceptType); err != nil {
		writeJSONError(w, err.Error(), http.StatusBadRequest)
		return
	}

	updatedIds, err := h.ConceptsService.Write(inst, transID)

	if err != nil {
		switch e := err.(type) {
		case noContentReturnedError:
			writeJSONError(w, e.NoContentReturnedDetails(), http.StatusNoContent)
			return
		case rwapi.ConstraintOrTransactionError:
			writeJSONError(w, e.Error(), http.StatusConflict)
			return
		case invalidRequestError:
			writeJSONError(w, e.InvalidRequestDetails(), http.StatusBadRequest)
			return
		default:
			writeJSONError(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
	}

	updateIDsBody, err := json.Marshal(updatedIds)
	if err != nil {
		writeJSONError(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write(updateIDsBody)
	return
}

func (h *ConceptsHandler) GetConcept(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	uuid := vars["uuid"]
	conceptType := vars["concept_type"]

	transID := transactionidutils.GetTransactionIDFromRequest(r)

	obj, found, err := h.ConceptsService.Read(uuid, transID)

	w.Header().Add("Content-Type", "application/json")
	w.Header().Set("X-Request-Id", transID)

	if err != nil {
		writeJSONError(w, err.Error(), http.StatusServiceUnavailable)
		return
	}

	if !found {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(fmt.Sprintf("{\"message\":\"Concept with prefUUID %s not found in db.\"}", uuid)))
		return
	}

	agConcept := obj.(ontology.AggregatedConcept)
	if err := checkConceptTypeAgainstPath(agConcept.Type, conceptType); err != nil {
		writeJSONError(w, err.Error(), http.StatusBadRequest)
		return
	}

	enc := json.NewEncoder(w)
	if err := enc.Encode(obj); err != nil {
		writeJSONError(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func writeJSONError(w http.ResponseWriter, errorMsg string, statusCode int) {
	w.WriteHeader(statusCode)
	fmt.Fprintln(w, fmt.Sprintf("{\"message\": \"%s\"}", errorMsg))
}

func checkConceptTypeAgainstPath(conceptType, path string) error {
	if iPath, ok := irregularConceptTypePaths[conceptType]; ok && iPath != "" {
		if iPath != path {
			return errors.New("concept type does not match path")
		}
		return nil
	}

	if toSnakeCase(conceptType)+"s" != path {
		return errors.New("concept type does not match path")
	}
	return nil
}

var matchFirstCap = regexp.MustCompile("(.)([A-Z][a-z]+)")
var matchAllCap = regexp.MustCompile("([a-z0-9])([A-Z])")

func toSnakeCase(str string) string {
	snake := matchFirstCap.ReplaceAllString(str, "${1}-${2}")
	snake = matchAllCap.ReplaceAllString(snake, "${1}-${2}")
	return strings.ToLower(snake)
}
