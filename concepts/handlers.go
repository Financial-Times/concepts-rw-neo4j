package concepts

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"

	ontology "github.com/Financial-Times/cm-graph-ontology/v2"

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
	"FTAnIIndustryClassification": "industry-classifications",
	"SVCategory":                  "sv-categories",
}

type ConceptsHandler struct {
	ConceptsService ConceptServicer
}

func (h *ConceptsHandler) RegisterHandlers(router *mux.Router) {
	router.Handle("/{concept_type}/{uuid}", handlers.MethodHandler{
		"GET":    http.HandlerFunc(h.GetConcept),
		"PUT":    http.HandlerFunc(h.PutConcept),
		"DELETE": http.HandlerFunc(h.DeleteConcept),
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

	agConcept := inst.(ontology.CanonicalConcept)
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
		writeJSONError(w, fmt.Sprintf("Concept with prefUUID %s not found in db.", uuid), http.StatusNotFound)
		return
	}

	agConcept := obj.(ontology.CanonicalConcept)
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

func (h *ConceptsHandler) DeleteConcept(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	uuid := vars["uuid"]
	conceptType := vars["concept_type"]

	transID := transactionidutils.GetTransactionIDFromRequest(r)
	w.Header().Add("Content-Type", "application/json")
	w.Header().Set("X-Request-Id", transID)

	// Validate that the concept exists and is of the right type.
	obj, found, err := h.ConceptsService.Read(uuid, transID)
	if err != nil {
		writeJSONError(w, err.Error(), http.StatusServiceUnavailable, uuid)
		return
	}
	if !found {
		writeJSONError(w, fmt.Sprintf("Concept with prefUUID %s not found in db.", uuid), http.StatusNotFound, uuid)
		return
	}
	agConcept := obj.(ontology.CanonicalConcept)
	if err := checkConceptTypeAgainstPath(agConcept.Type, conceptType); err != nil {
		writeJSONError(w, err.Error(), http.StatusBadRequest, uuid)
		return
	}

	// Delete the concept
	affected, err := h.ConceptsService.Delete(uuid, transID)
	if errors.Is(err, ErrNotFound) {
		writeJSONError(w, fmt.Sprintf("Concept with prefUUID %s not found in db.", uuid), http.StatusNotFound, uuid)
		return
	}
	if errors.Is(err, ErrDeleteRelated) {
		writeJSONError(w, fmt.Sprintf("Concept with prefUUID %s is referenced by %q, remove these before deleting.", uuid, affected), http.StatusBadRequest, affected...)
		return
	}
	if errors.Is(err, ErrDeleteSource) {
		writeJSONError(w, fmt.Sprintf("Concept with UUID %s is a source concept, the canonical concept %q should be deleted instead.", uuid, affected[0]), http.StatusBadRequest, affected...)
		return
	}
	if err != nil {
		writeJSONError(w, err.Error(), http.StatusServiceUnavailable, uuid)
		return
	}

	resp := struct {
		UUIDS []string `json:"uuids"`
	}{affected}

	enc := json.NewEncoder(w)
	if err := enc.Encode(resp); err != nil {
		writeJSONError(w, err.Error(), http.StatusInternalServerError, uuid)
		return
	}
}

type errorResponse struct {
	Message string   `json:"message,omitempty"`
	UUIDs   []string `json:"uuids,omitempty"`
}

func writeJSONError(w http.ResponseWriter, errorMsg string, statusCode int, uuids ...string) {
	w.WriteHeader(statusCode)
	errorResp := errorResponse{errorMsg, uuids}
	enc := json.NewEncoder(w)
	if err := enc.Encode(errorResp); err != nil {
		return
	}
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
