package concepts

import (
	"compress/gzip"
	"encoding/json"
	"fmt"
	"github.com/Financial-Times/transactionid-utils-go"
	"github.com/Financial-Times/up-rw-app-api-go/rwapi"
	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	"io"
	"net/http"
)

type ConceptsHandler struct {
	ConceptsService ConceptServicer
}

func (h *ConceptsHandler) RegisterHandlers(router *mux.Router, path string) *mux.Router {
	urlPath := fmt.Sprintf("/%s/{uuid}", path)
	rwHandler := handlers.MethodHandler{
		"GET": http.HandlerFunc(h.GetConcept),
		"PUT": http.HandlerFunc(h.PutConcept),
	}
	router.Handle(urlPath, rwHandler)
	return router
}

func (hh *ConceptsHandler) PutConcept(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	uuid := vars["uuid"]

	transID := transactionidutils.GetTransactionIDFromRequest(r)
	w.Header().Add("Content-Type", "application/json")
	w.Header().Set("X-Request-Id", transID)

	var body io.Reader = r.Body
	dec := json.NewDecoder(body)
	inst, docUUID, err := hh.ConceptsService.DecodeJSON(dec)

	if err != nil {
		writeJSONError(w, err.Error(), http.StatusBadRequest)
		return
	}

	if docUUID != uuid {
		writeJSONError(w, fmt.Sprintf("Uuids from payload and request, respectively, do not match: '%v' '%v'", docUUID, uuid), http.StatusBadRequest)
		return
	}

	updatedIds, err := hh.ConceptsService.Write(inst, transID)

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

	enc := json.NewEncoder(w)
	if err = enc.Encode(updatedIds); err != nil {
		writeJSONError(w, err.Error(), http.StatusInternalServerError)
		return
	} else {
		w.WriteHeader(http.StatusOK)
		return
	}
}

func (hh *ConceptsHandler) GetConcept(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	uuid := vars["uuid"]

	transID := transactionidutils.GetTransactionIDFromRequest(r)

	obj, found, err := hh.ConceptsService.Read(uuid, transID)

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
