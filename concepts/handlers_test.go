package concepts

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Financial-Times/cm-graph-ontology/transform"

	"github.com/Financial-Times/go-logger/v2"
	"github.com/Financial-Times/up-rw-app-api-go/rwapi"

	"github.com/gorilla/mux"
	"github.com/stretchr/testify/assert"
)

const knownUUID = "12345"

func TestDeleteHandler(t *testing.T) {
	assert := assert.New(t)
	tests := []struct {
		name        string
		req         *http.Request
		ds          ConceptServicer
		statusCode  int
		contentType string // Contents of the Content-Type header
		body        string
	}{
		{
			name: "IrregularPathSuccess",
			req:  newRequest("DELETE", fmt.Sprintf("/dummies/%s", knownUUID), t),
			ds: &mockConceptService{
				read: func(uuid string, transID string) (interface{}, bool, error) {
					return transform.OldAggregatedConcept{PrefUUID: knownUUID, Type: "Dummy"}, true, nil
				},
				delete: func(uuid string, transID string) error {
					return nil
				},
			},
			statusCode:  http.StatusNoContent,
			contentType: "",
			body:        "",
		},
		{
			name: "IrregularPathFailure",
			req:  newRequest("DELETE", fmt.Sprintf("/Dummy/%s", knownUUID), t),
			ds: &mockConceptService{
				read: func(uuid string, transID string) (interface{}, bool, error) {
					return transform.OldAggregatedConcept{PrefUUID: knownUUID, Type: "Dummy"}, true, nil
				},
				delete: func(uuid string, transID string) error {
					return nil
				},
			},
			statusCode:  http.StatusBadRequest,
			contentType: "",
			body:        errorMessage("concept type does not match path"),
		},
		{
			name: "RegularPathSuccess",
			req:  newRequest("DELETE", fmt.Sprintf("/locations/%s", knownUUID), t),
			ds: &mockConceptService{
				read: func(uuid string, transID string) (interface{}, bool, error) {
					return transform.OldAggregatedConcept{PrefUUID: knownUUID, Type: "Location"}, true, nil
				},
				delete: func(uuid string, transID string) error {
					return nil
				},
			},
			statusCode:  http.StatusNoContent,
			contentType: "",
			body:        "",
		},
		{
			name: "RegularPathFailure",
			req:  newRequest("DELETE", fmt.Sprintf("/Location/%s", knownUUID), t),
			ds: &mockConceptService{
				read: func(uuid string, transID string) (interface{}, bool, error) {
					return transform.OldAggregatedConcept{PrefUUID: knownUUID, Type: "Location"}, true, nil
				},
				delete: func(uuid string, transID string) error {
					return nil
				},
			},
			statusCode:  http.StatusBadRequest,
			contentType: "",
			body:        errorMessage("concept type does not match path"),
		},
		{
			name: "NotFound",
			req:  newRequest("DELETE", fmt.Sprintf("/dummies/%s", "99999"), t),
			ds: &mockConceptService{
				read: func(uuid string, transID string) (interface{}, bool, error) {
					return nil, false, nil
				},
				delete: func(uuid string, transID string) error {
					return ErrNotFound
				},
			},
			statusCode:  http.StatusNotFound,
			contentType: "",
			body:        "{\"message\":\"Concept with prefUUID 99999 not found in db.\"}",
		},
		{
			name: "ReadError",
			req:  newRequest("DELETE", fmt.Sprintf("/dummies/%s", knownUUID), t),
			ds: &mockConceptService{
				read: func(uuid string, transID string) (interface{}, bool, error) {
					return nil, false, errors.New("TEST failing to READ")
				},
				delete: func(uuid string, transID string) error {
					return nil
				},
			},
			statusCode:  http.StatusServiceUnavailable,
			contentType: "",
			body:        errorMessage("TEST failing to READ"),
		},
		{
			name: "DeleteError",
			req:  newRequest("DELETE", fmt.Sprintf("/dummies/%s", knownUUID), t),
			ds: &mockConceptService{
				read: func(uuid string, transID string) (interface{}, bool, error) {
					return transform.OldAggregatedConcept{PrefUUID: knownUUID, Type: "Dummy"}, true, nil
				},
				delete: func(uuid string, transID string) error {
					return errors.New("TEST failing to READ")
				},
			},
			statusCode:  http.StatusServiceUnavailable,
			contentType: "",
			body:        errorMessage("TEST failing to READ"),
		},
		{
			name: "BadConceptType",
			req:  newRequest("DELETE", fmt.Sprintf("/dummies/%s", knownUUID), t),
			ds: &mockConceptService{
				read: func(uuid string, transID string) (interface{}, bool, error) {
					return transform.OldAggregatedConcept{PrefUUID: knownUUID, Type: "not-dummy"}, true, nil
				},
				delete: func(uuid string, transID string) error {
					return nil
				},
			},
			statusCode:  http.StatusBadRequest,
			contentType: "",
			body:        errorMessage("concept type does not match path"),
		},
		{
			name: "DeleteRelatedErr",
			req:  newRequest("DELETE", fmt.Sprintf("/dummies/%s", knownUUID), t),
			ds: &mockConceptService{
				read: func(uuid string, transID string) (interface{}, bool, error) {
					return transform.OldAggregatedConcept{PrefUUID: knownUUID, Type: "Dummy"}, true, nil
				},
				delete: func(uuid string, transID string) error {
					return ErrDeleteRelated
				},
			},
			statusCode:  http.StatusBadRequest,
			contentType: "",
			body:        errorMessage("Concept with prefUUID " + knownUUID + " is referenced by other concepts or content, remove these before deleting."),
		},
		{
			name: "DeleteSourceErr",
			req:  newRequest("DELETE", fmt.Sprintf("/dummies/%s", knownUUID), t),
			ds: &mockConceptService{
				read: func(uuid string, transID string) (interface{}, bool, error) {
					return transform.OldAggregatedConcept{PrefUUID: knownUUID, Type: "Dummy"}, true, nil
				},
				delete: func(uuid string, transID string) error {
					return ErrDeleteSource
				},
			},
			statusCode:  http.StatusBadRequest,
			contentType: "",
			body:        errorMessage("Concept with UUID " + knownUUID + " is a source concept, only canonical concepts can be deleted."),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			r := mux.NewRouter()
			handler := ConceptsHandler{test.ds}
			handler.RegisterHandlers(r)
			rec := httptest.NewRecorder()
			r.ServeHTTP(rec, test.req)
			assert.Equal(test.statusCode, rec.Code, fmt.Sprintf("%s: Wrong response code, was %d, should be %d", test.name, rec.Code, test.statusCode))
			assert.Equal(test.body, rec.Body.String(), fmt.Sprintf("%s: Wrong body", test.name))
		})
	}
}

func TestPutHandler(t *testing.T) {
	assert := assert.New(t)
	tests := []struct {
		name        string
		req         *http.Request
		mockService ConceptServicer
		statusCode  int
		contentType string // Contents of the Content-Type header
		body        string
	}{
		{
			name: "IrregularPathSuccess",
			req:  newRequest("PUT", fmt.Sprintf("/dummies/%s", knownUUID), t),
			mockService: &mockConceptService{
				decodeJSON: func(decoder *json.Decoder) (interface{}, string, error) {
					return transform.OldAggregatedConcept{PrefUUID: knownUUID, Type: "Dummy"}, knownUUID, nil
				},
				write: func(thing interface{}, transID string) (interface{}, error) {
					return ConceptChanges{}, nil
				},
			},
			statusCode:  http.StatusOK,
			contentType: "",
			body:        "{\"events\":null,\"updatedIDs\":null}",
		},
		{
			name: "IrregularPathFailure",
			req:  newRequest("PUT", fmt.Sprintf("/Dummy/%s", knownUUID), t),
			mockService: &mockConceptService{
				decodeJSON: func(decoder *json.Decoder) (interface{}, string, error) {
					return transform.OldAggregatedConcept{PrefUUID: knownUUID, Type: "Dummy"}, knownUUID, nil
				},
				write: func(thing interface{}, transID string) (interface{}, error) {
					return ConceptChanges{}, nil
				},
			},
			statusCode:  http.StatusBadRequest,
			contentType: "",
			body:        errorMessage("concept type does not match path"),
		},
		{
			name: "RegularPathSuccess",
			req:  newRequest("PUT", fmt.Sprintf("/financial-instruments/%s", knownUUID), t),
			mockService: &mockConceptService{
				decodeJSON: func(decoder *json.Decoder) (interface{}, string, error) {
					return transform.OldAggregatedConcept{PrefUUID: knownUUID, Type: "FinancialInstrument"}, knownUUID, nil
				},
				write: func(thing interface{}, transID string) (interface{}, error) {
					return ConceptChanges{}, nil
				},
			},
			statusCode:  http.StatusOK,
			contentType: "",
			body:        "{\"events\":null,\"updatedIDs\":null}",
		},
		{
			name: "RegularPathFailure",
			req:  newRequest("PUT", fmt.Sprintf("/FinancialInstrument/%s", knownUUID), t),
			mockService: &mockConceptService{
				decodeJSON: func(decoder *json.Decoder) (interface{}, string, error) {
					return transform.OldAggregatedConcept{PrefUUID: knownUUID, Type: "FinancialInstrument"}, knownUUID, nil
				},
				write: func(thing interface{}, transID string) (interface{}, error) {
					return ConceptChanges{}, nil
				},
			},
			statusCode:  http.StatusBadRequest,
			contentType: "",
			body:        errorMessage("concept type does not match path"),
		},
		{
			name: "ParseError",
			req:  newRequest("PUT", fmt.Sprintf("/dummies/%s", knownUUID), t),
			mockService: &mockConceptService{
				decodeJSON: func(decoder *json.Decoder) (interface{}, string, error) {
					return nil, "", errors.New("TEST failing to DECODE")
				},
			},
			statusCode:  http.StatusBadRequest,
			contentType: "",
			body:        errorMessage("TEST failing to DECODE"),
		},
		{
			name: "UUIDMisMatch",
			req:  newRequest("PUT", fmt.Sprintf("/dummies/%s", "99999"), t),
			mockService: &mockConceptService{
				decodeJSON: func(decoder *json.Decoder) (interface{}, string, error) {
					return transform.OldAggregatedConcept{PrefUUID: knownUUID, Type: "Dummy"}, knownUUID, nil
				},
				write: func(thing interface{}, transID string) (interface{}, error) {
					return ConceptChanges{}, nil
				},
			},
			statusCode:  http.StatusBadRequest,
			contentType: "",
			body:        errorMessage("Uuids from payload and request, respectively, do not match: '12345' '99999'"),
		},
		{
			name: "WriteFailed",
			req:  newRequest("PUT", fmt.Sprintf("/dummies/%s", knownUUID), t),
			mockService: &mockConceptService{
				decodeJSON: func(decoder *json.Decoder) (interface{}, string, error) {
					return transform.OldAggregatedConcept{PrefUUID: knownUUID, Type: "Dummy"}, knownUUID, nil
				},
				write: func(thing interface{}, transID string) (interface{}, error) {
					return nil, errors.New("TEST failing to WRITE")
				},
			},
			statusCode:  http.StatusServiceUnavailable,
			contentType: "",
			body:        errorMessage("TEST failing to WRITE"),
		},
		{
			name: "WriteFailedDueToConflict",
			req:  newRequest("PUT", fmt.Sprintf("/dummies/%s", knownUUID), t),
			mockService: &mockConceptService{
				decodeJSON: func(decoder *json.Decoder) (interface{}, string, error) {
					return transform.OldAggregatedConcept{PrefUUID: knownUUID, Type: "Dummy"}, knownUUID, nil
				},
				write: func(thing interface{}, transID string) (interface{}, error) {
					return nil, rwapi.ConstraintOrTransactionError{}
				},
			},
			statusCode:  http.StatusConflict,
			contentType: "",
			body:        errorMessage(""),
		},
		{
			name: "BadConceptOrPath",
			req:  newRequest("PUT", fmt.Sprintf("/dummies/%s", knownUUID), t),
			mockService: &mockConceptService{
				decodeJSON: func(decoder *json.Decoder) (interface{}, string, error) {
					return transform.OldAggregatedConcept{PrefUUID: knownUUID, Type: "not-dummy"}, knownUUID, nil
				},
				write: func(thing interface{}, transID string) (interface{}, error) {
					return ConceptChanges{}, nil
				},
			},
			statusCode:  http.StatusBadRequest,
			contentType: "",
			body:        errorMessage("concept type does not match path"),
		},
	}

	for _, test := range tests {
		r := mux.NewRouter()
		handler := ConceptsHandler{test.mockService}
		handler.RegisterHandlers(r)
		rec := httptest.NewRecorder()
		r.ServeHTTP(rec, test.req)
		assert.Equal(test.statusCode, rec.Code, fmt.Sprintf("%s: Wrong response code, was %d, should be %d", test.name, rec.Code, test.statusCode))
		assert.Equal(test.body, rec.Body.String(), fmt.Sprintf("%s: Wrong body", test.name))
	}
}

func TestGetHandler(t *testing.T) {
	assert := assert.New(t)
	tests := []struct {
		name        string
		req         *http.Request
		ds          ConceptServicer
		statusCode  int
		contentType string // Contents of the Content-Type header
		body        string
	}{
		{
			name: "IrregularPathSuccess",
			req:  newRequest("GET", fmt.Sprintf("/dummies/%s", knownUUID), t),
			ds: &mockConceptService{
				read: func(uuid string, transID string) (interface{}, bool, error) {
					return transform.OldAggregatedConcept{PrefUUID: knownUUID, Type: "Dummy"}, true, nil
				},
			},
			statusCode:  http.StatusOK,
			contentType: "",
			body:        "{\"prefUUID\":\"12345\",\"type\":\"Dummy\"}\n",
		},
		{
			name: "IrregularPathFailure",
			req:  newRequest("GET", fmt.Sprintf("/Dummy/%s", knownUUID), t),
			ds: &mockConceptService{
				read: func(uuid string, transID string) (interface{}, bool, error) {
					return transform.OldAggregatedConcept{PrefUUID: knownUUID, Type: "Dummy"}, true, nil
				},
			},
			statusCode:  http.StatusBadRequest,
			contentType: "",
			body:        errorMessage("concept type does not match path"),
		},
		{
			name: "RegularPathSuccess",
			req:  newRequest("GET", fmt.Sprintf("/locations/%s", knownUUID), t),
			ds: &mockConceptService{
				read: func(uuid string, transID string) (interface{}, bool, error) {
					return transform.OldAggregatedConcept{PrefUUID: knownUUID, Type: "Location"}, true, nil
				},
			},
			statusCode:  http.StatusOK,
			contentType: "",
			body:        "{\"prefUUID\":\"12345\",\"type\":\"Location\"}\n",
		},
		{
			name: "RegularPathFailure",
			req:  newRequest("GET", fmt.Sprintf("/Location/%s", knownUUID), t),
			ds: &mockConceptService{
				read: func(uuid string, transID string) (interface{}, bool, error) {
					return transform.OldAggregatedConcept{PrefUUID: knownUUID, Type: "Location"}, true, nil
				},
			},
			statusCode:  http.StatusBadRequest,
			contentType: "",
			body:        errorMessage("concept type does not match path"),
		},
		{
			name: "NotFound",
			req:  newRequest("GET", fmt.Sprintf("/dummies/%s", "99999"), t),
			ds: &mockConceptService{
				read: func(uuid string, transID string) (interface{}, bool, error) {
					return nil, false, nil
				},
			},
			statusCode:  http.StatusNotFound,
			contentType: "",
			body:        "{\"message\":\"Concept with prefUUID 99999 not found in db.\"}",
		},
		{
			name: "ReadError",
			req:  newRequest("GET", fmt.Sprintf("/dummies/%s", knownUUID), t),
			ds: &mockConceptService{
				read: func(uuid string, transID string) (interface{}, bool, error) {
					return nil, false, errors.New("TEST failing to READ")
				},
			},
			statusCode:  http.StatusServiceUnavailable,
			contentType: "",
			body:        errorMessage("TEST failing to READ"),
		},
		{
			name: "BadConceptType",
			req:  newRequest("GET", fmt.Sprintf("/dummies/%s", knownUUID), t),
			ds: &mockConceptService{
				read: func(uuid string, transID string) (interface{}, bool, error) {
					return transform.OldAggregatedConcept{PrefUUID: knownUUID, Type: "not-dummy"}, true, nil
				},
			},
			statusCode:  http.StatusBadRequest,
			contentType: "",
			body:        errorMessage("concept type does not match path"),
		},
	}

	for _, test := range tests {
		r := mux.NewRouter()
		handler := ConceptsHandler{test.ds}
		handler.RegisterHandlers(r)
		rec := httptest.NewRecorder()
		r.ServeHTTP(rec, test.req)
		assert.Equal(test.statusCode, rec.Code, fmt.Sprintf("%s: Wrong response code, was %d, should be %d", test.name, rec.Code, test.statusCode))
		assert.Equal(test.body, rec.Body.String(), fmt.Sprintf("%s: Wrong body", test.name))
	}
}

func TestGtgHandler(t *testing.T) {
	assert := assert.New(t)
	tests := []struct {
		name        string
		req         *http.Request
		ds          ConceptServicer
		statusCode  int
		contentType string // Contents of the Content-Type header
		body        string
	}{
		{
			"Success",
			newRequest("GET", "/__gtg", t),
			&mockConceptService{
				check: func() error {
					return nil
				},
			},
			http.StatusOK,
			"",
			"OK",
		},
		{
			"GTGError",
			newRequest("GET", "/__gtg", t),
			&mockConceptService{
				check: func() error {
					return errors.New("TEST failing to CHECK")
				},
			},
			http.StatusServiceUnavailable,
			"",
			"TEST failing to CHECK",
		},
	}

	for _, test := range tests {
		r := mux.NewRouter()
		handler := ConceptsHandler{test.ds}
		log := logger.NewUPPLogger("handlers_test", "PANIC")
		sm := handler.RegisterAdminHandlers(r, log, "", "", "", true)
		rec := httptest.NewRecorder()
		sm.ServeHTTP(rec, test.req)
		assert.Equal(test.statusCode, rec.Code, fmt.Sprintf("%s: Wrong response code, was %d, should be %d", test.name, rec.Code, test.statusCode))
		assert.Equal(test.body, rec.Body.String(), fmt.Sprintf("%s: Wrong body", test.name))
	}
}

func newRequest(method, url string, t *testing.T) *http.Request {
	req, err := http.NewRequest(method, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	return req
}

func errorMessage(errMsg string) string {
	return fmt.Sprintf("{\"message\": \"%s\"}\n", errMsg)
}
