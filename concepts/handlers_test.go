package concepts

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Financial-Times/neo-utils-go/neoutils"
	"github.com/gorilla/mux"
	"github.com/jmcvetta/neoism"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

const knownUUID = "12345"

func TestPutHandler(t *testing.T) {
	assert := assert.New(t)
	tests := []struct {
		name        string
		req         *http.Request
		ds          ConceptServicer
		statusCode  int
		contentType string // Contents of the Content-Type header
		body        string
	}{
		{"Success", newRequest("PUT", fmt.Sprintf("/dummies/%s", knownUUID)), mockConceptService{uuid: knownUUID}, http.StatusOK, "", "{\"UpdatedIds\":null}\n"},
		{"ParseError", newRequest("PUT", fmt.Sprintf("/dummies/%s", knownUUID)), mockConceptService{uuid: knownUUID, failParse: true}, http.StatusBadRequest, "", errorMessage("TEST failing to DECODE")},
		{"UUIDMisMatch", newRequest("PUT", fmt.Sprintf("/dummies/%s", "99999")), mockConceptService{uuid: knownUUID}, http.StatusBadRequest, "", errorMessage("Uuids from payload and request, respectively, do not match: '12345' '99999'")},
		{"WriteFailed", newRequest("PUT", fmt.Sprintf("/dummies/%s", knownUUID)), mockConceptService{uuid: knownUUID, failWrite: true}, http.StatusServiceUnavailable, "", errorMessage("TEST failing to WRITE")},
		{"WriteFailedDueToConflict", newRequest("PUT", fmt.Sprintf("/dummies/%s", knownUUID)), mockConceptService{uuid: knownUUID, failConflict: true}, http.StatusConflict, "", errorMessage("Neo4j ConstraintViolation TEST failing to WRITE due to CONFLICT")},
	}

	for _, test := range tests {
		r := mux.NewRouter()
		handler := ConceptsHandler{test.ds}
		handler.RegisterHandlers(r, "dummies")
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
		{"Success", newRequest("GET", fmt.Sprintf("/dummies/%s", knownUUID)), mockConceptService{uuid: knownUUID}, http.StatusOK, "", "{}\n"},
		{"NotFound", newRequest("GET", fmt.Sprintf("/dummies/%s", "99999")), mockConceptService{uuid: knownUUID}, http.StatusNotFound, "", "{\"message\":\"Concept with prefUUID 99999 not found in db.\"}"},
		{"ReadError", newRequest("GET", fmt.Sprintf("/dummies/%s", knownUUID)), mockConceptService{uuid: knownUUID, failRead: true}, http.StatusServiceUnavailable, "", errorMessage("TEST failing to READ")},
	}

	for _, test := range tests {
		r := mux.NewRouter()
		handler := ConceptsHandler{test.ds}
		handler.RegisterHandlers(r, "dummies")
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
		{"Success", newRequest("GET", "/__gtg"), mockConceptService{failCheck: false}, http.StatusOK, "", ""},
		{"GTGError", newRequest("GET", "/__gtg"), mockConceptService{failCheck: true}, http.StatusServiceUnavailable, "", "Connection to Neo4j healthcheck failed"},
	}

	for _, test := range tests {
		r := mux.NewRouter()
		handler := ConceptsHandler{test.ds}
		handler.RegisterAdminHandlers(r, "", "", "", true)
		rec := httptest.NewRecorder()
		r.ServeHTTP(rec, test.req)
		assert.Equal(test.statusCode, rec.Code, fmt.Sprintf("%s: Wrong response code, was %d, should be %d", test.name, rec.Code, test.statusCode))
		assert.Equal(test.body, rec.Body.String(), fmt.Sprintf("%s: Wrong body", test.name))
	}
}

func newRequest(method, url string) *http.Request {
	req, err := http.NewRequest(method, url, nil)
	if err != nil {
		panic(err)
	}
	return req
}

func errorMessage(errMsg string) string {
	return fmt.Sprintf("{\"message\": \"%s\"}\n", errMsg)
}

type mockConceptService struct {
	mock.Mock
	uuid         string
	transId      string
	uuidList     []string
	failParse    bool
	failWrite    bool
	failRead     bool
	failDelete   bool
	failCount    bool
	failConflict bool
	failCheck    bool
}

type mockServiceData struct {
}

func (dS mockConceptService) Write(thing interface{}, transId string) (interface{}, error) {
	mockList := UpdatedConcepts{}
	if dS.failWrite {
		return mockList, errors.New("TEST failing to WRITE")
	}
	if dS.failConflict {
		return mockList, neoutils.NewConstraintViolationError("TEST failing to WRITE due to CONFLICT", &neoism.NeoError{})
	}
	if len(dS.uuidList) > 0 {
		mockList.UpdatedIds = dS.uuidList
	}
	dS.transId = transId
	return mockList, nil
}

func (dS mockConceptService) Read(uuid string, transId string) (thing interface{}, found bool, err error) {
	if dS.failRead {
		return nil, false, errors.New("TEST failing to READ")
	}
	if uuid == dS.uuid {
		return mockServiceData{}, true, nil
	}
	dS.transId = transId
	return nil, false, nil
}

func (dS mockConceptService) DecodeJSON(*json.Decoder) (thing interface{}, identity string, err error) {
	if dS.failParse {
		return "", "", errors.New("TEST failing to DECODE")
	}
	return mockServiceData{}, dS.uuid, nil
}

func (dS mockConceptService) Check() error {
	if dS.failCheck {
		return errors.New("TEST failing to CHECK")
	}
	return nil
}

func (dS mockConceptService) Initialise() error {
	return nil
}

//type dummyServiceData struct {
//}
//
//func (dS dummyService) Write(thing interface{}, transId string) (interface{}, error) {
//	mockList := UpdatedConcepts{}
//	if dS.failWrite {
//		return mockList, errors.New("TEST failing to WRITE")
//	}
//	if dS.failConflict {
//		return mockList, neoutils.NewConstraintViolationError("TEST failing to WRITE due to CONFLICT", &neoism.NeoError{})
//	}
//	if len(dS.uuidList) > 0 {
//		mockList.UpdatedIds = dS.uuidList
//	}
//	dS.transId = transId
//	return mockList, nil
//}
//
//func (dS dummyService) Read(uuid string, transId string) (thing interface{}, found bool, err error) {
//	if dS.failRead {
//		return nil, false, errors.New("TEST failing to READ")
//	}
//	if uuid == dS.uuid {
//		return dummyServiceData{}, true, nil
//	}
//	dS.transId = transId
//	return nil, false, nil
//}
//
//func (dS dummyService) DecodeJSON(*json.Decoder) (thing interface{}, identity string, err error) {
//	if dS.failParse {
//		return "", "", errors.New("TEST failing to DECODE")
//	}
//	return dummyServiceData{}, dS.uuid, nil
//}
//
//func (dS dummyService) Check() error {
//	if dS.failCheck {
//		return errors.New("TEST failing to CHECK")
//	}
//	return nil
//}
//
//func (dS dummyService) Initialise() error {
//	return nil
//}

func healthHandler(http.ResponseWriter, *http.Request) {
}