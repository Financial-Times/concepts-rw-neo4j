package concepts

import (
	"fmt"
	"github.com/Financial-Times/go-logger"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gorilla/mux"
	"github.com/stretchr/testify/assert"
)

const knownUUID = "12345"

func init() {
	logger.InitDefaultLogger("handlers-test")
}

func TestPutHandler(t *testing.T) {
	assert := assert.New(t)
	tests := []struct {
		name        string
		req         *http.Request
		mockService map[string]ConceptServicer
		statusCode  int
		body        string
	}{
		{
			name: "Success",
			req:  newRequest("PUT", fmt.Sprintf("/dummies/%s", knownUUID), strings.NewReader(`{"prefUUID":"12345","type":"Dummy"}`)),
			mockService: buildServiceMap(&mockConceptService{
				uuid:        knownUUID,
				conceptType: "Dummy",
			}),
			statusCode: http.StatusOK,
			body:       "{\"events\":null,\"updatedIDs\":null}",
		},
		{
			name: "RegularPathSuccess",
			req:  newRequest("PUT", fmt.Sprintf("/financial-instruments/%s", knownUUID), strings.NewReader(`{"prefUUID":"12345","type":"Dummy"}`)),
			mockService: buildServiceMap(&mockConceptService{
				uuid:        knownUUID,
				conceptType: "FinancialInstrument",
			}),
			statusCode: http.StatusOK,
			body:       "{\"events\":null,\"updatedIDs\":null}",
		},
		{
			name: "ParseError",
			req:  newRequest("PUT", fmt.Sprintf("/dummies/%s", knownUUID), strings.NewReader("{\\}")),
			mockService: buildServiceMap(&mockConceptService{
				uuid:        knownUUID,
				conceptType: "Dummy",
			}),
			statusCode: http.StatusBadRequest,
			body:       errorMessage("invalid character '\\\\' looking for beginning of object key string"),
		},
		{
			name: "UUIDMisMatch",
			req:  newRequest("PUT", fmt.Sprintf("/dummies/%s", "99999"), strings.NewReader(`{"prefUUID":"12345","type":"Dummy"}`)),
			mockService: buildServiceMap(&mockConceptService{
				uuid:        knownUUID,
				conceptType: "Dummy",
			}),
			statusCode: http.StatusBadRequest,
			body:       errorMessage("Uuids from payload and request, respectively, do not match: '12345' '99999'"),
		},
		{
			name: "WriteFailed",
			req:  newRequest("PUT", fmt.Sprintf("/dummies/%s", knownUUID), strings.NewReader(`{"prefUUID":"12345","type":"Dummy"}`)),
			mockService: buildServiceMap(&mockConceptService{
				uuid:        knownUUID,
				conceptType: "Dummy",
				failWrite:   true,
			}),
			statusCode: http.StatusServiceUnavailable,
			body:       errorMessage("TEST failing to WRITE"),
		},
		{
			name: "WriteFailedDueToConflict",
			req:  newRequest("PUT", fmt.Sprintf("/dummies/%s", knownUUID), strings.NewReader(`{"prefUUID":"12345","type":"Dummy"}`)),
			mockService: buildServiceMap(&mockConceptService{
				uuid:         knownUUID,
				conceptType:  "Dummy",
				failConflict: true,
			}),
			statusCode: http.StatusConflict,
			body:       errorMessage(""),
		},
		{
			name: "BadConceptOrPath",
			req:  newRequest("PUT", fmt.Sprintf("/unknown/%s", knownUUID), strings.NewReader(`{"prefUUID":"12345","type":"Dummy"}`)),
			mockService: buildServiceMap(&mockConceptService{
				uuid:        knownUUID,
				conceptType: "Unknown",
			}),
			statusCode: http.StatusBadRequest,
			body:       errorMessage("concept type unknown is not currently supported"),
		},
	}

	for _, test := range tests {
		r := mux.NewRouter()
		handler := ConceptsHandler{test.mockService, nil}
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
		name       string
		req        *http.Request
		ds         map[string]ConceptServicer
		statusCode int
		body       string
	}{
		{
			name: "Success",
			req:  newRequest("GET", fmt.Sprintf("/dummies/%s", knownUUID), nil),
			ds: buildServiceMap(&mockConceptService{
				uuid:        knownUUID,
				conceptType: "Dummy",
			}),
			statusCode: http.StatusOK,
			body:       "{\"prefUUID\":\"12345\",\"type\":\"Dummy\"}\n",
		},
		{
			name: "NotFound",
			req:  newRequest("GET", fmt.Sprintf("/dummies/%s", "99999"), nil),
			ds: buildServiceMap(&mockConceptService{
				uuid:        knownUUID,
				conceptType: "Dummy",
			}),
			statusCode: http.StatusNotFound,
			body:       "{\"message\":\"Concept with prefUUID 99999 not found in db.\"}",
		},
		{
			name: "ReadError",
			req:  newRequest("GET", fmt.Sprintf("/brands/%s", knownUUID), nil),
			ds: buildServiceMap(&mockConceptService{
				uuid:        knownUUID,
				conceptType: "Brand",
				failRead:    true,
			}),
			statusCode: http.StatusServiceUnavailable,
			body:       errorMessage("TEST failing to READ"),
		},
		{
			name: "BadConceptOrPath",
			req:  newRequest("GET", fmt.Sprintf("/unknown/%s", knownUUID), nil),
			ds: buildServiceMap(&mockConceptService{
				uuid:        knownUUID,
				conceptType: "Unknown",
			}),
			statusCode: http.StatusBadRequest,
			body:       errorMessage("concept type unknown is not currently supported"),
		},
	}

	for _, test := range tests {
		r := mux.NewRouter()
		handler := ConceptsHandler{test.ds, nil}
		handler.RegisterHandlers(r)
		rec := httptest.NewRecorder()
		r.ServeHTTP(rec, test.req)
		assert.Equal(test.statusCode, rec.Code, fmt.Sprintf("%s: Wrong response code, was %d, should be %d", test.name, rec.Code, test.statusCode))
		assert.Equal(test.body, rec.Body.String(), fmt.Sprintf("%s: Wrong body", test.name))
	}
}

func newRequest(method, url string, body io.Reader) *http.Request {
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		panic(err)
	}
	return req
}

func errorMessage(errMsg string) string {
	return fmt.Sprintf("{\"message\": \"%s\"}\n", errMsg)
}

func buildServiceMap(service *mockConceptService) map[string]ConceptServicer {
	serviceMap := make(map[string]ConceptServicer)
	serviceMap[convertType(service.conceptType)] = service
	return serviceMap
}

func convertType(conceptType string) string {
	var conversion string
	switch conceptType {
	case "Dummy":
		conversion = "dummies"
	case "FinancialInstrument":
		conversion = "financial-instruments"
	case "Brand":
		conversion = "brands"
	}
	return conversion
}
