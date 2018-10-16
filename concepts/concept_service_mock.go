package concepts

import (
	"errors"
	"github.com/Financial-Times/up-rw-app-api-go/rwapi"
	"github.com/stretchr/testify/mock"
)

type mockConceptService struct {
	mock.Mock
	uuid         string
	conceptType  string
	transID      string
	uuidList     []string
	failWrite    bool
	failRead     bool
	failConflict bool
}

func (dS *mockConceptService) Write(thing interface{}, transID string) (interface{}, error) {
	var mockList ConceptChanges
	if dS.failWrite {
		return mockList, errors.New("TEST failing to WRITE")
	}
	if dS.failConflict {
		return mockList, rwapi.ConstraintOrTransactionError{}
	}
	if len(dS.uuidList) > 0 {
		mockList.UpdatedIds = dS.uuidList
	}
	dS.transID = transID
	return mockList, nil
}

func (dS *mockConceptService) Read(uuid string, transID string) (interface{}, bool, error) {
	if dS.failRead {
		return nil, false, errors.New("TEST failing to READ")
	}
	if uuid == dS.uuid {
		return AggregatedConcept{
			PrefUUID: dS.uuid,
			Type:     dS.conceptType,
		}, true, nil
	}
	dS.transID = transID
	return nil, false, nil
}
