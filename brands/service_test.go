package brands

import (
	"fmt"
	"github.com/Financial-Times/concepts-rw-neo4j/concepts"
	"github.com/Financial-Times/go-logger"
	"github.com/Financial-Times/neo-utils-go/neoutils"
	"github.com/stretchr/testify/assert"
	"os"
	"reflect"
	"testing"
	"time"
)

//Reusable Neo4J connection
var db neoutils.NeoConnection

//Concept Service under test
var conceptsDriver *BrandService

const (
	testTID                              = "tid_1234test"
	martinSandbusFreeLunchTmeUUID        = "e35694c5-0cf2-3dbe-b20d-ff4e200d714c"
	martinSandbusFreeLunchSmartlogicUUID = "bb4a1ba8-b1c2-11e8-bfb1-da24cd01f044"
	martinSandbuTmeUUID                  = "4cc4424c-99f1-31b5-a810-5134847e6b02"
	freeLunchSmartlogicUUID              = "3678d538-b503-11e8-adcb-da24cd01f044"
	financialTimesUUID                   = "4771fdbb-2941-40b1-9c10-d0aec1a8d22c"
	invalidPayloadUUID                   = "d0360165-3ea7-3506-af2a-9a3b1316a78c"
	related1UUID                         = "d0f5eb30-b5b9-11e8-adcb-da24cd01f044"
	related2UUID                         = "bbf5d061-a3c9-4c95-850c-8e509e45091e"
)

func init() {
	// We are initialising a lot of constraints on an empty database therefore we need the database to be fit before
	// we run tests so initialising the service will create the constraints first
	logger.InitLogger("test-concepts-rw-neo4j", "info")

	conf := neoutils.DefaultConnectionConfig()
	conf.Transactional = false
	db, _ = neoutils.Connect(neoURL(), conf)
	if db == nil {
		panic("Cannot connect to Neo4J")
	}
	conceptsDriver = NewBrandService(db)

	duration := 2 * time.Second
	time.Sleep(duration)
}

func neoURL() string {
	url := os.Getenv("NEO4J_TEST_URL")
	if url == "" {
		url = "http://localhost:7474/db/data"
	}
	return url
}

func TestWriteService_EmptyDB(t *testing.T) {
	defer concepts.CleanTestDB(t, db, martinSandbusFreeLunchTmeUUID, financialTimesUUID, related1UUID, related2UUID, martinSandbusFreeLunchSmartlogicUUID)

	tests := []struct {
		testName            string
		filePathToWriteFunc func(concept string, uuid string) (ret interface{}, err error)
		filePathToWrite     string
		filePathToRead      string
		conceptUUID         string
		expectedError       string
		updatedConcepts     concepts.ConceptChanges
	}{
		{
			testName:            "Put payload with no prefLabel results in error",
			filePathToWriteFunc: concepts.NewMissingPrefLabel,
			conceptUUID:         invalidPayloadUUID,
			expectedError:       "invalid request, no prefLabel has been supplied",
			updatedConcepts:     concepts.ConceptChanges{},
		},
		{
			testName:            "Put payload with no type results in error",
			filePathToWriteFunc: concepts.NewMissingType,
			conceptUUID:         invalidPayloadUUID,
			expectedError:       "invalid request, no type has been supplied",
			updatedConcepts:     concepts.ConceptChanges{},
		},
		{
			testName:            "Put payload with invalid type results in error",
			filePathToWriteFunc: concepts.NewInvalidType,
			conceptUUID:         invalidPayloadUUID,
			expectedError:       "invalid request, invalid type has been supplied",
			updatedConcepts:     concepts.ConceptChanges{},
		},
		{
			testName:            "Put payload with no source representations results in error",
			filePathToWriteFunc: concepts.NewMissingSources,
			conceptUUID:         invalidPayloadUUID,
			expectedError:       "invalid request, no sourceRepresentation has been supplied",
			updatedConcepts:     concepts.ConceptChanges{},
		},
		{
			testName:            "Put payload with no source representation type results in error",
			filePathToWriteFunc: concepts.NewMissingSourceType,
			conceptUUID:         invalidPayloadUUID,
			expectedError:       "invalid request, no sourceRepresentation type has been supplied",
			updatedConcepts:     concepts.ConceptChanges{},
		},
		{
			testName:            "Put payload with invalid source representation type results in error",
			filePathToWriteFunc: concepts.NewInvalidSourceType,
			conceptUUID:         invalidPayloadUUID,
			expectedError:       "invalid request, invalid sourceRepresentation type has been supplied",
			updatedConcepts:     concepts.ConceptChanges{},
		},
		{
			testName:            "Put payload with no source representation authority value results in error",
			filePathToWriteFunc: concepts.NewMissingSourceAuthValue,
			conceptUUID:         invalidPayloadUUID,
			expectedError:       "invalid request, no sourceRepresentation.authorityValue has been supplied",
			updatedConcepts:     concepts.ConceptChanges{},
		},
		{
			testName:            "Put payload with no source representation authority results in error",
			filePathToWriteFunc: concepts.NewMissingSourceAuth,
			conceptUUID:         invalidPayloadUUID,
			expectedError:       "invalid request, no sourceRepresentation.authority has been supplied",
			updatedConcepts:     concepts.ConceptChanges{},
		},
		{
			testName:            "Put payload with no source representation prefLabel results in error",
			filePathToWriteFunc: concepts.NewMissingSourcePrefLabel,
			conceptUUID:         invalidPayloadUUID,
			expectedError:       "invalid request, no sourceRepresentation prefLabel has been supplied",
			updatedConcepts:     concepts.ConceptChanges{},
		},
		{
			testName:        "Simple TME brand is successful and can be read from DB",
			filePathToWrite: "./fixtures/write/martinSandbusFreeLunch_1tme.json",
			filePathToRead:  "./fixtures/read/martinSandbusFreeLunch_1tme.json",
			conceptUUID:     martinSandbusFreeLunchTmeUUID,
			expectedError:   "",
			updatedConcepts: concepts.ConceptChanges{
				UpdatedIds: []string{martinSandbusFreeLunchTmeUUID},
				ChangedRecords: []concepts.Event{
					{
						ConceptUUID:   martinSandbusFreeLunchTmeUUID,
						ConceptType:   "Brand",
						AggregateHash: "4682430659022430089",
						TransactionID: testTID,
						EventDetails: concepts.ConceptEvent{
							Type: concepts.UpdatedEvent,
						},
					},
				},
			},
		},
		{
			testName:        "Concorded brand is successful and can be read from DB",
			filePathToWrite: "./fixtures/write/martinSandbusFreeLunch_1sl_1tme.json",
			filePathToRead:  "./fixtures/read/martinSandbusFreeLunch_1sl_1tme.json",
			conceptUUID:     martinSandbusFreeLunchSmartlogicUUID,
			expectedError:   "",
			updatedConcepts: concepts.ConceptChanges{
				UpdatedIds: []string{martinSandbusFreeLunchTmeUUID, martinSandbusFreeLunchSmartlogicUUID},
				ChangedRecords: []concepts.Event{
					{
						ConceptUUID:   martinSandbusFreeLunchSmartlogicUUID,
						ConceptType:   "Brand",
						AggregateHash: "12665582270749871358",
						TransactionID: testTID,
						EventDetails: concepts.ConceptEvent{
							Type: concepts.UpdatedEvent,
						},
					},
					{
						ConceptUUID:   martinSandbusFreeLunchTmeUUID,
						ConceptType:   "Brand",
						AggregateHash: "12665582270749871358",
						TransactionID: testTID,
						EventDetails: concepts.ConceptEvent{
							Type: concepts.UpdatedEvent,
						},
					},
					{
						ConceptUUID:   martinSandbusFreeLunchTmeUUID,
						ConceptType:   "Brand",
						AggregateHash: "12665582270749871358",
						TransactionID: testTID,
						EventDetails: concepts.ConcordanceEvent{
							Type:  concepts.AddedEvent,
							NewID: martinSandbusFreeLunchSmartlogicUUID,
							OldID: martinSandbusFreeLunchTmeUUID,
						},
					},
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.testName, func(t *testing.T) {
			concepts.CleanTestDB(t, db, martinSandbusFreeLunchTmeUUID, financialTimesUUID, related1UUID, related2UUID, martinSandbusFreeLunchSmartlogicUUID)

			if test.filePathToWriteFunc != nil {
				concepts.RunWriteFailServiceTest(t,
					test.testName,
					conceptsDriver,
					testTID,
					"Brand",
					test.conceptUUID,
					test.expectedError,
					test.filePathToWriteFunc)
				return
			}

			write, _, err := concepts.ReadFileAndDecode(t, test.filePathToWrite)
			assert.NoError(t, err)

			output, err := conceptsDriver.Write(write, testTID)
			if test.expectedError != "" {
				assert.Equal(t, test.expectedError, err.Error(), fmt.Sprintf("test %s failed: actual error received differs from expected", test.testName))
				return
			}
			assert.NoError(t, err)

			changes := output.(concepts.ConceptChanges)

			assert.Equalf(t, test.updatedConcepts.UpdatedIds, changes.UpdatedIds, fmt.Sprintf("test %s failed: actual updatedID list differs from expected", test.testName))
			assert.Equalf(t, len(test.updatedConcepts.ChangedRecords), len(changes.ChangedRecords), fmt.Sprintf("test %s failed: recieved %d change events but expected %d", test.testName, len(test.updatedConcepts.ChangedRecords), len(changes.ChangedRecords)))
			assert.True(t, concepts.ChangedRecordsAreEqual(test.updatedConcepts.ChangedRecords, changes.ChangedRecords), fmt.Sprintf("test %s failed: actual change records differ from expected", test.testName))
			fmt.Printf("Expected hash is: %s; actual hash is %s\n", test.updatedConcepts.ChangedRecords[0].AggregateHash, changes.ChangedRecords[0].AggregateHash)

			actualConcept, exists, err := conceptsDriver.Read(test.conceptUUID, testTID)
			assert.NoError(t, err, fmt.Sprintf("test %s failed: there was an error reading the concept from the DB", test.testName))
			assert.True(t, exists, fmt.Sprintf("test %s failed: written concept could not be found in DB", test.testName))

			read, _, err := concepts.ReadFileAndDecode(t, test.filePathToRead)
			assert.NoError(t, err)

			expectedConcept := read.(concepts.AggregatedConcept)

			assert.True(t, reflect.DeepEqual(expectedConcept, actualConcept), fmt.Sprintf("test %s failed: concept read from DB does not match expected", test.testName))
			fmt.Printf("expected concept is %v\n", expectedConcept)
			fmt.Printf("  actual concept is %v\n", actualConcept)
		})
	}
}

func TestWriteService_HandlingConcordance(t *testing.T) {
	defer concepts.CleanTestDB(t, db, financialTimesUUID, martinSandbusFreeLunchTmeUUID, martinSandbuTmeUUID, related1UUID, related2UUID, freeLunchSmartlogicUUID, martinSandbusFreeLunchSmartlogicUUID)
	tests := []struct {
		testName             string
		pathToSetUpConcept   string
		pathToUpdatedConcept string
		pathToReadConcept    string
		conceptUUID          string
		expectedError        string
		updatedConcepts      concepts.ConceptChanges
	}{
		{
			testName:             "Adding an existing tme to a smartlogic produces one concorded concept and two events",
			pathToSetUpConcept:   "./fixtures/write/martinSandbusFreeLunch_1tme.json",
			pathToUpdatedConcept: "./fixtures/write/martinSandbusFreeLunch_1sl_1tme.json",
			pathToReadConcept:    "./fixtures/read/martinSandbusFreeLunch_1sl_1tme.json",
			conceptUUID:          martinSandbusFreeLunchSmartlogicUUID,
			expectedError:        "",
			updatedConcepts: concepts.ConceptChanges{
				UpdatedIds: []string{martinSandbusFreeLunchTmeUUID, martinSandbusFreeLunchSmartlogicUUID},
				ChangedRecords: []concepts.Event{
					{
						ConceptUUID:   martinSandbusFreeLunchSmartlogicUUID,
						ConceptType:   "Brand",
						AggregateHash: "12665582270749871358",
						TransactionID: testTID,
						EventDetails: concepts.ConceptEvent{
							Type: concepts.UpdatedEvent,
						},
					},
					{
						ConceptUUID:   martinSandbusFreeLunchTmeUUID,
						ConceptType:   "Brand",
						AggregateHash: "12665582270749871358",
						TransactionID: testTID,
						EventDetails: concepts.ConcordanceEvent{
							Type:  concepts.AddedEvent,
							NewID: martinSandbusFreeLunchSmartlogicUUID,
							OldID: martinSandbusFreeLunchTmeUUID,
						},
					},
				},
			},
		},
		{
			testName:             "Removing a source from a single concordance produces 1 updated and 1 concordance removed event",
			pathToSetUpConcept:   "./fixtures/write/martinSandbusFreeLunch_1sl_1tme.json",
			pathToUpdatedConcept: "./fixtures/write/martinSandbusFreeLunch_1sl.json",
			pathToReadConcept:    "./fixtures/read/martinSandbusFreeLunch_1sl.json",
			conceptUUID:          martinSandbusFreeLunchSmartlogicUUID,
			expectedError:        "",
			updatedConcepts: concepts.ConceptChanges{
				UpdatedIds: []string{martinSandbusFreeLunchSmartlogicUUID, martinSandbusFreeLunchTmeUUID},
				ChangedRecords: []concepts.Event{
					{
						ConceptUUID:   martinSandbusFreeLunchSmartlogicUUID,
						ConceptType:   "Brand",
						AggregateHash: "17351082195790991565",
						TransactionID: testTID,
						EventDetails: concepts.ConceptEvent{
							Type: concepts.UpdatedEvent,
						},
					},
					{
						ConceptUUID:   martinSandbusFreeLunchTmeUUID,
						ConceptType:   "Brand",
						AggregateHash: "17351082195790991565",
						TransactionID: testTID,
						EventDetails: concepts.ConcordanceEvent{
							Type:  concepts.RemovedEvent,
							NewID: martinSandbusFreeLunchTmeUUID,
							OldID: martinSandbusFreeLunchSmartlogicUUID,
						},
					},
				},
			},
		},
		{
			testName:             "Transferring a source from a single concordance produces 1 updated and 1 concordance transferred event",
			pathToSetUpConcept:   "./fixtures/write/martinSandbu_1sl_1tme.json",
			pathToUpdatedConcept: "./fixtures/write/martinSandbusFreeLunch_1sl_2tme.json",
			pathToReadConcept:    "./fixtures/read/martinSandbusFreeLunch_1sl_2tme.json",
			conceptUUID:          martinSandbusFreeLunchSmartlogicUUID,
			expectedError:        "",
			updatedConcepts: concepts.ConceptChanges{
				UpdatedIds: []string{martinSandbusFreeLunchTmeUUID, martinSandbuTmeUUID, martinSandbusFreeLunchSmartlogicUUID},
				ChangedRecords: []concepts.Event{
					{
						ConceptUUID:   martinSandbusFreeLunchSmartlogicUUID,
						ConceptType:   "Brand",
						AggregateHash: "12331261255214835180",
						TransactionID: testTID,
						EventDetails: concepts.ConceptEvent{
							Type: concepts.UpdatedEvent,
						},
					},
					{
						ConceptUUID:   martinSandbusFreeLunchTmeUUID,
						ConceptType:   "Brand",
						AggregateHash: "12331261255214835180",
						TransactionID: testTID,
						EventDetails: concepts.ConcordanceEvent{
							Type:  concepts.AddedEvent,
							NewID: martinSandbusFreeLunchSmartlogicUUID,
							OldID: martinSandbusFreeLunchTmeUUID,
						},
					},
					{
						ConceptUUID:   martinSandbusFreeLunchTmeUUID,
						ConceptType:   "Brand",
						AggregateHash: "12331261255214835180",
						TransactionID: testTID,
						EventDetails: concepts.ConceptEvent{
							Type: concepts.UpdatedEvent,
						},
					},
					{
						ConceptUUID:   martinSandbuTmeUUID,
						ConceptType:   "Person",
						AggregateHash: "12331261255214835180",
						TransactionID: testTID,
						EventDetails: concepts.ConcordanceEvent{
							Type:  concepts.RemovedEvent,
							NewID: martinSandbuTmeUUID,
							OldID: freeLunchSmartlogicUUID,
						},
					},
					{
						ConceptUUID:   martinSandbuTmeUUID,
						ConceptType:   "Person",
						AggregateHash: "12331261255214835180",
						TransactionID: testTID,
						EventDetails: concepts.ConcordanceEvent{
							Type:  concepts.AddedEvent,
							NewID: martinSandbusFreeLunchSmartlogicUUID,
							OldID: martinSandbuTmeUUID,
						},
					},
				},
			},
		},
		{
			testName:             "Trying to set an existing prefNode as a source results in error",
			pathToSetUpConcept:   "./fixtures/write/martinSandbu_1sl_1tme.json",
			pathToUpdatedConcept: "./fixtures/write/conflictedMartinSandbusFreeLunch.json",
			pathToReadConcept:    "",
			conceptUUID:          martinSandbusFreeLunchSmartlogicUUID,
			expectedError:        "cannot currently process this record as it will break an existing concordance with prefUuid: " + freeLunchSmartlogicUUID,
			updatedConcepts: concepts.ConceptChanges{
				UpdatedIds:     []string{},
				ChangedRecords: []concepts.Event{},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.testName, func(t *testing.T) {
			concepts.CleanTestDB(t, db, financialTimesUUID, martinSandbusFreeLunchTmeUUID, martinSandbuTmeUUID, related1UUID, related2UUID, freeLunchSmartlogicUUID, martinSandbusFreeLunchSmartlogicUUID)
			write, _, err := concepts.ReadFileAndDecode(t, test.pathToSetUpConcept)
			assert.NoError(t, err)

			_, err = conceptsDriver.Write(write, testTID)
			assert.NoError(t, err)

			write, _, err = concepts.ReadFileAndDecode(t, test.pathToUpdatedConcept)
			assert.NoError(t, err)

			output, err := conceptsDriver.Write(write, testTID)
			if test.expectedError != "" {
				assert.Equal(t, test.expectedError, err.Error(), fmt.Sprintf("test %s failed: actual error received differs from expected", test.testName))
				return
			}
			assert.NoError(t, err)

			changes := output.(concepts.ConceptChanges)

			assert.Equalf(t, test.updatedConcepts.UpdatedIds, changes.UpdatedIds, fmt.Sprintf("test %s failed: actual updatedID list differs from expected", test.testName))
			assert.Equalf(t, len(test.updatedConcepts.ChangedRecords), len(changes.ChangedRecords), fmt.Sprintf("test %s failed: recieved %d change events but expected %d", test.testName, len(test.updatedConcepts.ChangedRecords), len(changes.ChangedRecords)))
			assert.True(t, concepts.ChangedRecordsAreEqual(test.updatedConcepts.ChangedRecords, changes.ChangedRecords), fmt.Sprintf("test %s failed: actual change records differ from expected", test.testName))
			fmt.Printf("Expected hash is: %s; actual hash is %s\n", test.updatedConcepts.ChangedRecords[0].AggregateHash, changes.ChangedRecords[0].AggregateHash)

			actualConcept, exists, err := conceptsDriver.Read(test.conceptUUID, testTID)
			assert.NoError(t, err, fmt.Sprintf("test %s failed: there was an error reading the concept from the DB", test.testName))
			assert.True(t, exists, fmt.Sprintf("test %s failed: written concept could not be found in DB", test.testName))

			read, _, err := concepts.ReadFileAndDecode(t, test.pathToReadConcept)
			assert.NoError(t, err)

			expectedConcept := read.(concepts.AggregatedConcept)

			assert.True(t, reflect.DeepEqual(expectedConcept, actualConcept), fmt.Sprintf("test %s failed: concept read from DB does not match expected", test.testName))
			fmt.Printf("expected concept is %v\n", expectedConcept)
			fmt.Printf("  actual concept is %v\n", actualConcept)
		})
	}
}
