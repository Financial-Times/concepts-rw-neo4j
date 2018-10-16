package organisations

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
var conceptsDriver *OrganisationService

const (
	testTID                = "tid_1234test"
	appleTmeUUID           = "33df7971-bc95-3029-bfb9-1a92e377b971"
	appleSmartlogicUUID    = "1d3711e2-b766-11e8-adcb-da24cd01f044"
	appleFactsetUUID       = "43ed1b5c-6043-348f-ac35-045b42d3b947"
	appleParentUUID        = "b1047bed-f6f6-31c4-9aa9-199f48aeb5a1"
	invalidPayloadUUID     = "d0360165-3ea7-3506-af2a-9a3b1316a78c"
	altAppleSmartlogicUUID = "58f3aeac-8a33-4b9f-a208-14ead197b361"
	relatedUUID            = "41d2defe-ba55-11e8-ba49-da24cd01f044"
	supercededUUID         = "b85c99fc-ae26-44c8-aa35-542fec638aaa"
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
	conceptsDriver = NewOrganisationService(db)

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
	defer concepts.CleanTestDB(t, db, invalidPayloadUUID, appleParentUUID, appleFactsetUUID, appleTmeUUID, appleSmartlogicUUID, altAppleSmartlogicUUID)

	tests := []struct {
		testName            string
		filePathToWrite     string
		filePathToWriteFunc func(concept string, uuid string) (ret interface{}, err error)
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
			testName:        "FS Organisation with all fields is successful and can be read from DB",
			filePathToWrite: "./fixtures/write/apple_1fs.json",
			filePathToRead:  "./fixtures/read/apple_1fs.json",
			conceptUUID:     appleFactsetUUID,
			expectedError:   "",
			updatedConcepts: concepts.ConceptChanges{
				UpdatedIds: []string{appleFactsetUUID},
				ChangedRecords: []concepts.Event{
					{
						ConceptUUID:   appleFactsetUUID,
						ConceptType:   "PublicCompany",
						AggregateHash: "9355132498402894989",
						TransactionID: testTID,
						EventDetails: concepts.ConceptEvent{
							Type: concepts.UpdatedEvent,
						},
					},
				},
			},
		},
		{
			testName:        "TME and FS Concorded Organisation is successful and can be read from DB",
			filePathToWrite: "./fixtures/write/apple_1sl_1tme_1fs.json",
			filePathToRead:  "./fixtures/read/apple_1sl_1tme_1fs.json",
			conceptUUID:     altAppleSmartlogicUUID,
			expectedError:   "",
			updatedConcepts: concepts.ConceptChanges{
				UpdatedIds: []string{appleFactsetUUID, appleTmeUUID, altAppleSmartlogicUUID},
				ChangedRecords: []concepts.Event{
					{
						ConceptUUID:   altAppleSmartlogicUUID,
						ConceptType:   "PublicCompany",
						AggregateHash: "378578028895595540",
						TransactionID: testTID,
						EventDetails: concepts.ConceptEvent{
							Type: concepts.UpdatedEvent,
						},
					},
					{
						ConceptUUID:   appleTmeUUID,
						ConceptType:   "Organisation",
						AggregateHash: "378578028895595540",
						TransactionID: testTID,
						EventDetails: concepts.ConceptEvent{
							Type: concepts.UpdatedEvent,
						},
					},
					{
						ConceptUUID:   appleTmeUUID,
						ConceptType:   "Organisation",
						AggregateHash: "378578028895595540",
						TransactionID: testTID,
						EventDetails: concepts.ConcordanceEvent{
							Type:  concepts.AddedEvent,
							NewID: altAppleSmartlogicUUID,
							OldID: appleTmeUUID,
						},
					},
					{
						ConceptUUID:   appleFactsetUUID,
						ConceptType:   "PublicCompany",
						AggregateHash: "378578028895595540",
						TransactionID: testTID,
						EventDetails: concepts.ConceptEvent{
							Type: concepts.UpdatedEvent,
						},
					},
					{
						ConceptUUID:   appleFactsetUUID,
						ConceptType:   "PublicCompany",
						AggregateHash: "378578028895595540",
						TransactionID: testTID,
						EventDetails: concepts.ConcordanceEvent{
							Type:  concepts.AddedEvent,
							NewID: altAppleSmartlogicUUID,
							OldID: appleFactsetUUID,
						},
					},
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.testName, func(t *testing.T) {
			concepts.CleanTestDB(t, db, invalidPayloadUUID, appleParentUUID, appleFactsetUUID, appleTmeUUID, appleSmartlogicUUID, altAppleSmartlogicUUID)
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
		})
	}
}

func TestWriteService_HandlingConcordance(t *testing.T) {
	defer concepts.CleanTestDB(t, db, relatedUUID, supercededUUID, appleParentUUID, appleFactsetUUID, appleTmeUUID, appleSmartlogicUUID, altAppleSmartlogicUUID)
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
			testName:             "Re-writing the same payload produces one concept updated event",
			pathToSetUpConcept:   "./fixtures/write/apple_1sl_1tme.json",
			pathToUpdatedConcept: "./fixtures/write/apple_1sl_1tme.json",
			pathToReadConcept:    "./fixtures/read/apple_1sl_1tme.json",
			conceptUUID:          appleSmartlogicUUID,
			expectedError:        "",
			updatedConcepts: concepts.ConceptChanges{
				UpdatedIds: []string{appleTmeUUID, appleSmartlogicUUID},
				ChangedRecords: []concepts.Event{
					{
						ConceptUUID:   appleSmartlogicUUID,
						ConceptType:   "PublicCompany",
						AggregateHash: "8746192846972605183",
						TransactionID: testTID,
						EventDetails: concepts.ConceptEvent{
							Type: concepts.UpdatedEvent,
						},
					},
				},
			},
		},
		{
			testName:             "Adding an existing tme to a smartlogic produces one update concept and one concordance added event",
			pathToSetUpConcept:   "./fixtures/write/apple_1tme.json",
			pathToUpdatedConcept: "./fixtures/write/apple_1sl_1tme.json",
			pathToReadConcept:    "./fixtures/read/apple_1sl_1tme.json",
			conceptUUID:          appleSmartlogicUUID,
			expectedError:        "",
			updatedConcepts: concepts.ConceptChanges{
				UpdatedIds: []string{appleTmeUUID, appleSmartlogicUUID},
				ChangedRecords: []concepts.Event{
					{
						ConceptUUID:   appleSmartlogicUUID,
						ConceptType:   "PublicCompany",
						AggregateHash: "8746192846972605183",
						TransactionID: testTID,
						EventDetails: concepts.ConceptEvent{
							Type: concepts.UpdatedEvent,
						},
					},
					{
						ConceptUUID:   appleTmeUUID,
						ConceptType:   "Organisation",
						AggregateHash: "8746192846972605183",
						TransactionID: testTID,
						EventDetails: concepts.ConcordanceEvent{
							Type:  concepts.AddedEvent,
							NewID: appleSmartlogicUUID,
							OldID: appleTmeUUID,
						},
					},
				},
			},
		},
		{
			testName:             "Removing a source from a single concordance produces 1 updated and 1 concordance removed event",
			pathToSetUpConcept:   "./fixtures/write/apple_1sl_1tme.json",
			pathToUpdatedConcept: "./fixtures/write/apple_1sl.json",
			pathToReadConcept:    "./fixtures/read/apple_1sl.json",
			conceptUUID:          appleSmartlogicUUID,
			expectedError:        "",
			updatedConcepts: concepts.ConceptChanges{
				UpdatedIds: []string{appleSmartlogicUUID, appleTmeUUID},
				ChangedRecords: []concepts.Event{
					{
						ConceptUUID:   appleSmartlogicUUID,
						ConceptType:   "PublicCompany",
						AggregateHash: "10610334823062494641",
						TransactionID: testTID,
						EventDetails: concepts.ConceptEvent{
							Type: concepts.UpdatedEvent,
						},
					},
					{
						ConceptUUID:   appleTmeUUID,
						ConceptType:   "Organisation",
						AggregateHash: "10610334823062494641",
						TransactionID: testTID,
						EventDetails: concepts.ConcordanceEvent{
							Type:  concepts.RemovedEvent,
							NewID: appleTmeUUID,
							OldID: appleSmartlogicUUID,
						},
					},
				},
			},
		},
		{
			testName:             "Transferring a source from a single concordance produces 2 updated, 1 concordance removed and 2 concordance added event",
			pathToSetUpConcept:   "./fixtures/write/apple_1sl_1tme.json",
			pathToUpdatedConcept: "./fixtures/write/apple_1sl_1tme_1fs.json",
			pathToReadConcept:    "./fixtures/read/apple_1sl_1tme_1fs.json",
			conceptUUID:          altAppleSmartlogicUUID,
			expectedError:        "",
			updatedConcepts: concepts.ConceptChanges{
				UpdatedIds: []string{appleFactsetUUID, appleTmeUUID, altAppleSmartlogicUUID},
				ChangedRecords: []concepts.Event{
					{
						ConceptUUID:   altAppleSmartlogicUUID,
						ConceptType:   "PublicCompany",
						AggregateHash: "378578028895595540",
						TransactionID: testTID,
						EventDetails: concepts.ConceptEvent{
							Type: concepts.UpdatedEvent,
						},
					},
					{
						ConceptUUID:   appleFactsetUUID,
						ConceptType:   "PublicCompany",
						AggregateHash: "378578028895595540",
						TransactionID: testTID,
						EventDetails: concepts.ConcordanceEvent{
							Type:  concepts.AddedEvent,
							NewID: altAppleSmartlogicUUID,
							OldID: appleFactsetUUID,
						},
					},
					{
						ConceptUUID:   appleFactsetUUID,
						ConceptType:   "PublicCompany",
						AggregateHash: "378578028895595540",
						TransactionID: testTID,
						EventDetails: concepts.ConceptEvent{
							Type: concepts.UpdatedEvent,
						},
					},
					{
						ConceptUUID:   appleTmeUUID,
						ConceptType:   "Organisation",
						AggregateHash: "378578028895595540",
						TransactionID: testTID,
						EventDetails: concepts.ConcordanceEvent{
							Type:  concepts.RemovedEvent,
							NewID: appleTmeUUID,
							OldID: appleSmartlogicUUID,
						},
					},
					{
						ConceptUUID:   appleTmeUUID,
						ConceptType:   "Organisation",
						AggregateHash: "378578028895595540",
						TransactionID: testTID,
						EventDetails: concepts.ConcordanceEvent{
							Type:  concepts.AddedEvent,
							NewID: altAppleSmartlogicUUID,
							OldID: appleTmeUUID,
						},
					},
				},
			},
		},
		{
			testName:             "Trying to set an existing prefNode as a source results in error",
			pathToSetUpConcept:   "./fixtures/write/apple_1sl_1tme.json",
			pathToUpdatedConcept: "./fixtures/write/conflictedApple.json",
			pathToReadConcept:    "",
			conceptUUID:          altAppleSmartlogicUUID,
			expectedError:        "cannot currently process this record as it will break an existing concordance with prefUuid: " + appleSmartlogicUUID,
			updatedConcepts: concepts.ConceptChanges{
				UpdatedIds:     []string{},
				ChangedRecords: []concepts.Event{},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.testName, func(t *testing.T) {
			concepts.CleanTestDB(t, db, relatedUUID, supercededUUID, appleParentUUID, appleFactsetUUID, appleTmeUUID, appleSmartlogicUUID, altAppleSmartlogicUUID)
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
