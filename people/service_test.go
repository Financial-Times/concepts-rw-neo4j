package people

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
var conceptsDriver *PeopleService

const (
	testTID                      = "tid_1234test"
	barackObamaSmartlogicUUID    = "3ca8899c-ba69-11e8-ba49-da24cd01f044"
	barackObamaTMEUUID           = "57d79ccb-8804-3651-9144-8fc5fb4eacd9"
	barackObamaAuthorUUID        = "208edf7d-d4eb-329c-a293-560713d75250"
	barackObamaFactsetUUID       = "2d3a0bfb-80ff-395d-8b26-afaeb353d08c"
	barackObamaWikidataUUID      = "7eff22dd-7bf4-4e15-aea1-70b9592af499"
	invalidPayloadUUID           = "d0360165-3ea7-3506-af2a-9a3b1316a78c"
	barackObamaAltSmartlogicUUID = "96622afa-ba6f-11e8-989a-da24cd01f044"
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
	conceptsDriver = NewPeopleService(db)

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
	defer concepts.CleanTestDB(t, db, invalidPayloadUUID, barackObamaWikidataUUID, barackObamaTMEUUID, barackObamaFactsetUUID, barackObamaAuthorUUID, barackObamaSmartlogicUUID, barackObamaAltSmartlogicUUID)

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
			testName:        "Fully concorded person with all fields is successful and can be read from DB",
			filePathToWrite: "./fixtures/write/barackObama_1sl_1tme_1auth_1fs_1wiki.json",
			filePathToRead:  "./fixtures/read/barackObama_1sl_1tme_1auth_1fs_1wiki.json",
			conceptUUID:     barackObamaSmartlogicUUID,
			expectedError:   "",
			updatedConcepts: concepts.ConceptChanges{
				UpdatedIds: []string{barackObamaAuthorUUID, barackObamaTMEUUID, barackObamaWikidataUUID, barackObamaFactsetUUID, barackObamaSmartlogicUUID},
				ChangedRecords: []concepts.Event{
					{
						ConceptUUID:   barackObamaSmartlogicUUID,
						ConceptType:   "Person",
						AggregateHash: "6175303859824525987",
						TransactionID: testTID,
						EventDetails: concepts.ConceptEvent{
							Type: concepts.UpdatedEvent,
						},
					},
					{
						ConceptUUID:   barackObamaFactsetUUID,
						ConceptType:   "Person",
						AggregateHash: "6175303859824525987",
						TransactionID: testTID,
						EventDetails: concepts.ConceptEvent{
							Type: concepts.UpdatedEvent,
						},
					},
					{
						ConceptUUID:   barackObamaFactsetUUID,
						ConceptType:   "Person",
						AggregateHash: "6175303859824525987",
						TransactionID: testTID,
						EventDetails: concepts.ConcordanceEvent{
							Type:  concepts.AddedEvent,
							NewID: barackObamaSmartlogicUUID,
							OldID: barackObamaFactsetUUID,
						},
					},
					{
						ConceptUUID:   barackObamaWikidataUUID,
						ConceptType:   "Person",
						AggregateHash: "6175303859824525987",
						TransactionID: testTID,
						EventDetails: concepts.ConceptEvent{
							Type: concepts.UpdatedEvent,
						},
					},
					{
						ConceptUUID:   barackObamaWikidataUUID,
						ConceptType:   "Person",
						AggregateHash: "6175303859824525987",
						TransactionID: testTID,
						EventDetails: concepts.ConcordanceEvent{
							Type:  concepts.AddedEvent,
							NewID: barackObamaSmartlogicUUID,
							OldID: barackObamaWikidataUUID,
						},
					},
					{
						ConceptUUID:   barackObamaTMEUUID,
						ConceptType:   "Person",
						AggregateHash: "6175303859824525987",
						TransactionID: testTID,
						EventDetails: concepts.ConceptEvent{
							Type: concepts.UpdatedEvent,
						},
					},
					{
						ConceptUUID:   barackObamaTMEUUID,
						ConceptType:   "Person",
						AggregateHash: "6175303859824525987",
						TransactionID: testTID,
						EventDetails: concepts.ConcordanceEvent{
							Type:  concepts.AddedEvent,
							NewID: barackObamaSmartlogicUUID,
							OldID: barackObamaTMEUUID,
						},
					},
					{
						ConceptUUID:   barackObamaAuthorUUID,
						ConceptType:   "Person",
						AggregateHash: "6175303859824525987",
						TransactionID: testTID,
						EventDetails: concepts.ConceptEvent{
							Type: concepts.UpdatedEvent,
						},
					},
					{
						ConceptUUID:   barackObamaAuthorUUID,
						ConceptType:   "Person",
						AggregateHash: "6175303859824525987",
						TransactionID: testTID,
						EventDetails: concepts.ConcordanceEvent{
							Type:  concepts.AddedEvent,
							NewID: barackObamaSmartlogicUUID,
							OldID: barackObamaAuthorUUID,
						},
					},
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.testName, func(t *testing.T) {
			concepts.CleanTestDB(t, db, invalidPayloadUUID, barackObamaWikidataUUID, barackObamaTMEUUID, barackObamaFactsetUUID, barackObamaAuthorUUID, barackObamaSmartlogicUUID, barackObamaAltSmartlogicUUID)

			if test.filePathToWriteFunc != nil {
				concepts.RunWriteFailServiceTest(t,
					test.testName,
					conceptsDriver,
					testTID,
					"Person",
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
			fmt.Printf("expected concept is %v\n", expectedConcept)
			fmt.Printf("  actual concept is %v\n", actualConcept)
		})
	}
}

func TestWriteService_HandlingConcordance(t *testing.T) {
	defer concepts.CleanTestDB(t, db, barackObamaWikidataUUID, barackObamaTMEUUID, barackObamaFactsetUUID, barackObamaAuthorUUID, barackObamaSmartlogicUUID, barackObamaAltSmartlogicUUID)
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
			pathToSetUpConcept:   "./fixtures/write/barackObama_1sl_1tme.json",
			pathToUpdatedConcept: "./fixtures/write/barackObama_1sl_1tme.json",
			pathToReadConcept:    "./fixtures/read/barackObama_1sl_1tme.json",
			conceptUUID:          barackObamaAltSmartlogicUUID,
			expectedError:        "",
			updatedConcepts: concepts.ConceptChanges{
				UpdatedIds: []string{barackObamaTMEUUID, barackObamaAltSmartlogicUUID},
				ChangedRecords: []concepts.Event{
					{
						ConceptUUID:   barackObamaAltSmartlogicUUID,
						ConceptType:   "Person",
						AggregateHash: "5325651572798695401",
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
			pathToSetUpConcept:   "./fixtures/write/barackObama_1tme.json",
			pathToUpdatedConcept: "./fixtures/write/barackObama_1sl_1tme.json",
			pathToReadConcept:    "./fixtures/read/barackObama_1sl_1tme.json",
			conceptUUID:          barackObamaAltSmartlogicUUID,
			expectedError:        "",
			updatedConcepts: concepts.ConceptChanges{
				UpdatedIds: []string{barackObamaTMEUUID, barackObamaAltSmartlogicUUID},
				ChangedRecords: []concepts.Event{
					{
						ConceptUUID:   barackObamaAltSmartlogicUUID,
						ConceptType:   "Person",
						AggregateHash: "5325651572798695401",
						TransactionID: testTID,
						EventDetails: concepts.ConceptEvent{
							Type: concepts.UpdatedEvent,
						},
					},
					{
						ConceptUUID:   barackObamaTMEUUID,
						ConceptType:   "Person",
						AggregateHash: "5325651572798695401",
						TransactionID: testTID,
						EventDetails: concepts.ConcordanceEvent{
							Type:  concepts.AddedEvent,
							NewID: barackObamaAltSmartlogicUUID,
							OldID: barackObamaTMEUUID,
						},
					},
				},
			},
		},
		{
			testName:             "Removing a source from a single concordance produces 1 updated and 1 concordance removed event",
			pathToSetUpConcept:   "./fixtures/write/barackObama_1sl_1tme.json",
			pathToUpdatedConcept: "./fixtures/write/barackObama_1sl.json",
			pathToReadConcept:    "./fixtures/read/barackObama_1sl.json",
			conceptUUID:          barackObamaAltSmartlogicUUID,
			expectedError:        "",
			updatedConcepts: concepts.ConceptChanges{
				UpdatedIds: []string{barackObamaAltSmartlogicUUID, barackObamaTMEUUID},
				ChangedRecords: []concepts.Event{
					{
						ConceptUUID:   barackObamaAltSmartlogicUUID,
						ConceptType:   "Person",
						AggregateHash: "2803435629242666447",
						TransactionID: testTID,
						EventDetails: concepts.ConceptEvent{
							Type: concepts.UpdatedEvent,
						},
					},
					{
						ConceptUUID:   barackObamaTMEUUID,
						ConceptType:   "Person",
						AggregateHash: "2803435629242666447",
						TransactionID: testTID,
						EventDetails: concepts.ConcordanceEvent{
							Type:  concepts.RemovedEvent,
							NewID: barackObamaTMEUUID,
							OldID: barackObamaAltSmartlogicUUID,
						},
					},
				},
			},
		},
		{
			testName:             "Transferring a source from a single concordance produces 2 updated, 1 concordance removed and 2 concordance added event",
			pathToSetUpConcept:   "./fixtures/write/barackObama_1sl_1tme.json",
			pathToUpdatedConcept: "./fixtures/write/barackObama_1sl_1tme_1auth_1fs_1wiki.json",
			pathToReadConcept:    "./fixtures/read/barackObama_1sl_1tme_1auth_1fs_1wiki.json",
			conceptUUID:          barackObamaSmartlogicUUID,
			expectedError:        "",
			updatedConcepts: concepts.ConceptChanges{
				UpdatedIds: []string{barackObamaAuthorUUID, barackObamaTMEUUID, barackObamaWikidataUUID, barackObamaFactsetUUID, barackObamaSmartlogicUUID},
				ChangedRecords: []concepts.Event{
					{
						ConceptUUID:   barackObamaSmartlogicUUID,
						ConceptType:   "Person",
						AggregateHash: "6175303859824525987",
						TransactionID: testTID,
						EventDetails: concepts.ConceptEvent{
							Type: concepts.UpdatedEvent,
						},
					},
					{
						ConceptUUID:   barackObamaFactsetUUID,
						ConceptType:   "Person",
						AggregateHash: "6175303859824525987",
						TransactionID: testTID,
						EventDetails: concepts.ConceptEvent{
							Type: concepts.UpdatedEvent,
						},
					},
					{
						ConceptUUID:   barackObamaFactsetUUID,
						ConceptType:   "Person",
						AggregateHash: "6175303859824525987",
						TransactionID: testTID,
						EventDetails: concepts.ConcordanceEvent{
							Type:  concepts.AddedEvent,
							NewID: barackObamaSmartlogicUUID,
							OldID: barackObamaFactsetUUID,
						},
					},
					{
						ConceptUUID:   barackObamaWikidataUUID,
						ConceptType:   "Person",
						AggregateHash: "6175303859824525987",
						TransactionID: testTID,
						EventDetails: concepts.ConceptEvent{
							Type: concepts.UpdatedEvent,
						},
					},
					{
						ConceptUUID:   barackObamaWikidataUUID,
						ConceptType:   "Person",
						AggregateHash: "6175303859824525987",
						TransactionID: testTID,
						EventDetails: concepts.ConcordanceEvent{
							Type:  concepts.AddedEvent,
							NewID: barackObamaSmartlogicUUID,
							OldID: barackObamaWikidataUUID,
						},
					},
					{
						ConceptUUID:   barackObamaTMEUUID,
						ConceptType:   "Person",
						AggregateHash: "6175303859824525987",
						TransactionID: testTID,
						EventDetails: concepts.ConcordanceEvent{
							Type:  concepts.RemovedEvent,
							NewID: barackObamaTMEUUID,
							OldID: barackObamaAltSmartlogicUUID,
						},
					},
					{
						ConceptUUID:   barackObamaTMEUUID,
						ConceptType:   "Person",
						AggregateHash: "6175303859824525987",
						TransactionID: testTID,
						EventDetails: concepts.ConcordanceEvent{
							Type:  concepts.AddedEvent,
							NewID: barackObamaSmartlogicUUID,
							OldID: barackObamaTMEUUID,
						},
					},
					{
						ConceptUUID:   barackObamaAuthorUUID,
						ConceptType:   "Person",
						AggregateHash: "6175303859824525987",
						TransactionID: testTID,
						EventDetails: concepts.ConceptEvent{
							Type: concepts.UpdatedEvent,
						},
					},
					{
						ConceptUUID:   barackObamaAuthorUUID,
						ConceptType:   "Person",
						AggregateHash: "6175303859824525987",
						TransactionID: testTID,
						EventDetails: concepts.ConcordanceEvent{
							Type:  concepts.AddedEvent,
							NewID: barackObamaSmartlogicUUID,
							OldID: barackObamaAuthorUUID,
						},
					},
				},
			},
		},
		{
			testName:             "Trying to set an existing prefNode as a source results in error",
			pathToSetUpConcept:   "./fixtures/write/barackObama_1sl_1tme.json",
			pathToUpdatedConcept: "./fixtures/write/conflictedBarack.json",
			pathToReadConcept:    "",
			conceptUUID:          barackObamaSmartlogicUUID,
			expectedError:        "cannot currently process this record as it will break an existing concordance with prefUuid: " + barackObamaAltSmartlogicUUID,
			updatedConcepts: concepts.ConceptChanges{
				UpdatedIds:     []string{},
				ChangedRecords: []concepts.Event{},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.testName, func(t *testing.T) {
			concepts.CleanTestDB(t, db, barackObamaWikidataUUID, barackObamaTMEUUID, barackObamaFactsetUUID, barackObamaAuthorUUID, barackObamaSmartlogicUUID, barackObamaAltSmartlogicUUID)
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
