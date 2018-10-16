package locations

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

var db neoutils.NeoConnection

var conceptsDriver *LocationService

const (
	testTID                      = "tid_1234test"
	asiaPacificSmartlogicUUID    = "318dc7a8-bcbb-11e8-9c7a-da24cd01f044"
	asiaPacificLocationTmeUUID   = "2dac3134-874d-3bbd-9392-da6d99fb01a0"
	asiaPacificSectionTmeUUID    = "96b88cae-6988-31b9-b916-ab91f116d98a"
	asiaPacificAltSmartlogicUUID = "94f3ff02-bcbf-11e8-9c7a-da24cd01f044"
	invalidPayloadUUID           = "d0360165-3ea7-3506-af2a-9a3b1316a78c"

	broader1UUID = "7254e6c9-e9de-4952-9a94-564cc9ada79c"
	broader2UUID = "e073df10-861d-4212-b0ae-436f0ade834f"

	related1UUID = "99f28ed2-bcc0-11e8-9c7a-da24cd01f044"
	related2UUID = "a60afddb-fc00-4424-868b-e0e90470912d"

	supercededByUUID = "a27d9d8a-bcc0-11e8-9c7a-da24cd01f044"
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
	conceptsDriver = NewLocationService(db)

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
	defer concepts.CleanTestDB(t, db, invalidPayloadUUID, broader1UUID, broader2UUID, related1UUID, related2UUID, supercededByUUID,
		asiaPacificLocationTmeUUID, asiaPacificSectionTmeUUID, asiaPacificSmartlogicUUID, asiaPacificAltSmartlogicUUID)

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
			filePathToWrite: "./fixtures/write/asiaPacific_1tme.json",
			filePathToRead:  "./fixtures/read/asiaPacific_1tme.json",
			conceptUUID:     asiaPacificLocationTmeUUID,
			expectedError:   "",
			updatedConcepts: concepts.ConceptChanges{
				UpdatedIds: []string{asiaPacificLocationTmeUUID},
				ChangedRecords: []concepts.Event{
					{
						ConceptUUID:   asiaPacificLocationTmeUUID,
						ConceptType:   "Location",
						AggregateHash: "16487057592411441084",
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
			filePathToWrite: "./fixtures/write/asiaPacific_1sl_2tme.json",
			filePathToRead:  "./fixtures/read/asiaPacific_1sl_2tme.json",
			conceptUUID:     asiaPacificSmartlogicUUID,
			expectedError:   "",
			updatedConcepts: concepts.ConceptChanges{
				UpdatedIds: []string{asiaPacificLocationTmeUUID, asiaPacificSectionTmeUUID, asiaPacificSmartlogicUUID},
				ChangedRecords: []concepts.Event{
					{
						ConceptUUID:   asiaPacificSmartlogicUUID,
						ConceptType:   "Location",
						AggregateHash: "11940516222520648946",
						TransactionID: testTID,
						EventDetails: concepts.ConceptEvent{
							Type: concepts.UpdatedEvent,
						},
					},
					{
						ConceptUUID:   asiaPacificLocationTmeUUID,
						ConceptType:   "Location",
						AggregateHash: "11940516222520648946",
						TransactionID: testTID,
						EventDetails: concepts.ConceptEvent{
							Type: concepts.UpdatedEvent,
						},
					},
					{
						ConceptUUID:   asiaPacificLocationTmeUUID,
						ConceptType:   "Location",
						AggregateHash: "11940516222520648946",
						TransactionID: testTID,
						EventDetails: concepts.ConcordanceEvent{
							Type:  concepts.AddedEvent,
							NewID: asiaPacificSmartlogicUUID,
							OldID: asiaPacificLocationTmeUUID,
						},
					},
					{
						ConceptUUID:   asiaPacificSectionTmeUUID,
						ConceptType:   "Section",
						AggregateHash: "11940516222520648946",
						TransactionID: testTID,
						EventDetails: concepts.ConceptEvent{
							Type: concepts.UpdatedEvent,
						},
					},
					{
						ConceptUUID:   asiaPacificSectionTmeUUID,
						ConceptType:   "Section",
						AggregateHash: "11940516222520648946",
						TransactionID: testTID,
						EventDetails: concepts.ConcordanceEvent{
							Type:  concepts.AddedEvent,
							NewID: asiaPacificSmartlogicUUID,
							OldID: asiaPacificSectionTmeUUID,
						},
					},
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.testName, func(t *testing.T) {
			concepts.CleanTestDB(t, db, invalidPayloadUUID, broader1UUID, broader2UUID, related1UUID, related2UUID, supercededByUUID,
				asiaPacificLocationTmeUUID, asiaPacificSectionTmeUUID, asiaPacificSmartlogicUUID, asiaPacificAltSmartlogicUUID)

			if test.filePathToWriteFunc != nil {
				concepts.RunWriteFailServiceTest(t,
					test.testName,
					conceptsDriver,
					testTID,
					"Location",
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
	defer concepts.CleanTestDB(t, db, broader1UUID, broader2UUID, related1UUID, related2UUID, supercededByUUID,
		asiaPacificLocationTmeUUID, asiaPacificSectionTmeUUID, asiaPacificAltSmartlogicUUID, asiaPacificSmartlogicUUID)
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
			pathToSetUpConcept:   "./fixtures/write/asiaPacific_1sl_1tme.json",
			pathToUpdatedConcept: "./fixtures/write/asiaPacific_1sl_1tme.json",
			pathToReadConcept:    "./fixtures/read/asiaPacific_1sl_1tme.json",
			conceptUUID:          asiaPacificAltSmartlogicUUID,
			expectedError:        "",
			updatedConcepts: concepts.ConceptChanges{
				UpdatedIds: []string{asiaPacificLocationTmeUUID, asiaPacificAltSmartlogicUUID},
				ChangedRecords: []concepts.Event{
					{
						ConceptUUID:   asiaPacificAltSmartlogicUUID,
						ConceptType:   "Location",
						AggregateHash: "776668893078123080",
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
			pathToSetUpConcept:   "./fixtures/write/asiaPacific_1tme.json",
			pathToUpdatedConcept: "./fixtures/write/asiaPacific_1sl_1tme.json",
			pathToReadConcept:    "./fixtures/read/asiaPacific_1sl_1tme.json",
			conceptUUID:          asiaPacificAltSmartlogicUUID,
			expectedError:        "",
			updatedConcepts: concepts.ConceptChanges{
				UpdatedIds: []string{asiaPacificLocationTmeUUID, asiaPacificAltSmartlogicUUID},
				ChangedRecords: []concepts.Event{
					{
						ConceptUUID:   asiaPacificAltSmartlogicUUID,
						ConceptType:   "Location",
						AggregateHash: "776668893078123080",
						TransactionID: testTID,
						EventDetails: concepts.ConceptEvent{
							Type: concepts.UpdatedEvent,
						},
					},
					{
						ConceptUUID:   asiaPacificLocationTmeUUID,
						ConceptType:   "Location",
						AggregateHash: "776668893078123080",
						TransactionID: testTID,
						EventDetails: concepts.ConcordanceEvent{
							Type:  concepts.AddedEvent,
							NewID: asiaPacificAltSmartlogicUUID,
							OldID: asiaPacificLocationTmeUUID,
						},
					},
				},
			},
		},
		{
			testName:             "Removing a source from a single concordance produces 1 updated and 1 concordance removed event",
			pathToSetUpConcept:   "./fixtures/write/asiaPacific_1sl_1tme.json",
			pathToUpdatedConcept: "./fixtures/write/asiaPacific_1sl.json",
			pathToReadConcept:    "./fixtures/read/asiaPacific_1sl.json",
			conceptUUID:          asiaPacificAltSmartlogicUUID,
			expectedError:        "",
			updatedConcepts: concepts.ConceptChanges{
				UpdatedIds: []string{asiaPacificAltSmartlogicUUID, asiaPacificLocationTmeUUID},
				ChangedRecords: []concepts.Event{
					{
						ConceptUUID:   asiaPacificAltSmartlogicUUID,
						ConceptType:   "Location",
						AggregateHash: "14063650178371408195",
						TransactionID: testTID,
						EventDetails: concepts.ConceptEvent{
							Type: concepts.UpdatedEvent,
						},
					},
					{
						ConceptUUID:   asiaPacificLocationTmeUUID,
						ConceptType:   "Location",
						AggregateHash: "14063650178371408195",
						TransactionID: testTID,
						EventDetails: concepts.ConcordanceEvent{
							Type:  concepts.RemovedEvent,
							NewID: asiaPacificLocationTmeUUID,
							OldID: asiaPacificAltSmartlogicUUID,
						},
					},
				},
			},
		},
		{
			testName:             "Transferring a source from a single concordance produces 2 updated, 1 concordance removed and 2 concordance added event",
			pathToSetUpConcept:   "./fixtures/write/asiaPacific_1sl_1tme.json",
			pathToUpdatedConcept: "./fixtures/write/asiaPacific_1sl_2tme.json",
			pathToReadConcept:    "./fixtures/read/asiaPacific_1sl_2tme.json",
			conceptUUID:          asiaPacificSmartlogicUUID,
			expectedError:        "",
			updatedConcepts: concepts.ConceptChanges{
				UpdatedIds: []string{asiaPacificLocationTmeUUID, asiaPacificSectionTmeUUID, asiaPacificSmartlogicUUID},
				ChangedRecords: []concepts.Event{
					{
						ConceptUUID:   asiaPacificSmartlogicUUID,
						ConceptType:   "Location",
						AggregateHash: "11940516222520648946",
						TransactionID: testTID,
						EventDetails: concepts.ConceptEvent{
							Type: concepts.UpdatedEvent,
						},
					},
					{
						ConceptUUID:   asiaPacificSectionTmeUUID,
						ConceptType:   "Section",
						AggregateHash: "11940516222520648946",
						TransactionID: testTID,
						EventDetails: concepts.ConceptEvent{
							Type: concepts.UpdatedEvent,
						},
					},
					{
						ConceptUUID:   asiaPacificSectionTmeUUID,
						ConceptType:   "Section",
						AggregateHash: "11940516222520648946",
						TransactionID: testTID,
						EventDetails: concepts.ConcordanceEvent{
							Type:  concepts.AddedEvent,
							NewID: asiaPacificSmartlogicUUID,
							OldID: asiaPacificSectionTmeUUID,
						},
					},
					{
						ConceptUUID:   asiaPacificLocationTmeUUID,
						ConceptType:   "Location",
						AggregateHash: "11940516222520648946",
						TransactionID: testTID,
						EventDetails: concepts.ConcordanceEvent{
							Type:  concepts.AddedEvent,
							NewID: asiaPacificSmartlogicUUID,
							OldID: asiaPacificLocationTmeUUID,
						},
					},
					{
						ConceptUUID:   asiaPacificLocationTmeUUID,
						ConceptType:   "Location",
						AggregateHash: "11940516222520648946",
						TransactionID: testTID,
						EventDetails: concepts.ConcordanceEvent{
							Type:  concepts.RemovedEvent,
							NewID: asiaPacificLocationTmeUUID,
							OldID: asiaPacificAltSmartlogicUUID,
						},
					},
				},
			},
		},
		{
			testName:             "Trying to set an existing prefNode as a source results in error",
			pathToSetUpConcept:   "./fixtures/write/asiaPacific_1sl_1tme.json",
			pathToUpdatedConcept: "./fixtures/write/conflictedAsiaPacific_2sl.json",
			pathToReadConcept:    "",
			conceptUUID:          asiaPacificSmartlogicUUID,
			expectedError:        "cannot currently process this record as it will break an existing concordance with prefUuid: " + asiaPacificAltSmartlogicUUID,
			updatedConcepts: concepts.ConceptChanges{
				UpdatedIds:     []string{},
				ChangedRecords: []concepts.Event{},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.testName, func(t *testing.T) {
			concepts.CleanTestDB(t, db, broader1UUID, broader2UUID, related1UUID, related2UUID, supercededByUUID,
				asiaPacificLocationTmeUUID, asiaPacificSectionTmeUUID, asiaPacificAltSmartlogicUUID, asiaPacificSmartlogicUUID)
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
