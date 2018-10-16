package genres

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
var conceptsDriver *GenreService

const (
	testTID                 = "tid_1234test"
	letterTmeUUID           = "fdc96953-d639-3df9-a373-4d6e94c55a93"
	lettersToTheEditorUUID  = "73394ef6-c73a-386c-b433-21deef7a88ac"
	letterSmartlogicUUID    = "f96c16f2-bbfe-11e8-9d1c-da24cd01f044"
	letterAltSmartlogicUUID = "58a458ee-bc06-11e8-9ef6-da24cd01f044"
	invalidPayloadUUID      = "d0360165-3ea7-3506-af2a-9a3b1316a78c"
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
	conceptsDriver = NewGenreService(db)

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
	defer concepts.CleanTestDB(t, db, invalidPayloadUUID, letterTmeUUID, lettersToTheEditorUUID, letterSmartlogicUUID)

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
			testName:        "Basic Tme Genre is successful and can be read from DB",
			filePathToWrite: "./fixtures/write/letter_1tme.json",
			filePathToRead:  "./fixtures/read/letter_1tme.json",
			conceptUUID:     letterTmeUUID,
			expectedError:   "",
			updatedConcepts: concepts.ConceptChanges{
				UpdatedIds: []string{letterTmeUUID},
				ChangedRecords: []concepts.Event{
					{
						ConceptUUID:   letterTmeUUID,
						ConceptType:   "Genre",
						AggregateHash: "5207007797745973891",
						TransactionID: testTID,
						EventDetails: concepts.ConceptEvent{
							Type: concepts.UpdatedEvent,
						},
					},
				},
			},
		},
		{
			testName:        "Concorded Section and Genre is successful and can be read from DB",
			filePathToWrite: "./fixtures/write/letter_1sl_2tme.json",
			filePathToRead:  "./fixtures/read/letter_1sl_2tme.json",
			conceptUUID:     letterSmartlogicUUID,
			expectedError:   "",
			updatedConcepts: concepts.ConceptChanges{
				UpdatedIds: []string{lettersToTheEditorUUID, letterTmeUUID, letterSmartlogicUUID},
				ChangedRecords: []concepts.Event{
					{
						ConceptUUID:   letterSmartlogicUUID,
						ConceptType:   "Genre",
						AggregateHash: "10174895412509843074",
						TransactionID: testTID,
						EventDetails: concepts.ConceptEvent{
							Type: concepts.UpdatedEvent,
						},
					},
					{
						ConceptUUID:   letterTmeUUID,
						ConceptType:   "Genre",
						AggregateHash: "10174895412509843074",
						TransactionID: testTID,
						EventDetails: concepts.ConceptEvent{
							Type: concepts.UpdatedEvent,
						},
					},
					{
						ConceptUUID:   letterTmeUUID,
						ConceptType:   "Genre",
						AggregateHash: "10174895412509843074",
						TransactionID: testTID,
						EventDetails: concepts.ConcordanceEvent{
							Type:  concepts.AddedEvent,
							OldID: letterTmeUUID,
							NewID: letterSmartlogicUUID,
						},
					},
					{
						ConceptUUID:   lettersToTheEditorUUID,
						ConceptType:   "Section",
						AggregateHash: "10174895412509843074",
						TransactionID: testTID,
						EventDetails: concepts.ConceptEvent{
							Type: concepts.UpdatedEvent,
						},
					},
					{
						ConceptUUID:   lettersToTheEditorUUID,
						ConceptType:   "Section",
						AggregateHash: "10174895412509843074",
						TransactionID: testTID,
						EventDetails: concepts.ConcordanceEvent{
							Type:  concepts.AddedEvent,
							OldID: lettersToTheEditorUUID,
							NewID: letterSmartlogicUUID,
						},
					},
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.testName, func(t *testing.T) {
			concepts.CleanTestDB(t, db, invalidPayloadUUID, invalidPayloadUUID, letterTmeUUID, lettersToTheEditorUUID, letterSmartlogicUUID)

			if test.filePathToWriteFunc != nil {
				concepts.RunWriteFailServiceTest(t,
					test.testName,
					conceptsDriver,
					testTID,
					"Genre",
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
	defer concepts.CleanTestDB(t, db, invalidPayloadUUID, invalidPayloadUUID, letterTmeUUID, lettersToTheEditorUUID, letterSmartlogicUUID, letterAltSmartlogicUUID)
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
			pathToSetUpConcept:   "./fixtures/write/letter_1sl_1tme.json",
			pathToUpdatedConcept: "./fixtures/write/letter_1sl_1tme.json",
			pathToReadConcept:    "./fixtures/read/letter_1sl_1tme.json",
			conceptUUID:          letterAltSmartlogicUUID,
			expectedError:        "",
			updatedConcepts: concepts.ConceptChanges{
				UpdatedIds: []string{letterTmeUUID, letterAltSmartlogicUUID},
				ChangedRecords: []concepts.Event{
					{
						ConceptUUID:   letterAltSmartlogicUUID,
						ConceptType:   "Genre",
						AggregateHash: "13146433331602868990",
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
			pathToSetUpConcept:   "./fixtures/write/letter_1sl.json",
			pathToUpdatedConcept: "./fixtures/write/letter_1sl_1tme.json",
			pathToReadConcept:    "./fixtures/read/letter_1sl_1tme.json",
			conceptUUID:          letterAltSmartlogicUUID,
			expectedError:        "",
			updatedConcepts: concepts.ConceptChanges{
				UpdatedIds: []string{letterTmeUUID, letterAltSmartlogicUUID},
				ChangedRecords: []concepts.Event{
					{
						ConceptUUID:   letterAltSmartlogicUUID,
						ConceptType:   "Genre",
						AggregateHash: "13146433331602868990",
						TransactionID: testTID,
						EventDetails: concepts.ConceptEvent{
							Type: concepts.UpdatedEvent,
						},
					},
					{
						ConceptUUID:   letterTmeUUID,
						ConceptType:   "Genre",
						AggregateHash: "13146433331602868990",
						TransactionID: testTID,
						EventDetails: concepts.ConceptEvent{
							Type: concepts.UpdatedEvent,
						},
					},
					{
						ConceptUUID:   letterTmeUUID,
						ConceptType:   "Genre",
						AggregateHash: "13146433331602868990",
						TransactionID: testTID,
						EventDetails: concepts.ConcordanceEvent{
							Type:  concepts.AddedEvent,
							NewID: letterAltSmartlogicUUID,
							OldID: letterTmeUUID,
						},
					},
				},
			},
		},
		{
			testName:             "Removing a source from a single concordance produces 1 updated and 1 concordance removed event",
			pathToSetUpConcept:   "./fixtures/write/letter_1sl_1tme.json",
			pathToUpdatedConcept: "./fixtures/write/letter_1sl.json",
			pathToReadConcept:    "./fixtures/read/letter_1sl.json",
			conceptUUID:          letterAltSmartlogicUUID,
			expectedError:        "",
			updatedConcepts: concepts.ConceptChanges{
				UpdatedIds: []string{letterAltSmartlogicUUID, letterTmeUUID},
				ChangedRecords: []concepts.Event{
					{
						ConceptUUID:   letterAltSmartlogicUUID,
						ConceptType:   "Genre",
						AggregateHash: "821911387633104374",
						TransactionID: testTID,
						EventDetails: concepts.ConceptEvent{
							Type: concepts.UpdatedEvent,
						},
					},
					{
						ConceptUUID:   letterTmeUUID,
						ConceptType:   "Genre",
						AggregateHash: "821911387633104374",
						TransactionID: testTID,
						EventDetails: concepts.ConcordanceEvent{
							Type:  concepts.RemovedEvent,
							NewID: letterTmeUUID,
							OldID: letterAltSmartlogicUUID,
						},
					},
				},
			},
		},
		{
			testName:             "Transferring a source from a single concordance produces 2 updated, 1 concordance removed and 2 concordance added event",
			pathToSetUpConcept:   "./fixtures/write/letter_1sl_1tme.json",
			pathToUpdatedConcept: "./fixtures/write/letter_1sl_2tme.json",
			pathToReadConcept:    "./fixtures/read/letter_1sl_2tme.json",
			conceptUUID:          letterSmartlogicUUID,
			expectedError:        "",
			updatedConcepts: concepts.ConceptChanges{
				UpdatedIds: []string{lettersToTheEditorUUID, letterTmeUUID, letterSmartlogicUUID},
				ChangedRecords: []concepts.Event{
					{
						ConceptUUID:   letterSmartlogicUUID,
						ConceptType:   "Genre",
						AggregateHash: "10174895412509843074",
						TransactionID: testTID,
						EventDetails: concepts.ConceptEvent{
							Type: concepts.UpdatedEvent,
						},
					},
					{
						ConceptUUID:   letterTmeUUID,
						ConceptType:   "Genre",
						AggregateHash: "10174895412509843074",
						TransactionID: testTID,
						EventDetails: concepts.ConcordanceEvent{
							Type:  concepts.RemovedEvent,
							OldID: letterAltSmartlogicUUID,
							NewID: letterTmeUUID,
						},
					},
					{
						ConceptUUID:   letterTmeUUID,
						ConceptType:   "Genre",
						AggregateHash: "10174895412509843074",
						TransactionID: testTID,
						EventDetails: concepts.ConcordanceEvent{
							Type:  concepts.AddedEvent,
							OldID: letterTmeUUID,
							NewID: letterSmartlogicUUID,
						},
					},
					{
						ConceptUUID:   lettersToTheEditorUUID,
						ConceptType:   "Section",
						AggregateHash: "10174895412509843074",
						TransactionID: testTID,
						EventDetails: concepts.ConceptEvent{
							Type: concepts.UpdatedEvent,
						},
					},
					{
						ConceptUUID:   lettersToTheEditorUUID,
						ConceptType:   "Section",
						AggregateHash: "10174895412509843074",
						TransactionID: testTID,
						EventDetails: concepts.ConcordanceEvent{
							Type:  concepts.AddedEvent,
							OldID: lettersToTheEditorUUID,
							NewID: letterSmartlogicUUID,
						},
					},
				},
			},
		},
		{
			testName:             "Trying to set an existing prefNode as a source results in error",
			pathToSetUpConcept:   "./fixtures/write/letter_1sl_1tme.json",
			pathToUpdatedConcept: "./fixtures/write/conflictedLetter_2sl.json",
			pathToReadConcept:    "",
			conceptUUID:          letterSmartlogicUUID,
			expectedError:        "cannot currently process this record as it will break an existing concordance with prefUuid: " + letterAltSmartlogicUUID,
			updatedConcepts: concepts.ConceptChanges{
				UpdatedIds:     []string{},
				ChangedRecords: []concepts.Event{},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.testName, func(t *testing.T) {
			concepts.CleanTestDB(t, db, invalidPayloadUUID, invalidPayloadUUID, letterTmeUUID, lettersToTheEditorUUID, letterSmartlogicUUID, letterAltSmartlogicUUID)
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
