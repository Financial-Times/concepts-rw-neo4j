package topics

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
var conceptsDriver *TopicService

const (
	testTID                 = "tid_1234test"
	fintechSmartlogicUUID   = "fd3b0d26-bfd5-11e8-9c7a-da24cd01f044"
	fintechTmeUUID          = "42767939-582e-39d3-a940-39c5ccb15d0f"
	companiesTmeUUID        = "aa956186-0794-3427-bf18-527626bab96b"
	companiesSmartlogicUUID = "7626fab0-7e3e-4598-9089-ad0afdb33459"
	invalidPayloadUUID      = "d0360165-3ea7-3506-af2a-9a3b1316a78c"

	broader1UUID = "66f1abb1-d35e-4d98-88db-b7a943104e84"
	broader2UUID = "c91b1fad-1097-468b-be82-9a8ff717d54c"

	related1UUID = "a072c32f-42de-4cc6-8e77-c468d681e5db"
	related2UUID = "fbfd271a-71a6-4663-842b-0c6406785bc6"

	supercededByUUID = "b062fd6b-01fa-4049-a1bc-092f9f43c307"
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
	conceptsDriver = NewTopicService(db)

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
		fintechTmeUUID, fintechSmartlogicUUID, fintechSmartlogicUUID, companiesSmartlogicUUID)

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
			testName:        "TME Concorded Section is successful and can be read from DB",
			filePathToWrite: "./fixtures/write/companies_1sl_1tme.json",
			filePathToRead:  "./fixtures/read/companies_1sl_1tme.json",
			conceptUUID:     companiesSmartlogicUUID,
			expectedError:   "",
			updatedConcepts: concepts.ConceptChanges{
				UpdatedIds: []string{companiesTmeUUID, companiesSmartlogicUUID},
				ChangedRecords: []concepts.Event{
					{
						ConceptUUID:   companiesSmartlogicUUID,
						ConceptType:   "Topic",
						AggregateHash: "13090325035734702176",
						TransactionID: testTID,
						EventDetails: concepts.ConceptEvent{
							Type: concepts.UpdatedEvent,
						},
					},
					{
						ConceptUUID:   companiesTmeUUID,
						ConceptType:   "Section",
						AggregateHash: "13090325035734702176",
						TransactionID: testTID,
						EventDetails: concepts.ConceptEvent{
							Type: concepts.UpdatedEvent,
						},
					},
					{
						ConceptUUID:   companiesTmeUUID,
						ConceptType:   "Section",
						AggregateHash: "13090325035734702176",
						TransactionID: testTID,
						EventDetails: concepts.ConcordanceEvent{
							Type:  concepts.AddedEvent,
							NewID: companiesSmartlogicUUID,
							OldID: companiesTmeUUID,
						},
					},
				},
			},
		},
		{
			testName:        "TME Concorded Topic is successful and can be read from DB",
			filePathToWrite: "./fixtures/write/fintech_1sl_1tme.json",
			filePathToRead:  "./fixtures/read/fintech_1sl_1tme.json",
			conceptUUID:     fintechSmartlogicUUID,
			expectedError:   "",
			updatedConcepts: concepts.ConceptChanges{
				UpdatedIds: []string{fintechTmeUUID, fintechSmartlogicUUID},
				ChangedRecords: []concepts.Event{
					{
						ConceptUUID:   fintechSmartlogicUUID,
						ConceptType:   "Topic",
						AggregateHash: "2463375033613724857",
						TransactionID: testTID,
						EventDetails: concepts.ConceptEvent{
							Type: concepts.UpdatedEvent,
						},
					},
					{
						ConceptUUID:   fintechTmeUUID,
						ConceptType:   "Topic",
						AggregateHash: "2463375033613724857",
						TransactionID: testTID,
						EventDetails: concepts.ConceptEvent{
							Type: concepts.UpdatedEvent,
						},
					},
					{
						ConceptUUID:   fintechTmeUUID,
						ConceptType:   "Topic",
						AggregateHash: "2463375033613724857",
						TransactionID: testTID,
						EventDetails: concepts.ConcordanceEvent{
							Type:  concepts.AddedEvent,
							NewID: fintechSmartlogicUUID,
							OldID: fintechTmeUUID,
						},
					},
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.testName, func(t *testing.T) {
			concepts.CleanTestDB(t, db, invalidPayloadUUID, broader1UUID, broader2UUID, related1UUID, related2UUID, supercededByUUID,
				fintechTmeUUID, companiesTmeUUID, fintechSmartlogicUUID, companiesSmartlogicUUID)

			if test.filePathToWriteFunc != nil {
				concepts.RunWriteFailServiceTest(t,
					test.testName,
					conceptsDriver,
					testTID,
					"Topic",
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
	defer concepts.CleanTestDB(t, db, invalidPayloadUUID, broader1UUID, broader2UUID, related1UUID, related2UUID, supercededByUUID,
		fintechTmeUUID, companiesTmeUUID, fintechSmartlogicUUID, companiesSmartlogicUUID)
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
			pathToSetUpConcept:   "./fixtures/write/fintech_1sl_1tme.json",
			pathToUpdatedConcept: "./fixtures/write/fintech_1sl_1tme.json",
			pathToReadConcept:    "./fixtures/read/fintech_1sl_1tme.json",
			conceptUUID:          fintechSmartlogicUUID,
			expectedError:        "",
			updatedConcepts: concepts.ConceptChanges{
				UpdatedIds: []string{fintechTmeUUID, fintechSmartlogicUUID},
				ChangedRecords: []concepts.Event{
					{
						ConceptUUID:   fintechSmartlogicUUID,
						ConceptType:   "Topic",
						AggregateHash: "2463375033613724857",
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
			pathToSetUpConcept:   "./fixtures/write/fintech_1tme.json",
			pathToUpdatedConcept: "./fixtures/write/fintech_1sl_1tme.json",
			pathToReadConcept:    "./fixtures/read/fintech_1sl_1tme.json",
			conceptUUID:          fintechSmartlogicUUID,
			expectedError:        "",
			updatedConcepts: concepts.ConceptChanges{
				UpdatedIds: []string{fintechTmeUUID, fintechSmartlogicUUID},
				ChangedRecords: []concepts.Event{
					{
						ConceptUUID:   fintechSmartlogicUUID,
						ConceptType:   "Topic",
						AggregateHash: "2463375033613724857",
						TransactionID: testTID,
						EventDetails: concepts.ConceptEvent{
							Type: concepts.UpdatedEvent,
						},
					},
					{
						ConceptUUID:   fintechTmeUUID,
						ConceptType:   "Topic",
						AggregateHash: "2463375033613724857",
						TransactionID: testTID,
						EventDetails: concepts.ConcordanceEvent{
							Type:  concepts.AddedEvent,
							NewID: fintechSmartlogicUUID,
							OldID: fintechTmeUUID,
						},
					},
				},
			},
		},
		{
			testName:             "Removing a source from a single concordance produces 1 updated and 1 concordance removed event",
			pathToSetUpConcept:   "./fixtures/write/fintech_1sl_1tme.json",
			pathToUpdatedConcept: "./fixtures/write/fintech_1sl.json",
			pathToReadConcept:    "./fixtures/read/fintech_1sl.json",
			conceptUUID:          fintechSmartlogicUUID,
			expectedError:        "",
			updatedConcepts: concepts.ConceptChanges{
				UpdatedIds: []string{fintechSmartlogicUUID, fintechTmeUUID},
				ChangedRecords: []concepts.Event{
					{
						ConceptUUID:   fintechSmartlogicUUID,
						ConceptType:   "Topic",
						AggregateHash: "12199117414329345103",
						TransactionID: testTID,
						EventDetails: concepts.ConceptEvent{
							Type: concepts.UpdatedEvent,
						},
					},
					{
						ConceptUUID:   fintechTmeUUID,
						ConceptType:   "Topic",
						AggregateHash: "12199117414329345103",
						TransactionID: testTID,
						EventDetails: concepts.ConcordanceEvent{
							Type:  concepts.RemovedEvent,
							NewID: fintechTmeUUID,
							OldID: fintechSmartlogicUUID,
						},
					},
				},
			},
		},
		{
			testName:             "Transferring a source from a single concordance produces 2 updated, 1 concordance removed and 2 concordance added event",
			pathToSetUpConcept:   "./fixtures/write/companies_1sl_1tme.json",
			pathToUpdatedConcept: "./fixtures/write/fintech_1sl_2tme.json",
			pathToReadConcept:    "./fixtures/read/fintech_1sl_2tme.json",
			conceptUUID:          fintechSmartlogicUUID,
			expectedError:        "",
			updatedConcepts: concepts.ConceptChanges{
				UpdatedIds: []string{fintechTmeUUID, fintechSmartlogicUUID, companiesTmeUUID},
				ChangedRecords: []concepts.Event{
					{
						ConceptUUID:   fintechSmartlogicUUID,
						ConceptType:   "Topic",
						AggregateHash: "5946678527081678893",
						TransactionID: testTID,
						EventDetails: concepts.ConceptEvent{
							Type: concepts.UpdatedEvent,
						},
					},
					{
						ConceptUUID:   fintechTmeUUID,
						ConceptType:   "Topic",
						AggregateHash: "5946678527081678893",
						TransactionID: testTID,
						EventDetails: concepts.ConceptEvent{
							Type: concepts.UpdatedEvent,
						},
					},
					{
						ConceptUUID:   fintechTmeUUID,
						ConceptType:   "Topic",
						AggregateHash: "5946678527081678893",
						TransactionID: testTID,
						EventDetails: concepts.ConcordanceEvent{
							Type:  concepts.AddedEvent,
							NewID: fintechSmartlogicUUID,
							OldID: fintechTmeUUID,
						},
					},
					{
						ConceptUUID:   companiesTmeUUID,
						ConceptType:   "Section",
						AggregateHash: "5946678527081678893",
						TransactionID: testTID,
						EventDetails: concepts.ConcordanceEvent{
							Type:  concepts.AddedEvent,
							NewID: fintechSmartlogicUUID,
							OldID: companiesTmeUUID,
						},
					},
					{
						ConceptUUID:   companiesTmeUUID,
						ConceptType:   "Section",
						AggregateHash: "5946678527081678893",
						TransactionID: testTID,
						EventDetails: concepts.ConcordanceEvent{
							Type:  concepts.RemovedEvent,
							NewID: companiesTmeUUID,
							OldID: companiesSmartlogicUUID,
						},
					},
				},
			},
		},
		{
			testName:             "Trying to set an existing prefNode as a source results in error",
			pathToSetUpConcept:   "./fixtures/write/companies_1sl_1tme.json",
			pathToUpdatedConcept: "./fixtures/write/conflictedFintech_2sl.json",
			pathToReadConcept:    "",
			conceptUUID:          fintechSmartlogicUUID,
			expectedError:        "cannot currently process this record as it will break an existing concordance with prefUuid: " + companiesSmartlogicUUID,
			updatedConcepts: concepts.ConceptChanges{
				UpdatedIds:     []string{},
				ChangedRecords: []concepts.Event{},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.testName, func(t *testing.T) {
			concepts.CleanTestDB(t, db, invalidPayloadUUID, broader1UUID, broader2UUID, related1UUID, related2UUID, supercededByUUID,
				fintechTmeUUID, companiesTmeUUID, fintechSmartlogicUUID, companiesSmartlogicUUID)
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
