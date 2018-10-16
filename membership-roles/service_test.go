package membership_roles

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

var conceptsDriver *MembershipRoleService

const (
	testTID                   = "tid_1234test"
	basicFsMembershipRoleUUID = "93a856fe-bb55-11e8-a488-da24cd01f044"
	basicSlMembershipRoleUUID = "17432cd1-5518-4c12-92ce-8cb6b09bf267"
	invalidPayloadUUID        = "d0360165-3ea7-3506-af2a-9a3b1316a78c"
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
	conceptsDriver = NewMembershipRoleService(db)

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
	defer concepts.CleanTestDB(t, db, basicFsMembershipRoleUUID, basicSlMembershipRoleUUID)

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
			testName:        "Basic FS MembershipRole is successful and can be read from DB",
			filePathToWrite: "./fixtures/write/boardRole_1fs.json",
			filePathToRead:  "./fixtures/read/boardRole_1fs.json",
			conceptUUID:     basicFsMembershipRoleUUID,
			expectedError:   "",
			updatedConcepts: concepts.ConceptChanges{
				UpdatedIds: []string{basicFsMembershipRoleUUID},
				ChangedRecords: []concepts.Event{
					{
						ConceptUUID:   basicFsMembershipRoleUUID,
						ConceptType:   "MembershipRole",
						AggregateHash: "3942826707094733046",
						TransactionID: testTID,
						EventDetails: concepts.ConceptEvent{
							Type: concepts.UpdatedEvent,
						},
					},
				},
			},
		},
		{
			testName:        "Basic SL MembershipRole is successful and can be read from DB",
			filePathToWrite: "./fixtures/write/journalist_1sl.json",
			filePathToRead:  "./fixtures/read/journalist_1sl.json",
			conceptUUID:     basicSlMembershipRoleUUID,
			expectedError:   "",
			updatedConcepts: concepts.ConceptChanges{
				UpdatedIds: []string{basicSlMembershipRoleUUID},
				ChangedRecords: []concepts.Event{
					{
						ConceptUUID:   basicSlMembershipRoleUUID,
						ConceptType:   "MembershipRole",
						AggregateHash: "14448244402611794809",
						TransactionID: testTID,
						EventDetails: concepts.ConceptEvent{
							Type: concepts.UpdatedEvent,
						},
					},
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.testName, func(t *testing.T) {
			defer concepts.CleanTestDB(t, db, basicFsMembershipRoleUUID, basicSlMembershipRoleUUID)
			if test.filePathToWriteFunc != nil {
				concepts.RunWriteFailServiceTest(t,
					test.testName,
					conceptsDriver,
					testTID,
					"MembershipRole",
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
