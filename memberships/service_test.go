package memberships

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

var conceptsDriver *MembershipService

const (
	testTID = "tid_1234test"
	//Basic FS
	basicFsMembershipUUID = "b3164c24-bb2f-11e8-989a-da24cd01f044"
	person1UUID           = "ba26b1a2-bb2f-11e8-989a-da24cd01f044"
	org1UUID              = "6f0df55d-372e-4b36-b272-1a361933c7af"
	role1UUID             = "c27c9826-bb2f-11e8-989a-da24cd01f044"
	//Basic SL
	basicSlMembershipUUID = "0665c4a0-2e63-4b5d-8397-6d0aaa30f52b"
	person2UUID           = "035df8a2-bb45-11e8-a488-da24cd01f044"
	org2UUID              = "2d42983c-9c91-4d72-bc5d-a4153b11ec86"
	role2UUID             = "06c838ad-4fa2-4cae-b55b-9a2b0477b6f5"
	//Complex FS
	complexFsMembershipUUID = "529101e4-a0ce-4b16-bbc5-73edb3ae46c5"
	person3UUID             = "c74f2a24-bb3b-11e8-a488-da24cd01f044"
	org3UUID                = "54916418-9239-424b-b15e-a6291159330e"
	role3UUID               = "c92fc814-bb2f-11e8-989a-da24cd01f044"
	role4UUID               = "79734640-cd65-4108-b621-a350c102c694"
	role5UUID               = "d02bd360-bb2f-11e8-989a-da24cd01f044"
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
	conceptsDriver = NewMembershipService(db)

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
	defer concepts.CleanTestDB(t, db, org1UUID, person1UUID, role1UUID, basicFsMembershipUUID)

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
			testName:        "Put payload with no organisationUUID results in error",
			filePathToWrite: "./fixtures/write/invalidPayloads/missingOrganisationUUID.json",
			filePathToRead:  "",
			conceptUUID:     invalidPayloadUUID,
			expectedError:   "invalid request, no OrganisationUUID has been supplied",
			updatedConcepts: concepts.ConceptChanges{},
		},
		{
			testName:        "Put payload with no personUUID results in error",
			filePathToWrite: "./fixtures/write/invalidPayloads/missingPersonUUID.json",
			filePathToRead:  "",
			conceptUUID:     invalidPayloadUUID,
			expectedError:   "invalid request, no PersonUUID has been supplied",
			updatedConcepts: concepts.ConceptChanges{},
		},
		{
			testName:        "Put payload with no membershipRoles results in error",
			filePathToWrite: "./fixtures/write/invalidPayloads/missingMembershipRoles.json",
			filePathToRead:  "",
			conceptUUID:     invalidPayloadUUID,
			expectedError:   "invalid request, no MembershipRoles have been supplied",
			updatedConcepts: concepts.ConceptChanges{},
		},
		{
			testName:        "Basic FS membership is successful and can be read from DB",
			filePathToWrite: "./fixtures/write/simpleMemb_1fs_1role.json",
			filePathToRead:  "./fixtures/read/simpleMemb_1fs_1role.json",
			conceptUUID:     basicFsMembershipUUID,
			expectedError:   "",
			updatedConcepts: concepts.ConceptChanges{
				UpdatedIds: []string{basicFsMembershipUUID},
				ChangedRecords: []concepts.Event{
					{
						ConceptUUID:   basicFsMembershipUUID,
						ConceptType:   "Membership",
						AggregateHash: "13359611713231072576",
						TransactionID: testTID,
						EventDetails: concepts.ConceptEvent{
							Type: concepts.UpdatedEvent,
						},
					},
				},
			},
		},
		{
			testName:        "Basic Sl membership is successful and can be read from DB",
			filePathToWrite: "./fixtures/write/simpleMemb_1sl_1role.json",
			filePathToRead:  "./fixtures/read/simpleMemb_1sl_1role.json",
			conceptUUID:     basicSlMembershipUUID,
			expectedError:   "",
			updatedConcepts: concepts.ConceptChanges{
				UpdatedIds: []string{basicSlMembershipUUID},
				ChangedRecords: []concepts.Event{
					{
						ConceptUUID:   basicSlMembershipUUID,
						ConceptType:   "Membership",
						AggregateHash: "17753846049123574313",
						TransactionID: testTID,
						EventDetails: concepts.ConceptEvent{
							Type: concepts.UpdatedEvent,
						},
					},
				},
			},
		},
		{
			testName:        "Complex FS membership is successful and can be read from DB",
			filePathToWrite: "./fixtures/write/complexMemb_1fs_5role.json",
			filePathToRead:  "./fixtures/read/complexMemb_1fs_5role.json",
			conceptUUID:     complexFsMembershipUUID,
			expectedError:   "",
			updatedConcepts: concepts.ConceptChanges{
				UpdatedIds: []string{complexFsMembershipUUID},
				ChangedRecords: []concepts.Event{
					{
						ConceptUUID:   complexFsMembershipUUID,
						ConceptType:   "Membership",
						AggregateHash: "15635993764074561321",
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
			defer concepts.CleanTestDB(t, db, org1UUID, person1UUID, org2UUID, person2UUID, org3UUID, person3UUID,
				role1UUID, role2UUID, role3UUID, role4UUID, role5UUID, basicFsMembershipUUID, basicSlMembershipUUID, complexFsMembershipUUID)

			if test.filePathToWriteFunc != nil {
				concepts.RunWriteFailServiceTest(t,
					test.testName,
					conceptsDriver,
					testTID,
					"Membership",
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

func TestWriteService_ConceptsCanBeUpdated(t *testing.T) {
	defer concepts.CleanTestDB(t, db, org1UUID, person1UUID, org2UUID, person2UUID, org3UUID, person3UUID,
		role1UUID, role2UUID, role3UUID, role4UUID, role5UUID, basicFsMembershipUUID, basicSlMembershipUUID, complexFsMembershipUUID)
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
			testName:             "Rewriting the same membership, results in proper teardown and repopulation with a single update event",
			pathToSetUpConcept:   "./fixtures/write/simpleMemb_1fs_1role.json",
			pathToUpdatedConcept: "./fixtures/write/simpleMemb_1fs_1role.json",
			pathToReadConcept:    "./fixtures/read/simpleMemb_1fs_1role.json",
			conceptUUID:          basicFsMembershipUUID,
			expectedError:        "",
			updatedConcepts: concepts.ConceptChanges{
				UpdatedIds: []string{basicFsMembershipUUID},
				ChangedRecords: []concepts.Event{
					{
						ConceptUUID:   basicFsMembershipUUID,
						ConceptType:   "Membership",
						AggregateHash: "13359611713231072576",
						TransactionID: testTID,
						EventDetails: concepts.ConceptEvent{
							Type: concepts.UpdatedEvent,
						},
					},
				},
			},
		},
		{
			testName:             "Can update membership with less roles roles to members, results in proper teardown and repopulation with a single update event",
			pathToSetUpConcept:   "./fixtures/write/complexMemb_1fs_5role.json",
			pathToUpdatedConcept: "./fixtures/write/basicComplexMemb_1fs_1role.json",
			pathToReadConcept:    "./fixtures/read/basicComplexMemb_1fs_1role.json",
			conceptUUID:          complexFsMembershipUUID,
			expectedError:        "",
			updatedConcepts: concepts.ConceptChanges{
				UpdatedIds: []string{complexFsMembershipUUID},
				ChangedRecords: []concepts.Event{
					{
						ConceptUUID:   complexFsMembershipUUID,
						ConceptType:   "Membership",
						AggregateHash: "13469763115481050918",
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
			concepts.CleanTestDB(t, db, org1UUID, person1UUID, org2UUID, person2UUID, org3UUID, person3UUID,
				role1UUID, role2UUID, role3UUID, role4UUID, role5UUID, basicFsMembershipUUID, basicSlMembershipUUID, complexFsMembershipUUID)
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
