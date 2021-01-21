// +build integration

package concepts

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"
	"time"

	"sort"
	"strings"

	logger "github.com/Financial-Times/go-logger"
	"github.com/Financial-Times/neo-utils-go/neoutils"
	"github.com/jmcvetta/neoism"
	"github.com/mitchellh/hashstructure"
	"github.com/stretchr/testify/assert"
)

//all uuids to be cleaned from DB
const (
	basicConceptUUID           = "bbc4f575-edb3-4f51-92f0-5ce6c708d1ea"
	anotherBasicConceptUUID    = "4c41f314-4548-4fb6-ac48-4618fcbfa84c"
	yetAnotherBasicConceptUUID = "f7e3fe2d-7496-4d42-b19f-378094efd263"
	simpleSmartlogicTopicUUID  = "abd38d90-2152-11e8-9ac1-da24cd01f044"
	parentUUID                 = "2ef39c2a-da9c-4263-8209-ebfd490d3101"

	boardRoleUUID                     = "aa9ef631-c025-43b2-b0ce-d78d394cc6e6"
	membershipRoleUUID                = "f807193d-337b-412f-b32c-afa14b385819"
	organisationUUID                  = "7f40d291-b3cb-47c4-9bce-18413e9350cf"
	personUUID                        = "35946807-0205-4fc1-8516-bb1ae141659b"
	financialInstrumentUUID           = "475b7b59-66d5-47e2-a273-adc3d1ba8286"
	financialInstrumentSameIssuerUUID = "08c6066c-9356-4e96-abd5-9a4f3726724a"
	financialOrgUUID                  = "4290f028-05e9-4c2d-9f11-61ec59ba081a"
	anotherFinancialOrgUUID           = "230e3a74-694a-4d94-8294-6a45ec1ced26"
	membershipUUID                    = "cbadd9a7-5da9-407a-a5ec-e379460991f2"
	anotherOrganisationUUID           = "7ccf2673-2ec0-4b42-b69e-9a2460b945c6"
	anotherPersonUUID                 = "69a8e241-2bfb-4aed-a441-8489d813c5f7"
	testOrgUUID                       = "c28fa0b4-4245-11e8-842f-0ed5f89f718b"
	parentOrgUUID                     = "c001ee9c-94c5-11e8-8f42-da24cd01f044"
	locationUUID                      = "82cba3ce-329b-3010-b29d-4282a215889f"
	anotherLocationUUID               = "6b683eff-56c3-43d9-acfc-7511d974fc01"
	naicsIndustryClassificationUUID   = "38ee195d-ebdd-48a9-af4b-c8a322e7b04d"

	supersededByUUID = "1a96ee7a-a4af-3a56-852c-60420b0b8da6"

	sourceID1 = "74c94c35-e16b-4527-8ef1-c8bcdcc8f05b"
	sourceID2 = "de3bcb30-992c-424e-8891-73f5bd9a7d3a"
	sourceID3 = "5b1d8c31-dfe4-4326-b6a9-6227cb59af1f"

	unknownThingUUID        = "b5d7c6b5-db7d-4bce-9d6a-f62195571f92"
	anotherUnknownThingUUID = "a4fe339d-664f-4609-9fe0-dd3ec6efe87e"

	brandUUID                  = "cce1bc63-3717-4ae6-9399-88dab5966815"
	anotherBrandUUID           = "21b4bdb5-25ca-4705-af5f-519b279f4764"
	yetAnotherBrandUUID        = "2d3e16e0-61cb-4322-8aff-3b01c59f4dab"
	topicUUID                  = "740c604b-8d97-443e-be70-33de6f1d6e67"
	anotherTopicUUID           = "2e7429bd-7a84-41cb-a619-2c702893e359"
	conceptHasFocusUUID        = "a39a4558-f562-4dca-8774-000246e6eebe"
	anotherConceptHasFocusUUID = "2abff0bd-544d-31c3-899b-fba2f60d53dd"
)

var (
	membershipRole = MembershipRole{
		RoleUUID:        "f807193d-337b-412f-b32c-afa14b385819",
		InceptionDate:   "2016-01-01",
		TerminationDate: "2017-02-02",
	}
	anotherMembershipRole = MembershipRole{
		RoleUUID:      "fe94adc6-ca44-438f-ad8f-0188d4a74987",
		InceptionDate: "2011-06-27",
	}
	anotherMembershipRole2 = MembershipRole{
		RoleUUID:        "83102635-e6d5-3c48-9d5f-ab34c1401c22",
		InceptionDate:   "2009-09-10",
		TerminationDate: "2013-02-20",
	}
)

//Reusable Neo4J connection
var db neoutils.NeoConnection

//Concept Service under test
var conceptsDriver ConceptService

var emptyList []string

func helperLoadBytes(t *testing.T, name string) []byte {
	path := filepath.Join("testdata", name)
	bytes, err := ioutil.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return bytes
}

// A lone concept should always have matching pref labels and uuid at the src system level and the top level - We are
// currently missing validation around this
func getAggregatedConcept(t *testing.T, name string) AggregatedConcept {
	ac := AggregatedConcept{}
	err := json.Unmarshal(helperLoadBytes(t, name), &ac)
	if err != nil {
		t.Fatal(err)
	}
	return ac
}

func getOrganisationWithAllCountries() AggregatedConcept {
	return AggregatedConcept{
		PrefUUID:   testOrgUUID,
		Type:       "PublicCompany",
		ProperName: "Strix Group Plc",
		PrefLabel:  "Strix Group Plc",
		ShortName:  "Strix Group",
		TradeNames: []string{
			"STRIX GROUP PLC",
		},
		FormerNames: []string{
			"Castletown Thermostats",
			"Steam Plc",
		},
		Aliases: []string{
			"Strix Group Plc",
			"STRIX GROUP PLC",
			"Strix Group",
			"Castletown Thermostats",
			"Steam Plc",
		},
		CountryCode:            "BG",
		CountryOfIncorporation: "GB",
		CountryOfOperations:    "FR",
		CountryOfRisk:          "BG",
		PostalCode:             "IM9 2RG",
		YearFounded:            1951,
		EmailAddress:           "info@strix.com",
		LeiCode:                "213800KZEW5W6BZMNT62",
		SourceRepresentations: []Concept{
			{
				UUID:           testOrgUUID,
				Type:           "PublicCompany",
				Authority:      "FACTSET",
				AuthorityValue: "B000BB-S",
				ProperName:     "Strix Group Plc",
				PrefLabel:      "Strix Group Plc",
				ShortName:      "Strix Group",
				TradeNames: []string{
					"STRIX GROUP PLC",
				},
				FormerNames: []string{
					"Castletown Thermostats",
					"Steam Plc",
				},
				Aliases: []string{
					"Strix Group Plc",
					"STRIX GROUP PLC",
					"Strix Group",
					"Castletown Thermostats",
					"Steam Plc",
				},
				CountryCode:                "BG",
				CountryOfIncorporation:     "GB",
				CountryOfOperations:        "FR",
				CountryOfRisk:              "BG",
				CountryOfIncorporationUUID: locationUUID,
				CountryOfOperationsUUID:    locationUUID,
				CountryOfRiskUUID:          anotherLocationUUID,
				PostalCode:                 "IM9 2RG",
				YearFounded:                1951,
				EmailAddress:               "info@strix.com",
				LeiCode:                    "213800KZEW5W6BZMNT62",
				ParentOrganisation:         parentOrgUUID,
			},
		},
	}
}

func getConcept(t *testing.T, name string) Concept {
	c := Concept{}
	err := json.Unmarshal(helperLoadBytes(t, name), &c)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func getLocation() AggregatedConcept {
	return AggregatedConcept{
		PrefUUID:  locationUUID,
		PrefLabel: "Location Pref Label",
		Type:      "Location",
		SourceRepresentations: []Concept{{
			UUID:           locationUUID,
			PrefLabel:      "Location Pref Label",
			Type:           "Location",
			Authority:      "ManagedLocation",
			AuthorityValue: locationUUID,
		}},
	}
}

func getLocationWithISO31661() AggregatedConcept {
	return AggregatedConcept{
		PrefUUID:  locationUUID,
		PrefLabel: "Location Pref Label 2",
		Type:      "Location",
		Aliases: []string{
			"Bulgaria",
			"Bulgarie",
			"Bulgarien",
		},
		ISO31661: "BG",
		SourceRepresentations: []Concept{{
			UUID:           locationUUID,
			PrefLabel:      "Location Pref Label 2",
			Type:           "Location",
			Authority:      "ManagedLocation",
			AuthorityValue: locationUUID,
			Aliases: []string{
				"Bulgaria",
				"Bulgarie",
				"Bulgarien",
			},
			ISO31661: "BG",
		}},
	}
}

func getLocationWithISO31661AndConcordance() AggregatedConcept {
	return AggregatedConcept{
		PrefUUID:  anotherLocationUUID,
		PrefLabel: "Location Pref Label 2",
		Type:      "Location",
		Aliases: []string{
			"Bulgaria",
			"Bulgarie",
			"Bulgarien",
		},
		ISO31661: "BG",
		SourceRepresentations: []Concept{
			{
				UUID:           locationUUID,
				PrefLabel:      "Location Pref Label 2",
				Type:           "Location",
				Authority:      "ManagedLocation",
				AuthorityValue: locationUUID,
				Aliases: []string{
					"Bulgaria",
					"Bulgarie",
					"Bulgarien",
				},
				ISO31661: "BG",
			},
			{
				UUID:           anotherLocationUUID,
				PrefLabel:      "Location Pref Label 2",
				Type:           "Location",
				Authority:      "Smartlogic",
				AuthorityValue: anotherLocationUUID,
				Aliases: []string{
					"Bulgaria",
					"Bulgarie",
					"Bulgarien",
				},
			},
		},
	}
}

func init() {
	// We are initialising a lot of constraints on an empty database therefore we need the database to be fit before
	// we run tests so initialising the service will create the constraints first
	logger.InitLogger("test-concepts-rw-neo4j", "panic")

	conf := neoutils.DefaultConnectionConfig()
	conf.Transactional = false
	db, _ = neoutils.Connect(newURL(), conf)
	if db == nil {
		panic("Cannot connect to Neo4J")
	}
	conceptsDriver = NewConceptService(db)
	conceptsDriver.Initialise()

	duration := 5 * time.Second
	time.Sleep(duration)
}

func TestWriteService(t *testing.T) {
	defer cleanDB(t)

	tests := []struct {
		testName             string
		aggregatedConcept    AggregatedConcept
		otherRelatedConcepts []AggregatedConcept
		errStr               string
		updatedConcepts      ConceptChanges
	}{
		{
			testName:          "Throws validation error for invalid concept",
			aggregatedConcept: AggregatedConcept{PrefUUID: basicConceptUUID},
			errStr:            "Invalid request, no prefLabel has been supplied",
			updatedConcepts: ConceptChanges{
				UpdatedIds: []string{},
			},
		},
		{
			testName:          "Creates All Values Present for a Lone Concept",
			aggregatedConcept: getAggregatedConcept(t, "full-lone-aggregated-concept.json"),
			updatedConcepts: ConceptChanges{
				ChangedRecords: []Event{
					{
						ConceptType:   "Section",
						ConceptUUID:   basicConceptUUID,
						AggregateHash: "3491967680448072605",
						EventDetails: ConceptEvent{
							Type: UpdatedEvent,
						},
					},
				},
				UpdatedIds: []string{
					basicConceptUUID,
				},
			},
		},
		{
			testName:          "Creates All Values Present for a MembershipRole",
			aggregatedConcept: getAggregatedConcept(t, "membership-role.json"),
			updatedConcepts: ConceptChanges{
				ChangedRecords: []Event{
					{
						ConceptType:   "MembershipRole",
						ConceptUUID:   membershipRoleUUID,
						AggregateHash: "11048497011098404746",
						EventDetails: ConceptEvent{
							Type: UpdatedEvent,
						},
					},
				},
				UpdatedIds: []string{
					membershipRoleUUID,
				},
			},
		},
		{
			testName:          "Creates All Values Present for a BoardRole",
			aggregatedConcept: getAggregatedConcept(t, "board-role.json"),
			updatedConcepts: ConceptChanges{
				ChangedRecords: []Event{
					{
						ConceptType:   "BoardRole",
						ConceptUUID:   boardRoleUUID,
						AggregateHash: "1019299878214183265",
						EventDetails: ConceptEvent{
							Type: UpdatedEvent,
						},
					},
				},
				UpdatedIds: []string{
					boardRoleUUID,
				},
			},
		},
		{
			testName:          "Creates All Values Present for a Membership",
			aggregatedConcept: getAggregatedConcept(t, "membership.json"),
			updatedConcepts: ConceptChanges{
				ChangedRecords: []Event{
					{
						ConceptType:   "Membership",
						ConceptUUID:   membershipUUID,
						AggregateHash: "11776285835759245866",
						EventDetails: ConceptEvent{
							Type: UpdatedEvent,
						},
					},
				},
				UpdatedIds: []string{
					membershipUUID,
				},
			},
		},
		{
			testName:          "Creates All Values Present for a FinancialInstrument",
			aggregatedConcept: getAggregatedConcept(t, "financial-instrument.json"),
			updatedConcepts: ConceptChanges{
				ChangedRecords: []Event{
					{
						ConceptType:   "FinancialInstrument",
						ConceptUUID:   financialInstrumentUUID,
						AggregateHash: "8038897842433619119",
						EventDetails: ConceptEvent{
							Type: UpdatedEvent,
						},
					},
				},
				UpdatedIds: []string{
					financialInstrumentUUID,
				},
			},
		},
		{
			testName:          "Creates All Values Present for a Concept with a IS_RELATED_TO relationship",
			aggregatedConcept: getAggregatedConcept(t, "concept-with-related-to.json"),
			otherRelatedConcepts: []AggregatedConcept{
				getAggregatedConcept(t, "yet-another-full-lone-aggregated-concept.json"),
			},
			updatedConcepts: ConceptChanges{
				ChangedRecords: []Event{
					{
						ConceptType:   "Section",
						ConceptUUID:   basicConceptUUID,
						AggregateHash: "18135707651357636081",
						EventDetails: ConceptEvent{
							Type: UpdatedEvent,
						},
					},
				},
				UpdatedIds: []string{
					basicConceptUUID,
				},
			},
		},
		{
			testName:          "Creates All Values Present for a Concept with a IS_RELATED_TO relationship to an unknown thing",
			aggregatedConcept: getAggregatedConcept(t, "concept-with-related-to-unknown-thing.json"),
			updatedConcepts: ConceptChanges{
				ChangedRecords: []Event{
					{
						ConceptType:   "Section",
						ConceptUUID:   basicConceptUUID,
						AggregateHash: "12476507523912620922",
						EventDetails: ConceptEvent{
							Type: UpdatedEvent,
						},
					},
				},
				UpdatedIds: []string{
					basicConceptUUID,
				},
			},
		},
		{
			testName:          "Creates All Values correctly for a Concept with multiple IS_RELATED_TO relationships",
			aggregatedConcept: getAggregatedConcept(t, "concept-with-multiple-related-to.json"),
			otherRelatedConcepts: []AggregatedConcept{
				getAggregatedConcept(t, "yet-another-full-lone-aggregated-concept.json"),
			},
			updatedConcepts: ConceptChanges{
				ChangedRecords: []Event{
					{
						ConceptType:   "Section",
						ConceptUUID:   basicConceptUUID,
						AggregateHash: "17161201095476783810",
						EventDetails: ConceptEvent{
							Type: UpdatedEvent,
						},
					},
				},
				UpdatedIds: []string{
					basicConceptUUID,
				},
			},
		},
		{
			testName:          "Creates All Values Present for a Concept with a HAS_BROADER relationship",
			aggregatedConcept: getAggregatedConcept(t, "concept-with-has-broader.json"),
			otherRelatedConcepts: []AggregatedConcept{
				getAggregatedConcept(t, "yet-another-full-lone-aggregated-concept.json"),
			},
			updatedConcepts: ConceptChanges{
				ChangedRecords: []Event{
					{
						ConceptType:   "Section",
						ConceptUUID:   basicConceptUUID,
						AggregateHash: "4648752967199429213",
						EventDetails: ConceptEvent{
							Type: UpdatedEvent,
						},
					},
				},
				UpdatedIds: []string{
					basicConceptUUID,
				},
			},
		},
		{
			testName:          "Creates All Values Present for a Concept with a HAS_BROADER relationship to an unknown thing",
			aggregatedConcept: getAggregatedConcept(t, "concept-with-has-broader-to-unknown-thing.json"),
			updatedConcepts: ConceptChanges{
				ChangedRecords: []Event{
					{
						ConceptType:   "Section",
						ConceptUUID:   basicConceptUUID,
						AggregateHash: "764715006061732697",
						EventDetails: ConceptEvent{
							Type: UpdatedEvent,
						},
					},
				},
				UpdatedIds: []string{
					basicConceptUUID,
				},
			},
		},
		{
			testName:          "Creates All Values correctly for a Concept with multiple HAS_BROADER relationships",
			aggregatedConcept: getAggregatedConcept(t, "concept-with-multiple-has-broader.json"),
			otherRelatedConcepts: []AggregatedConcept{
				getAggregatedConcept(t, "yet-another-full-lone-aggregated-concept.json"),
			},
			updatedConcepts: ConceptChanges{
				ChangedRecords: []Event{
					{
						ConceptType:   "Section",
						ConceptUUID:   basicConceptUUID,
						AggregateHash: "5738107041457848041",
						EventDetails: ConceptEvent{
							Type: UpdatedEvent,
						},
					},
				},
				UpdatedIds: []string{
					basicConceptUUID,
				},
			},
		},
		{
			testName:          "Creates All Values Present for a Brand with an IMPLIED_BY relationship",
			aggregatedConcept: getAggregatedConcept(t, "brand-with-implied-by.json"),
			otherRelatedConcepts: []AggregatedConcept{
				getAggregatedConcept(t, "topic.json"),
			},
			updatedConcepts: ConceptChanges{
				ChangedRecords: []Event{
					{
						ConceptType:   "Brand",
						ConceptUUID:   brandUUID,
						AggregateHash: "508934245484827953",
						EventDetails: ConceptEvent{
							Type: UpdatedEvent,
						},
					},
				},
				UpdatedIds: []string{
					brandUUID,
				},
			},
		},
		{
			testName:          "Creates All Values Present for a Brand with an IMPLIED_BY relationship to an unknown thing",
			aggregatedConcept: getAggregatedConcept(t, "brand-with-implied-by-unknown-thing.json"),
			updatedConcepts: ConceptChanges{
				ChangedRecords: []Event{
					{
						ConceptType:   "Brand",
						ConceptUUID:   brandUUID,
						AggregateHash: "12219396850690880324",
						EventDetails: ConceptEvent{
							Type: UpdatedEvent,
						},
					},
				},
				UpdatedIds: []string{
					brandUUID,
				},
			},
		},
		{
			testName:          "Creates All Values correctly for a Brand with multiple IMPLIED_BY relationships",
			aggregatedConcept: getAggregatedConcept(t, "brand-with-multiple-implied-by.json"),
			otherRelatedConcepts: []AggregatedConcept{
				getAggregatedConcept(t, "topic.json"),
			},
			updatedConcepts: ConceptChanges{
				ChangedRecords: []Event{
					{
						ConceptType:   "Brand",
						ConceptUUID:   brandUUID,
						AggregateHash: "5490410860647006277",
						EventDetails: ConceptEvent{
							Type: UpdatedEvent,
						},
					},
				},
				UpdatedIds: []string{
					brandUUID,
				},
			},
		},
		{
			testName:          "Creates All Values correctly for multiple Brand sources with common IMPLIED_BY relationships",
			aggregatedConcept: getAggregatedConcept(t, "concorded-brand-with-multiple-implied-by.json"),
			otherRelatedConcepts: []AggregatedConcept{
				getAggregatedConcept(t, "topic.json"),
			},
			updatedConcepts: ConceptChanges{
				ChangedRecords: []Event{
					{
						ConceptType:   "Brand",
						ConceptUUID:   brandUUID,
						AggregateHash: "15982498092642642669",
						EventDetails: ConceptEvent{
							Type: UpdatedEvent,
						},
					},
					{
						ConceptType:   "Brand",
						ConceptUUID:   anotherBrandUUID,
						AggregateHash: "15982498092642642669",
						EventDetails: ConceptEvent{
							Type: UpdatedEvent,
						},
					},
					{
						ConceptType:   "Brand",
						ConceptUUID:   anotherBrandUUID,
						AggregateHash: "15982498092642642669",
						EventDetails: ConcordanceEvent{
							Type:  AddedEvent,
							OldID: anotherBrandUUID,
							NewID: brandUUID,
						},
					},
				},
				UpdatedIds: []string{
					brandUUID,
					anotherBrandUUID,
				},
			},
		},
		{
			testName:          "Creates All Values Present for a Concept with a HAS_FOCUS relationship",
			aggregatedConcept: getAggregatedConcept(t, "concept-with-has-focus.json"),
			otherRelatedConcepts: []AggregatedConcept{
				getAggregatedConcept(t, "another-topic.json"),
			},
			updatedConcepts: ConceptChanges{
				ChangedRecords: []Event{
					{
						ConceptType:   "Organisation",
						ConceptUUID:   conceptHasFocusUUID,
						AggregateHash: "1225809164348796173",
						EventDetails: ConceptEvent{
							Type: UpdatedEvent,
						},
					},
				},
				UpdatedIds: []string{
					conceptHasFocusUUID,
				},
			},
		},
		{
			testName:          "Creates All Values Present for a Brand with a HAS_FOCUS relationship",
			aggregatedConcept: getAggregatedConcept(t, "brand-with-has-focus.json"),
			otherRelatedConcepts: []AggregatedConcept{
				getAggregatedConcept(t, "another-topic.json"), getAggregatedConcept(t, "organisation.json"),
			},
			updatedConcepts: ConceptChanges{
				ChangedRecords: []Event{
					{
						ConceptType:   "Brand",
						ConceptUUID:   yetAnotherBrandUUID,
						AggregateHash: "8165632283786895173",
						EventDetails: ConceptEvent{
							Type: UpdatedEvent,
						},
					},
				},
				UpdatedIds: []string{
					yetAnotherBrandUUID,
				},
			},
		},
		{
			testName:          "Creates All Values Present for a Concept with a HAS_FOCUS relationship to an unknown thing",
			aggregatedConcept: getAggregatedConcept(t, "concept-with-has-focus-unknown-thing.json"),
			updatedConcepts: ConceptChanges{
				ChangedRecords: []Event{
					{
						ConceptType:   "Organisation",
						ConceptUUID:   conceptHasFocusUUID,
						AggregateHash: "6743874925516155226",
						EventDetails: ConceptEvent{
							Type: UpdatedEvent,
						},
					},
				},
				UpdatedIds: []string{
					conceptHasFocusUUID,
				},
			},
		},
		{
			testName:          "Creates All Values correctly for a Concept with multiple HAS_FOCUS relationships",
			aggregatedConcept: getAggregatedConcept(t, "concept-with-multiple-has-focus.json"),
			otherRelatedConcepts: []AggregatedConcept{
				getAggregatedConcept(t, "another-topic.json"), getAggregatedConcept(t, "organisation.json"),
			},
			updatedConcepts: ConceptChanges{
				ChangedRecords: []Event{
					{
						ConceptType:   "Organisation",
						ConceptUUID:   conceptHasFocusUUID,
						AggregateHash: "12038104575240509813",
						EventDetails: ConceptEvent{
							Type: UpdatedEvent,
						},
					},
				},
				UpdatedIds: []string{
					conceptHasFocusUUID,
				},
			},
		},
		{
			testName:          "Creates All Values correctly for multiple Concept sources with common HAS_FOCUS relationships",
			aggregatedConcept: getAggregatedConcept(t, "concorded-concept-with-multiple-has-focus.json"),
			otherRelatedConcepts: []AggregatedConcept{
				getAggregatedConcept(t, "topic.json"), getAggregatedConcept(t, "another-topic.json"), getAggregatedConcept(t, "organisation.json"),
			},
			updatedConcepts: ConceptChanges{
				ChangedRecords: []Event{
					{
						ConceptType:   "Organisation",
						ConceptUUID:   conceptHasFocusUUID,
						AggregateHash: "1890507434499951948",
						EventDetails: ConceptEvent{
							Type: UpdatedEvent,
						},
					},
					{
						ConceptType:   "Organisation",
						ConceptUUID:   anotherConceptHasFocusUUID,
						AggregateHash: "1890507434499951948",
						EventDetails: ConceptEvent{
							Type: UpdatedEvent,
						},
					},
					{
						ConceptType:   "Organisation",
						ConceptUUID:   anotherConceptHasFocusUUID,
						AggregateHash: "1890507434499951948",
						EventDetails: ConcordanceEvent{
							Type:  AddedEvent,
							OldID: anotherConceptHasFocusUUID,
							NewID: conceptHasFocusUUID,
						},
					},
				},
				UpdatedIds: []string{
					conceptHasFocusUUID,
					anotherConceptHasFocusUUID,
				},
			},
		},
		{
			testName:          "Creates All Values correctly for a Concept with multiple SUPERSEDED_BY relationships",
			aggregatedConcept: getAggregatedConcept(t, "concept-with-multiple-superseded-by.json"),
			updatedConcepts: ConceptChanges{
				ChangedRecords: []Event{
					{
						ConceptType:   "Section",
						ConceptUUID:   basicConceptUUID,
						AggregateHash: "8848064898313336497",
						EventDetails: ConceptEvent{
							Type: UpdatedEvent,
						},
					},
				},
				UpdatedIds: []string{
					basicConceptUUID,
				},
			},
		},
		{
			testName:          "Creates All Values Present for a Concorded Concept",
			aggregatedConcept: getAggregatedConcept(t, "full-concorded-aggregated-concept.json"),
			updatedConcepts: ConceptChanges{
				ChangedRecords: []Event{
					{
						ConceptType:   "Section",
						ConceptUUID:   anotherBasicConceptUUID,
						AggregateHash: "15229274803604197202",
						EventDetails: ConceptEvent{
							Type: UpdatedEvent,
						},
					},
					{
						ConceptType:   "Section",
						ConceptUUID:   anotherBasicConceptUUID,
						AggregateHash: "15229274803604197202",
						EventDetails: ConcordanceEvent{
							Type:  AddedEvent,
							OldID: anotherBasicConceptUUID,
							NewID: basicConceptUUID,
						},
					},
					{
						ConceptType:   "Section",
						ConceptUUID:   basicConceptUUID,
						AggregateHash: "15229274803604197202",
						EventDetails: ConceptEvent{
							Type: UpdatedEvent,
						},
					},
				},
				UpdatedIds: []string{
					anotherBasicConceptUUID,
					basicConceptUUID,
				},
			},
		},
		{
			testName:          "Creates Handles Special Characters",
			aggregatedConcept: getAggregatedConcept(t, "lone-source-system-pref-label.json"),
			updatedConcepts: ConceptChanges{
				ChangedRecords: []Event{
					{
						ConceptType:   "Section",
						ConceptUUID:   basicConceptUUID,
						AggregateHash: "13764076751366764378",
						EventDetails: ConceptEvent{
							Type: UpdatedEvent,
						},
					},
				},
				UpdatedIds: []string{
					basicConceptUUID,
				},
			},
		},
		{
			testName:          "Adding Concept with existing Identifiers fails",
			aggregatedConcept: getAggregatedConcept(t, "concorded-concept-with-conflicted-identifier.json"),
			errStr:            "already exists with label `TMEIdentifier` and property `value` = '1234'",
			updatedConcepts: ConceptChanges{
				UpdatedIds: []string{},
			},
		},
		{
			testName:          "Adding Organisation with all related locations in place works",
			aggregatedConcept: getOrganisationWithAllCountries(),
			otherRelatedConcepts: []AggregatedConcept{
				getLocationWithISO31661(),
			},
			updatedConcepts: ConceptChanges{
				ChangedRecords: []Event{
					{
						ConceptType:   "PublicCompany",
						ConceptUUID:   testOrgUUID,
						AggregateHash: "13083306498290355057",
						TransactionID: "",
						EventDetails: ConceptEvent{
							Type: UpdatedEvent,
						},
					},
				},
				UpdatedIds: []string{
					testOrgUUID,
				},
			},
		},
		{
			testName:          "Unknown Authority Should Fail",
			aggregatedConcept: getAggregatedConcept(t, "unknown-authority.json"),
			errStr:            "Invalid Request",
			updatedConcepts: ConceptChanges{
				UpdatedIds: []string{},
			},
		},
		{
			testName:          "Concord a ManagedLocation concept with ISO code to a Smartlogic concept",
			aggregatedConcept: getLocationWithISO31661AndConcordance(),
			otherRelatedConcepts: []AggregatedConcept{
				getLocationWithISO31661(),
			},
			updatedConcepts: ConceptChanges{
				ChangedRecords: []Event{
					{
						ConceptType:   "Location",
						ConceptUUID:   locationUUID,
						AggregateHash: "12723733697854321476",
						EventDetails: ConcordanceEvent{
							Type:  AddedEvent,
							OldID: locationUUID,
							NewID: anotherLocationUUID,
						},
					},
					{
						ConceptType:   "Location",
						ConceptUUID:   anotherLocationUUID,
						AggregateHash: "12723733697854321476",
						EventDetails: ConceptEvent{
							Type: UpdatedEvent,
						},
					},
				},
				UpdatedIds: []string{
					locationUUID,
					anotherLocationUUID,
				},
			},
		},
		{
			testName:          "Creates All Values Present for a NAICSIndustryClassification",
			aggregatedConcept: getAggregatedConcept(t, "naics-industry-classification.json"),
			updatedConcepts: ConceptChanges{
				ChangedRecords: []Event{
					{
						ConceptType:   "NAICSIndustryClassification",
						ConceptUUID:   naicsIndustryClassificationUUID,
						AggregateHash: "14518640806283390052",
						EventDetails: ConceptEvent{
							Type: UpdatedEvent,
						},
					},
				},
				UpdatedIds: []string{
					naicsIndustryClassificationUUID,
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.testName, func(t *testing.T) {
			defer cleanDB(t)
			// Create the related, broader than and impliedBy on concepts
			for _, relatedConcept := range test.otherRelatedConcepts {
				_, err := conceptsDriver.Write(relatedConcept, "")
				assert.NoError(t, err, "Failed to write related/broader/impliedBy concept")
			}

			updatedConcepts, err := conceptsDriver.Write(test.aggregatedConcept, "")
			if test.errStr == "" {
				assert.NoError(t, err, "Failed to write concept")
				readConceptAndCompare(t, test.aggregatedConcept, test.testName)

				sort.Slice(test.updatedConcepts.ChangedRecords, func(i, j int) bool {
					l, _ := json.Marshal(test.updatedConcepts.ChangedRecords[i])
					r, _ := json.Marshal(test.updatedConcepts.ChangedRecords[j])
					c := strings.Compare(string(l), string(r))
					return c >= 0
				})

				updatedConcepts := updatedConcepts.(ConceptChanges)
				sort.Slice(updatedConcepts.ChangedRecords, func(i, j int) bool {
					l, _ := json.Marshal(updatedConcepts.ChangedRecords[i])
					r, _ := json.Marshal(updatedConcepts.ChangedRecords[j])
					c := strings.Compare(string(l), string(r))
					return c >= 0
				})

				sort.Strings(test.updatedConcepts.UpdatedIds)
				sort.Strings(updatedConcepts.UpdatedIds)

				assert.Equal(t, test.updatedConcepts, updatedConcepts, "Test "+test.testName+" failed: Updated uuid list differs from expected")

				// Check lone nodes and leaf nodes for identifiers nodes
				// lone node
				if len(test.aggregatedConcept.SourceRepresentations) != 1 {
					// Check leaf nodes for Identifiers
					for _, leaf := range test.aggregatedConcept.SourceRepresentations {
						// We don't have Identifiers for ManagedLocation concepts
						if leaf.Authority == "ManagedLocation" {
							continue
						}
						actualValue := getIdentifierValue(t, "uuid", leaf.UUID, fmt.Sprintf("%vIdentifier", leaf.Authority))
						assert.Equal(t, leaf.AuthorityValue, actualValue, "Identifier value incorrect")
					}

					// Check Canonical node doesn't have a Identifier node
					actualValue := getIdentifierValue(t, "prefUUID", test.aggregatedConcept.PrefUUID, "UPPIdentifier")
					assert.Equal(t, "", actualValue, "Identifier nodes should not be related to Canonical Nodes")
				}
			} else {
				if err != nil {
					assert.Error(t, err, "Error was expected")
					assert.Contains(t, err.Error(), test.errStr, "Error message is not correct")
				}
				// TODO: Check these errors better
			}
		})
	}
}

func TestWriteMemberships_Organisation(t *testing.T) {
	defer cleanDB(t)

	org := getAggregatedConcept(t, "organisation.json")
	_, err := conceptsDriver.Write(org, "test_tid")
	assert.NoError(t, err, "Failed to write concept")
	readConceptAndCompare(t, org, "TestWriteMemberships_Organisation")

	upOrg := getAggregatedConcept(t, "updated-organisation.json")
	_, err = conceptsDriver.Write(upOrg, "test_tid")
	assert.NoError(t, err, "Failed to write concept")
	readConceptAndCompare(t, upOrg, "TestWriteMemberships_Organisation.Updated")
}

func TestWriteMemberships_CleansUpExisting(t *testing.T) {
	defer cleanDB(t)

	_, err := conceptsDriver.Write(getAggregatedConcept(t, "membership.json"), "test_tid")
	assert.NoError(t, err, "Failed to write membership")

	result, _, err := conceptsDriver.Read(membershipUUID, "test_tid")
	assert.NoError(t, err, "Failed to read membership")
	ab, err := json.Marshal(cleanHash(result.(AggregatedConcept)))

	originalMembership := AggregatedConcept{}
	json.Unmarshal(ab, &originalMembership)

	originalMembership = cleanConcept(originalMembership)

	assert.Equal(t, len(originalMembership.MembershipRoles), 2)
	assert.True(t, reflect.DeepEqual([]MembershipRole{membershipRole, anotherMembershipRole}, originalMembership.MembershipRoles))
	assert.Equal(t, organisationUUID, originalMembership.OrganisationUUID)
	assert.Equal(t, personUUID, originalMembership.PersonUUID)
	assert.Equal(t, "Mr", originalMembership.Salutation)
	assert.Equal(t, 2018, originalMembership.BirthYear)

	_, err = conceptsDriver.Write(getAggregatedConcept(t, "updated-membership.json"), "test_tid")
	assert.NoError(t, err, "Failed to write membership")

	updatedResult, _, err := conceptsDriver.Read(membershipUUID, "test_tid")
	assert.NoError(t, err, "Failed to read membership")
	cd, err := json.Marshal(cleanHash(updatedResult.(AggregatedConcept)))

	updatedMemebership := AggregatedConcept{}
	json.Unmarshal(cd, &updatedMemebership)

	assert.Equal(t, len(updatedMemebership.MembershipRoles), 1)
	assert.Equal(t, []MembershipRole{anotherMembershipRole}, updatedMemebership.MembershipRoles)
	assert.Equal(t, anotherOrganisationUUID, updatedMemebership.OrganisationUUID)
	assert.Equal(t, anotherPersonUUID, updatedMemebership.PersonUUID)
}

func TestWriteMemberships_FixOldData(t *testing.T) {
	defer cleanDB(t)

	queries := createNodeQueries(getConcept(t, "old-membership.json"), "", membershipUUID)
	err := db.CypherBatch(queries)
	assert.NoError(t, err, "Failed to write source")

	_, err = conceptsDriver.Write(getAggregatedConcept(t, "membership.json"), "test_tid")
	assert.NoError(t, err, "Failed to write membership")

	result, _, err := conceptsDriver.Read(membershipUUID, "test_tid")
	assert.NoError(t, err, "Failed to read membership")
	ab, err := json.Marshal(cleanHash(result.(AggregatedConcept)))

	originalMembership := AggregatedConcept{}
	json.Unmarshal(ab, &originalMembership)

	originalMembership = cleanConcept(originalMembership)

	assert.Equal(t, len(originalMembership.MembershipRoles), 2)
	assert.True(t, reflect.DeepEqual([]MembershipRole{membershipRole, anotherMembershipRole}, originalMembership.MembershipRoles))
	assert.Equal(t, organisationUUID, originalMembership.OrganisationUUID)
	assert.Equal(t, personUUID, originalMembership.PersonUUID)
}

func TestFinancialInstrumentExistingIssuedByRemoved(t *testing.T) {
	defer cleanDB(t)

	_, err := conceptsDriver.Write(getAggregatedConcept(t, "financial-instrument.json"), "test_tid")
	assert.NoError(t, err, "Failed to write financial instrument")

	_, err = conceptsDriver.Write(getAggregatedConcept(t, "financial-instrument.json"), "test_tid")
	assert.NoError(t, err, "Failed to write financial instrument")

	readConceptAndCompare(t, getAggregatedConcept(t, "financial-instrument.json"), "TestFinancialInstrumentExistingIssuedByRemoved")

	_, err = conceptsDriver.Write(getAggregatedConcept(t, "updated-financial-instrument.json"), "test_tid")
	assert.NoError(t, err, "Failed to write financial instrument")

	_, err = conceptsDriver.Write(getAggregatedConcept(t, "financial-instrument.json"), "test_tid")
	assert.NoError(t, err, "Failed to write financial instrument")

	readConceptAndCompare(t, getAggregatedConcept(t, "financial-instrument.json"), "TestFinancialInstrumentExistingIssuedByRemoved")
}

func TestFinancialInstrumentIssuerOrgRelationRemoved(t *testing.T) {
	defer cleanDB(t)

	_, err := conceptsDriver.Write(getAggregatedConcept(t, "financial-instrument.json"), "test_tid")
	assert.NoError(t, err, "Failed to write financial instrument")

	readConceptAndCompare(t, getAggregatedConcept(t, "financial-instrument.json"), "TestFinancialInstrumentExistingIssuedByRemoved")

	_, err = conceptsDriver.Write(getAggregatedConcept(t, "financial-instrument-with-same-issuer.json"), "test_tid")
	assert.NoError(t, err, "Failed to write financial instrument")

	readConceptAndCompare(t, getAggregatedConcept(t, "financial-instrument-with-same-issuer.json"), "TestFinancialInstrumentExistingIssuedByRemoved")
}

func TestWriteService_HandlingConcordance(t *testing.T) {
	tid := "test_tid"
	type testStruct struct {
		testName        string
		setUpConcept    AggregatedConcept
		testConcept     AggregatedConcept
		uuidsToCheck    []string
		returnedError   string
		updatedConcepts ConceptChanges
		customAssertion func(t *testing.T, concept AggregatedConcept)
	}
	singleConcordanceNoChangesNoUpdates := testStruct{
		testName:     "singleConcordanceNoChangesNoUpdates",
		setUpConcept: getAggregatedConcept(t, "single-concordance.json"),
		testConcept:  getAggregatedConcept(t, "single-concordance.json"),
		uuidsToCheck: []string{
			basicConceptUUID,
		},
		updatedConcepts: ConceptChanges{
			UpdatedIds: emptyList,
		},
	}
	dualConcordanceNoChangesNoUpdates := testStruct{
		testName:     "dualConcordanceNoChangesNoUpdates",
		setUpConcept: getAggregatedConcept(t, "dual-concordance.json"),
		testConcept:  getAggregatedConcept(t, "dual-concordance.json"),
		uuidsToCheck: []string{
			basicConceptUUID,
			sourceID1,
		},
		updatedConcepts: ConceptChanges{
			UpdatedIds: emptyList,
		},
	}
	singleConcordanceToDualConcordanceUpdatesBoth := testStruct{
		testName:     "singleConcordanceToDualConcordanceUpdatesBoth",
		setUpConcept: getAggregatedConcept(t, "single-concordance.json"),
		testConcept:  getAggregatedConcept(t, "dual-concordance.json"),
		uuidsToCheck: []string{
			basicConceptUUID,
			sourceID1,
		},
		updatedConcepts: ConceptChanges{
			ChangedRecords: []Event{
				{
					ConceptType:   "Brand",
					ConceptUUID:   sourceID1,
					AggregateHash: "14943420062273765673",
					TransactionID: "test_tid",
					EventDetails: ConceptEvent{
						Type: UpdatedEvent,
					},
				},
				{
					ConceptType:   "Brand",
					ConceptUUID:   sourceID1,
					AggregateHash: "14943420062273765673",
					TransactionID: "test_tid",
					EventDetails: ConcordanceEvent{
						Type:  AddedEvent,
						OldID: sourceID1,
						NewID: basicConceptUUID,
					},
				},
				{
					ConceptType:   "Brand",
					ConceptUUID:   basicConceptUUID,
					AggregateHash: "14943420062273765673",
					TransactionID: "test_tid",
					EventDetails: ConceptEvent{
						Type: UpdatedEvent,
					},
				},
			},
			UpdatedIds: []string{
				basicConceptUUID,
				sourceID1,
			},
		},
	}
	dualConcordanceToSingleConcordanceUpdatesBoth := testStruct{
		testName:     "dualConcordanceToSingleConcordanceUpdatesBoth",
		setUpConcept: getAggregatedConcept(t, "dual-concordance.json"),
		testConcept:  getAggregatedConcept(t, "single-concordance.json"),
		uuidsToCheck: []string{
			basicConceptUUID,
			sourceID1,
		},
		updatedConcepts: ConceptChanges{
			ChangedRecords: []Event{
				{
					ConceptType:   "Brand",
					ConceptUUID:   sourceID1,
					AggregateHash: "3378021760860367808",
					TransactionID: "test_tid",
					EventDetails: ConcordanceEvent{
						Type:  RemovedEvent,
						OldID: basicConceptUUID,
						NewID: sourceID1,
					},
				},
				{
					ConceptType:   "Brand",
					ConceptUUID:   basicConceptUUID,
					AggregateHash: "3378021760860367808",
					TransactionID: "test_tid",
					EventDetails: ConceptEvent{
						Type: UpdatedEvent,
					},
				},
			},
			UpdatedIds: []string{
				basicConceptUUID,
				sourceID1,
			},
		},
	}
	errorsOnAddingConcordanceOfCanonicalNode := testStruct{
		testName:      "errorsOnAddingConcordanceOfCanonicalNode",
		setUpConcept:  getAggregatedConcept(t, "dual-concordance.json"),
		testConcept:   getAggregatedConcept(t, "pref-uuid-as-source.json"),
		returnedError: "Cannot currently process this record as it will break an existing concordance with prefUuid: bbc4f575-edb3-4f51-92f0-5ce6c708d1ea",
	}
	oldCanonicalRemovedWhenSingleConcordancebecomesSource := testStruct{
		testName:     "oldCanonicalRemovedWhenSingleConcordancebecomesSource",
		setUpConcept: getAggregatedConcept(t, "single-concordance.json"),
		testConcept:  getAggregatedConcept(t, "pref-uuid-as-source.json"),
		uuidsToCheck: []string{
			anotherBasicConceptUUID,
			basicConceptUUID,
			sourceID2,
		},
		updatedConcepts: ConceptChanges{
			ChangedRecords: []Event{
				{
					ConceptType:   "Brand",
					ConceptUUID:   basicConceptUUID,
					AggregateHash: "9910869799780095755",
					TransactionID: "test_tid",
					EventDetails: ConcordanceEvent{
						Type:  AddedEvent,
						OldID: basicConceptUUID,
						NewID: anotherBasicConceptUUID,
					},
				},
				{
					ConceptType:   "Brand",
					ConceptUUID:   sourceID2,
					AggregateHash: "9910869799780095755",
					TransactionID: "test_tid",
					EventDetails: ConceptEvent{
						Type: UpdatedEvent,
					},
				},
				{
					ConceptType:   "Brand",
					ConceptUUID:   sourceID2,
					AggregateHash: "9910869799780095755",
					TransactionID: "test_tid",
					EventDetails: ConcordanceEvent{
						Type:  AddedEvent,
						OldID: sourceID2,
						NewID: anotherBasicConceptUUID,
					},
				},
				{
					ConceptType:   "Brand",
					ConceptUUID:   anotherBasicConceptUUID,
					AggregateHash: "9910869799780095755",
					TransactionID: "test_tid",
					EventDetails: ConceptEvent{
						Type: UpdatedEvent,
					},
				},
			},
			UpdatedIds: []string{
				anotherBasicConceptUUID,
				basicConceptUUID,
				sourceID2,
			},
		},
	}
	transferSourceFromOneConcordanceToAnother := testStruct{
		testName:     "transferSourceFromOneConcordanceToAnother",
		setUpConcept: getAggregatedConcept(t, "dual-concordance.json"),
		testConcept:  getAggregatedConcept(t, "transfer-source-concordance.json"),
		uuidsToCheck: []string{
			anotherBasicConceptUUID,
			sourceID1,
			basicConceptUUID,
		},
		updatedConcepts: ConceptChanges{
			ChangedRecords: []Event{
				{
					ConceptType:   "Brand",
					ConceptUUID:   sourceID1,
					AggregateHash: "10212657205424147493",
					TransactionID: "test_tid",
					EventDetails: ConcordanceEvent{
						Type:  RemovedEvent,
						OldID: basicConceptUUID,
						NewID: sourceID1,
					},
				},
				{
					ConceptType:   "Brand",
					ConceptUUID:   sourceID1,
					AggregateHash: "10212657205424147493",
					TransactionID: "test_tid",
					EventDetails: ConcordanceEvent{
						Type:  AddedEvent,
						OldID: sourceID1,
						NewID: anotherBasicConceptUUID,
					},
				},
				{
					ConceptType:   "Brand",
					ConceptUUID:   anotherBasicConceptUUID,
					AggregateHash: "10212657205424147493",
					TransactionID: "test_tid",
					EventDetails: ConceptEvent{
						Type: UpdatedEvent,
					},
				},
			},
			UpdatedIds: []string{
				anotherBasicConceptUUID,
				sourceID1,
			},
		},
	}
	addThirdSourceToDualConcordanceUpdateAll := testStruct{
		testName:     "addThirdSourceToDualConcordanceUpdateAll",
		setUpConcept: getAggregatedConcept(t, "dual-concordance.json"),
		testConcept:  getAggregatedConcept(t, "tri-concordance.json"),
		uuidsToCheck: []string{
			basicConceptUUID,
			sourceID1,
			sourceID2,
		},
		updatedConcepts: ConceptChanges{
			ChangedRecords: []Event{
				{
					ConceptType:   "Brand",
					ConceptUUID:   sourceID2,
					AggregateHash: "12197303408474725946",
					TransactionID: "test_tid",
					EventDetails: ConceptEvent{
						Type: UpdatedEvent,
					},
				},
				{
					ConceptType:   "Brand",
					ConceptUUID:   sourceID2,
					AggregateHash: "12197303408474725946",
					TransactionID: "test_tid",
					EventDetails: ConcordanceEvent{
						Type:  AddedEvent,
						OldID: sourceID2,
						NewID: basicConceptUUID,
					},
				},
				{
					ConceptType:   "Brand",
					ConceptUUID:   basicConceptUUID,
					AggregateHash: "12197303408474725946",
					TransactionID: "test_tid",
					EventDetails: ConceptEvent{
						Type: UpdatedEvent,
					},
				},
			},
			UpdatedIds: []string{
				basicConceptUUID,
				sourceID1,
				sourceID2,
			},
		},
	}
	triConcordanceToDualConcordanceUpdatesAll := testStruct{
		testName:     "triConcordanceToDualConcordanceUpdatesAll",
		setUpConcept: getAggregatedConcept(t, "tri-concordance.json"),
		testConcept:  getAggregatedConcept(t, "dual-concordance.json"),
		uuidsToCheck: []string{
			basicConceptUUID,
			sourceID1,
			sourceID2,
		},
		updatedConcepts: ConceptChanges{
			ChangedRecords: []Event{
				{
					ConceptType:   "Brand",
					ConceptUUID:   sourceID2,
					AggregateHash: "14943420062273765673",
					TransactionID: "test_tid",
					EventDetails: ConcordanceEvent{
						Type:  RemovedEvent,
						OldID: basicConceptUUID,
						NewID: sourceID2,
					},
				},
				{
					ConceptType:   "Brand",
					ConceptUUID:   basicConceptUUID,
					AggregateHash: "14943420062273765673",
					TransactionID: "test_tid",
					EventDetails: ConceptEvent{
						Type: UpdatedEvent,
					},
				},
			},
			UpdatedIds: []string{
				basicConceptUUID,
				sourceID1,
				sourceID2,
			},
		},
	}
	dataChangesOnCanonicalUpdateBoth := testStruct{
		testName:     "dataChangesOnCanonicalUpdateBoth",
		setUpConcept: getAggregatedConcept(t, "dual-concordance.json"),
		testConcept:  getAggregatedConcept(t, "updated-dual-concordance.json"),
		uuidsToCheck: []string{
			basicConceptUUID,
			sourceID1,
		},
		updatedConcepts: ConceptChanges{
			ChangedRecords: []Event{
				{
					ConceptType:   "Brand",
					ConceptUUID:   basicConceptUUID,
					AggregateHash: "14995121742957845343",
					TransactionID: "test_tid",
					EventDetails: ConceptEvent{
						Type: UpdatedEvent,
					},
				},
			},
			UpdatedIds: []string{
				basicConceptUUID,
				sourceID1,
			},
		},
	}
	singleConcordanceDeprecationChangesUpdates := testStruct{
		testName:     "singleConcordanceDeprecationChangesUpdates",
		setUpConcept: getAggregatedConcept(t, "single-concordance.json"),
		testConcept: func() AggregatedConcept {
			concept := getAggregatedConcept(t, "single-concordance.json")
			concept.IsDeprecated = true
			concept.SourceRepresentations[0].IsDeprecated = true
			return concept
		}(),
		uuidsToCheck: []string{
			basicConceptUUID,
		},
		updatedConcepts: ConceptChanges{
			ChangedRecords: []Event{
				{
					ConceptType:   "Brand",
					ConceptUUID:   basicConceptUUID,
					AggregateHash: "7666240458988060496",
					TransactionID: "test_tid",
					EventDetails: ConceptEvent{
						Type: UpdatedEvent,
					},
				},
			},
			UpdatedIds: []string{
				basicConceptUUID,
			},
		},
	}
	singleConcordanceSupersededByAddRelationship := testStruct{
		testName:     "singleConcordanceSupersededByAddRelationship",
		setUpConcept: getAggregatedConcept(t, "single-concordance.json"),
		testConcept: func() AggregatedConcept {
			concept := getAggregatedConcept(t, "single-concordance.json")
			concept.SourceRepresentations[0].SupersededByUUIDs = []string{supersededByUUID}
			return concept
		}(),
		uuidsToCheck: []string{
			basicConceptUUID,
		},
		updatedConcepts: ConceptChanges{
			ChangedRecords: []Event{
				{
					ConceptType:   "Brand",
					ConceptUUID:   basicConceptUUID,
					AggregateHash: "13406756089487254947",
					TransactionID: "test_tid",
					EventDetails: ConceptEvent{
						Type: UpdatedEvent,
					},
				},
			},
			UpdatedIds: []string{
				basicConceptUUID,
			},
		},
		customAssertion: func(t *testing.T, concept AggregatedConcept) {
			assert.Lenf(t, concept.SourceRepresentations, 1, "Test %s failed. Different number of sourceRepresentation items than expected", "singleConcordanceSupersededByRemoveRelationship")
			assert.Lenf(t, concept.SourceRepresentations[0].SupersededByUUIDs, 1, "Test %s failed. Different number of supersededByUUIDs items than expected", "singleConcordanceSupersededByRemoveRelationship")
			assert.Equalf(t, supersededByUUID, concept.SourceRepresentations[0].SupersededByUUIDs[0], "Test %s failed. Different supersededByUUID than expected", "singleConcordanceSupersededByRemoveRelationship")
		},
	}
	singleConcordanceSupersededByRemoveRelationship := testStruct{
		testName:     "singleConcordanceSupersededByRemoveRelationship",
		setUpConcept: getAggregatedConcept(t, "concept-with-superseded-by-uuids.json"),
		testConcept:  getAggregatedConcept(t, "single-concordance.json"),
		uuidsToCheck: []string{
			basicConceptUUID,
		},
		updatedConcepts: ConceptChanges{
			ChangedRecords: []Event{
				{
					ConceptType:   "Brand",
					ConceptUUID:   basicConceptUUID,
					AggregateHash: "3378021760860367808",
					TransactionID: "test_tid",
					EventDetails: ConceptEvent{
						Type: UpdatedEvent,
					},
				},
			},
			UpdatedIds: []string{
				basicConceptUUID,
			},
		},
		customAssertion: func(t *testing.T, concept AggregatedConcept) {
			assert.Lenf(t, concept.SourceRepresentations, 1, "Test %s failed. Different number of sourceRepresentation items than expected", "singleConcordanceSupersededByRemoveRelationship")
			assert.Emptyf(t, concept.SourceRepresentations[0].SupersededByUUIDs, "Test %s failed. No supersededByUUIDs content expected", "singleConcordanceSupersededByRemoveRelationship")
		},
	}

	scenarios := []testStruct{
		singleConcordanceNoChangesNoUpdates,
		dualConcordanceNoChangesNoUpdates,
		singleConcordanceToDualConcordanceUpdatesBoth,
		dualConcordanceToSingleConcordanceUpdatesBoth,
		errorsOnAddingConcordanceOfCanonicalNode,
		oldCanonicalRemovedWhenSingleConcordancebecomesSource,
		transferSourceFromOneConcordanceToAnother,
		addThirdSourceToDualConcordanceUpdateAll,
		triConcordanceToDualConcordanceUpdatesAll,
		dataChangesOnCanonicalUpdateBoth,
		singleConcordanceDeprecationChangesUpdates,
		singleConcordanceSupersededByAddRelationship,
		singleConcordanceSupersededByRemoveRelationship,
	}

	cleanDB(t)
	for _, scenario := range scenarios {
		//Write data into db, to set up test scenario
		_, err := conceptsDriver.Write(scenario.setUpConcept, tid)
		assert.NoError(t, err, "Scenario "+scenario.testName+" failed; returned unexpected error")
		verifyAggregateHashIsCorrect(t, scenario.setUpConcept, scenario.testName)
		//Overwrite data with update
		output, err := conceptsDriver.Write(scenario.testConcept, tid)
		if scenario.returnedError != "" {
			if assert.Error(t, err, "Scenario "+scenario.testName+" failed; should return an error") {
				assert.Contains(t, err.Error(), scenario.returnedError, "Scenario "+scenario.testName+" failed; returned unknown error")
			}
			// Do not check the output on error because it sometimes causes test errors
			continue
		}
		if !assert.NoError(t, err, "Scenario "+scenario.testName+" failed; returned unexpected error") {
			continue
		}

		actualChanges := output.(ConceptChanges)
		sort.Slice(actualChanges.ChangedRecords, func(i, j int) bool {
			l, _ := json.Marshal(actualChanges.ChangedRecords[i])
			r, _ := json.Marshal(actualChanges.ChangedRecords[j])
			c := strings.Compare(string(l), string(r))
			if c >= 0 {
				return true
			}
			return false
		})
		sort.Slice(scenario.updatedConcepts.ChangedRecords, func(i, j int) bool {
			l, _ := json.Marshal(scenario.updatedConcepts.ChangedRecords[i])
			r, _ := json.Marshal(scenario.updatedConcepts.ChangedRecords[j])
			c := strings.Compare(string(l), string(r))
			if c >= 0 {
				return true
			}
			return false
		})

		sort.Strings(scenario.updatedConcepts.UpdatedIds)
		sort.Strings(actualChanges.UpdatedIds)

		assert.Equal(t, scenario.updatedConcepts, actualChanges, "Scenario "+scenario.testName+" failed: Updated uuid list differs from expected")

		for _, id := range scenario.uuidsToCheck {
			conceptIf, found, err := conceptsDriver.Read(id, tid)
			concept := cleanHash(conceptIf.(AggregatedConcept))
			if found {
				assert.NotNil(t, concept, "Scenario "+scenario.testName+" failed; id: "+id+" should return a valid concept")
				assert.True(t, found, "Scenario "+scenario.testName+" failed; id: "+id+" should return a valid concept")
				assert.NoError(t, err, "Scenario "+scenario.testName+" failed; returned unexpected error")
				verifyAggregateHashIsCorrect(t, scenario.testConcept, scenario.testName)
			} else {
				assert.Equal(t, AggregatedConcept{}, concept, "Scenario "+scenario.testName+" failed; id: "+id+" should return a valid concept")
				assert.NoError(t, err, "Scenario "+scenario.testName+" failed; returned unexpected error")
			}
			if scenario.customAssertion != nil {
				scenario.customAssertion(t, concept)
			}
		}
		cleanDB(t)
	}
}

func TestMultipleConcordancesAreHandled(t *testing.T) {
	defer cleanDB(t)

	_, err := conceptsDriver.Write(getAggregatedConcept(t, "full-lone-aggregated-concept.json"), "test_tid")
	assert.NoError(t, err, "Test TestMultipleConcordancesAreHandled failed; returned unexpected error")

	_, err = conceptsDriver.Write(getAggregatedConcept(t, "lone-tme-section.json"), "test_tid")
	assert.NoError(t, err, "Test TestMultipleConcordancesAreHandled failed; returned unexpected error")

	_, err = conceptsDriver.Write(getAggregatedConcept(t, "transfer-multiple-source-concordance.json"), "test_tid")
	assert.NoError(t, err, "Test TestMultipleConcordancesAreHandled failed; returned unexpected error")

	conceptIf, found, err := conceptsDriver.Read(simpleSmartlogicTopicUUID, "test_tid")
	concept := cleanHash(conceptIf.(AggregatedConcept))
	assert.NoError(t, err, "Should be able to read concept with no problems")
	assert.True(t, found, "Concept should exist")
	assert.NotNil(t, concept, "Concept should be populated")
	readConceptAndCompare(t, getAggregatedConcept(t, "transfer-multiple-source-concordance.json"), "TestMultipleConcordancesAreHandled")
}

func TestInvalidTypesThrowError(t *testing.T) {
	invalidPrefConceptType := `MERGE (t:Thing{prefUUID:"bbc4f575-edb3-4f51-92f0-5ce6c708d1ea"}) SET t={prefUUID:"bbc4f575-edb3-4f51-92f0-5ce6c708d1ea", prefLabel:"The Best Label"} SET t:Concept:Brand:Unknown MERGE (s:Thing{uuid:"bbc4f575-edb3-4f51-92f0-5ce6c708d1ea"}) SET s={uuid:"bbc4f575-edb3-4f51-92f0-5ce6c708d1ea"} SET t:Concept:Brand MERGE (t)<-[:EQUIVALENT_TO]-(s)`
	invalidSourceConceptType := `MERGE (t:Thing{prefUUID:"4c41f314-4548-4fb6-ac48-4618fcbfa84c"}) SET t={prefUUID:"4c41f314-4548-4fb6-ac48-4618fcbfa84c", prefLabel:"The Best Label"} SET t:Concept:Brand MERGE (s:Thing{uuid:"4c41f314-4548-4fb6-ac48-4618fcbfa84c"}) SET s={uuid:"4c41f314-4548-4fb6-ac48-4618fcbfa84c"} SET t:Concept:Brand:Unknown MERGE (t)<-[:EQUIVALENT_TO]-(s)`

	type testStruct struct {
		testName         string
		prefUUID         string
		statementToWrite string
		returnedError    error
	}

	invalidPrefConceptTypeTest := testStruct{
		testName:         "invalidPrefConceptTypeTest",
		prefUUID:         basicConceptUUID,
		statementToWrite: invalidPrefConceptType,
		returnedError:    nil,
	}
	invalidSourceConceptTypeTest := testStruct{
		testName:         "invalidSourceConceptTypeTest",
		prefUUID:         anotherBasicConceptUUID,
		statementToWrite: invalidSourceConceptType,
		returnedError:    nil,
	}

	scenarios := []testStruct{invalidPrefConceptTypeTest, invalidSourceConceptTypeTest}

	for _, scenario := range scenarios {
		db.CypherBatch([]*neoism.CypherQuery{{Statement: scenario.statementToWrite}})
		aggConcept, found, err := conceptsDriver.Read(scenario.prefUUID, "")
		assert.Equal(t, AggregatedConcept{}, aggConcept, "Scenario "+scenario.testName+" failed; aggregate concept should be empty")
		assert.Equal(t, false, found, "Scenario "+scenario.testName+" failed; aggregate concept should not be returned from read")
		assert.Error(t, err, "Scenario "+scenario.testName+" failed; read of concept should return error")
		assert.Contains(t, err.Error(), "provided types are not a consistent hierarchy", "Scenario "+scenario.testName+" failed; should throw error from mapper.MostSpecificType function")
	}

	defer cleanDB(t)
}

func TestFilteringOfUniqueIds(t *testing.T) {
	type testStruct struct {
		testName     string
		firstList    map[string]string
		secondList   map[string]string
		filteredList map[string]string
	}

	emptyWhenBothListsAreEmpty := testStruct{
		testName:     "emptyWhenBothListsAreEmpty",
		firstList:    make(map[string]string),
		secondList:   make(map[string]string),
		filteredList: make(map[string]string),
	}
	emptyWhenListsAreTheIdentical := testStruct{
		testName: "emptyWhenListsAreTheIdentical",
		firstList: map[string]string{
			"1": "",
			"2": "",
			"3": "",
		},
		secondList: map[string]string{
			"1": "",
			"2": "",
			"3": "",
		},
		filteredList: make(map[string]string),
	}
	emptyWhenListsHaveSameIdsInDifferentOrder := testStruct{
		testName: "emptyWhenListsHaveSameIdsInDifferentOrder",
		firstList: map[string]string{
			"1": "",
			"2": "",
			"3": "",
		},
		secondList: map[string]string{
			"2": "",
			"3": "",
			"1": "",
		},
		filteredList: make(map[string]string),
	}
	hasCompleteFirstListWhenSecondListIsEmpty := testStruct{
		testName: "hasCompleteSecondListWhenFirstListIsEmpty",
		firstList: map[string]string{
			"1": "",
			"2": "",
			"3": "",
		},
		secondList: make(map[string]string),
		filteredList: map[string]string{
			"1": "",
			"2": "",
			"3": "",
		},
	}
	properlyFiltersWhen1IdIsUnique := testStruct{
		testName: "properlyFiltersWhen1IdIsUnique",
		firstList: map[string]string{
			"1": "",
			"2": "",
			"3": "",
		},
		secondList: map[string]string{
			"1": "",
			"2": "",
		},
		filteredList: map[string]string{
			"3": "",
		},
	}
	properlyFiltersWhen2IdsAreUnique := testStruct{
		testName: "properlyFiltersWhen2IdsAreUnique",
		firstList: map[string]string{
			"1": "",
			"2": "",
			"3": "",
		},
		secondList: map[string]string{
			"2": "",
		},
		filteredList: map[string]string{
			"1": "",
			"3": "",
		},
	}

	Scenarios := []testStruct{
		emptyWhenBothListsAreEmpty,
		emptyWhenListsAreTheIdentical,
		emptyWhenListsHaveSameIdsInDifferentOrder,
		hasCompleteFirstListWhenSecondListIsEmpty,
		properlyFiltersWhen1IdIsUnique,
		properlyFiltersWhen2IdsAreUnique,
	}

	for _, scenario := range Scenarios {
		returnedList := filterIdsThatAreUniqueToFirstMap(scenario.firstList, scenario.secondList)
		assert.Equal(t, scenario.filteredList, returnedList, "Scenario: "+scenario.testName+" returned unexpected results")
	}
}

func TestTransferConcordance(t *testing.T) {
	statement := `MERGE (a:Thing{prefUUID:"1"}) MERGE (b:Thing{uuid:"1"}) MERGE (c:Thing{uuid:"2"}) MERGE (d:Thing{uuid:"3"}) MERGE (w:Thing{prefUUID:"4"}) MERGE (y:Thing{uuid:"5"}) MERGE (j:Thing{prefUUID:"6"}) MERGE (k:Thing{uuid:"6"}) MERGE (c)-[:EQUIVALENT_TO]->(a)<-[:EQUIVALENT_TO]-(b) MERGE (w)<-[:EQUIVALENT_TO]-(d) MERGE (j)<-[:EQUIVALENT_TO]-(k)`
	db.CypherBatch([]*neoism.CypherQuery{{Statement: statement}})
	var emptyQuery []*neoism.CypherQuery
	var updatedConcept ConceptChanges

	type testStruct struct {
		testName         string
		updatedSourceIds map[string]string
		returnResult     bool
		returnedError    error
	}

	nodeHasNoConconcordance := testStruct{
		testName: "nodeHasNoConconcordance",
		updatedSourceIds: map[string]string{
			"5": "Brand"},
		returnedError: nil,
	}
	nodeHasExistingConcordanceWhichWouldCauseDataIssues := testStruct{
		testName: "nodeHasExistingConcordanceWhichNeedsToBeReWritten",
		updatedSourceIds: map[string]string{
			"1": "Brand"},
		returnedError: errors.New("Cannot currently process this record as it will break an existing concordance with prefUuid: 1"),
	}
	nodeHasExistingConcordanceWhichNeedsToBeReWritten := testStruct{
		testName: "nodeHasExistingConcordanceWhichNeedsToBeReWritten",
		updatedSourceIds: map[string]string{
			"2": "Brand"},
		returnedError: nil,
	}
	nodeHasInvalidConcordance := testStruct{
		testName: "nodeHasInvalidConcordance",
		updatedSourceIds: map[string]string{
			"3": "Brand"},
		returnedError: errors.New("This source id: 3 the only concordance to a non-matching node with prefUuid: 4"),
	}
	nodeIsPrefUUIDForExistingConcordance := testStruct{
		testName: "nodeIsPrefUuidForExistingConcordance",
		updatedSourceIds: map[string]string{
			"1": "Brand"},
		returnedError: errors.New("Cannot currently process this record as it will break an existing concordance with prefUuid: 1"),
	}
	nodeHasConcordanceToItselfPrefNodeNeedsToBeDeleted := testStruct{
		testName: "nodeHasConcordanceToItselfPrefNodeNeedsToBeDeleted",
		updatedSourceIds: map[string]string{
			"6": "Brand"},
		returnResult:  true,
		returnedError: nil,
	}

	scenarios := []testStruct{
		nodeHasNoConconcordance,
		nodeHasExistingConcordanceWhichWouldCauseDataIssues,
		nodeHasExistingConcordanceWhichNeedsToBeReWritten,
		nodeHasInvalidConcordance,
		nodeIsPrefUUIDForExistingConcordance,
		nodeHasConcordanceToItselfPrefNodeNeedsToBeDeleted,
	}

	for _, scenario := range scenarios {
		returnedQueryList, err := conceptsDriver.handleTransferConcordance(scenario.updatedSourceIds, &updatedConcept, "1234", AggregatedConcept{}, "")
		assert.Equal(t, scenario.returnedError, err, "Scenario "+scenario.testName+" returned unexpected error")
		if scenario.returnResult == true {
			assert.NotEqual(t, emptyQuery, returnedQueryList, "Scenario "+scenario.testName+" results do not match")
			break
		}
		assert.Equal(t, emptyQuery, returnedQueryList, "Scenario "+scenario.testName+" results do not match")
	}

	defer deleteSourceNodes(t, "1", "2", "3", "5", "6")
	defer deleteConcordedNodes(t, "1", "4", "6")
}

func TestTransferCanonicalMultipleConcordance(t *testing.T) {
	statement := `
	MERGE (editorialCanonical:Thing{prefUUID:"1"}) 
	MERGE (editorial:Thing{uuid:"1"}) 
	SET editorial.authority="Smartlogic"
	
	MERGE (mlCanonical:Thing{prefUUID:"2"}) 
	MERGE (ml:Thing{uuid:"2"}) 
	SET ml.authority="ManagedLocation"

	MERGE (geonames:Thing{uuid:"3"})
	SET geonames.authority="Geonames"

	MERGE (factset:Thing{uuid:"4"})
	SET factset.authority="FACTSET"

	MERGE (tme:Thing{uuid:"5"})
	SET tme.authority="TME"
	
	MERGE (editorial)-[:EQUIVALENT_TO]->(editorialCanonical)<-[:EQUIVALENT_TO]-(factset)
	MERGE (ml)-[:EQUIVALENT_TO]->(mlCanonical)<-[:EQUIVALENT_TO]-(tme)`
	db.CypherBatch([]*neoism.CypherQuery{{Statement: statement}})
	var emptyQuery []*neoism.CypherQuery
	var updatedConcept ConceptChanges

	type testStruct struct {
		testName          string
		updatedSourceIds  map[string]string
		returnResult      bool
		returnedError     error
		targetConcordance AggregatedConcept
	}
	mergeManagedLocationCanonicalWithTwoSources := testStruct{
		testName: "mergeManagedLocationCanonicalWithTwoSources",
		updatedSourceIds: map[string]string{
			"2": "Brand"},
		returnedError: nil,
		returnResult:  true,
		targetConcordance: AggregatedConcept{
			PrefUUID: "1",
			SourceRepresentations: []Concept{
				Concept{UUID: "1", Authority: "Smartlogic"},
				Concept{UUID: "4", Authority: "FACTSET"},
				Concept{UUID: "2", Authority: "ManagedLocation"},
			},
		},
	}
	mergeManagedLocationCanonicalWithTwoSourcesAndGeonames := testStruct{
		testName: "mergeManagedLocationCanonicalWithTwoSourcesAndGeonames",
		updatedSourceIds: map[string]string{
			"3": "Brand",
			"2": "Brand"},
		returnedError: nil,
		returnResult:  true,
		targetConcordance: AggregatedConcept{
			PrefUUID: "1",
			SourceRepresentations: []Concept{
				Concept{UUID: "1", Authority: "Smartlogic"},
				Concept{UUID: "4", Authority: "FACTSET"},
				Concept{UUID: "2", Authority: "ManagedLocation"},
				Concept{UUID: "5", Authority: "TME"},
			},
		},
	}
	mergeJustASourceConcordance := testStruct{
		testName: "mergeJustASourceConcordance",
		updatedSourceIds: map[string]string{
			"4": "Brand"},
		returnedError: nil,
	}

	scenarios := []testStruct{
		mergeManagedLocationCanonicalWithTwoSources,
		mergeManagedLocationCanonicalWithTwoSourcesAndGeonames,
		mergeJustASourceConcordance,
	}

	for _, scenario := range scenarios {
		returnedQueryList, err := conceptsDriver.handleTransferConcordance(scenario.updatedSourceIds, &updatedConcept, "1234", scenario.targetConcordance, "")
		assert.Equal(t, scenario.returnedError, err, "Scenario "+scenario.testName+" returned unexpected error")
		if scenario.returnResult == true {
			assert.NotEqual(t, emptyQuery, returnedQueryList, "Scenario "+scenario.testName+" results do not match")
			continue
		}
		assert.Equal(t, emptyQuery, returnedQueryList, "Scenario "+scenario.testName+" results do not match")
	}

	defer deleteSourceNodes(t, "1", "2", "3", "5")
	defer deleteConcordedNodes(t, "1", "2")
}

func TestObjectFieldValidationCorrectlyWorks(t *testing.T) {
	defer cleanDB(t)

	type testStruct struct {
		testName      string
		aggConcept    AggregatedConcept
		returnedError string
	}

	aggregateConceptNoPrefLabel := AggregatedConcept{
		PrefUUID: basicConceptUUID,
	}
	aggregateConceptNoType := AggregatedConcept{
		PrefUUID:  basicConceptUUID,
		PrefLabel: "The Best Label",
	}
	aggregateConceptNoSourceReps := AggregatedConcept{
		PrefUUID:  basicConceptUUID,
		PrefLabel: "The Best Label",
		Type:      "Brand",
	}
	sourceRepNoPrefLabel := AggregatedConcept{
		PrefUUID:  basicConceptUUID,
		PrefLabel: "The Best Label",
		Type:      "Brand",
		SourceRepresentations: []Concept{
			{
				UUID:           basicConceptUUID,
				Type:           "Brand",
				AuthorityValue: "123456-UPP",
				Authority:      "UPP",
			},
		},
	}
	sourceRepNoType := AggregatedConcept{
		PrefUUID:  basicConceptUUID,
		PrefLabel: "The Best Label",
		Type:      "Brand",
		SourceRepresentations: []Concept{
			{
				UUID:      basicConceptUUID,
				PrefLabel: "The Best Label",
			},
		},
	}
	sourceRepNoAuthorityValue := AggregatedConcept{
		PrefUUID:  basicConceptUUID,
		PrefLabel: "The Best Label",
		Type:      "Brand",
		SourceRepresentations: []Concept{
			{
				UUID:      basicConceptUUID,
				PrefLabel: "The Best Label",
				Type:      "Brand",
			},
		},
	}
	returnNoError := AggregatedConcept{
		PrefUUID:  basicConceptUUID,
		PrefLabel: "The Best Label",
		Type:      "Brand",
		SourceRepresentations: []Concept{
			{
				UUID:           basicConceptUUID,
				PrefLabel:      "The Best Label",
				Type:           "Brand",
				AuthorityValue: "123456-UPP",
			},
		},
	}
	testAggregateConceptNoPrefLabel := testStruct{
		testName:      "testAggregateConceptNoPrefLabel",
		aggConcept:    aggregateConceptNoPrefLabel,
		returnedError: "Invalid request, no prefLabel has been supplied",
	}
	testAggregateConceptNoType := testStruct{
		testName:      "testAggregateConceptNoType",
		aggConcept:    aggregateConceptNoType,
		returnedError: "Invalid request, no type has been supplied",
	}
	testAggregateConceptNoSourceReps := testStruct{
		testName:      "testAggregateConceptNoSourceReps",
		aggConcept:    aggregateConceptNoSourceReps,
		returnedError: "Invalid request, no sourceRepresentation has been supplied",
	}
	testSourceRepNoPrefLabel := testStruct{
		testName:   "testSourceRepNoPrefLabel",
		aggConcept: sourceRepNoPrefLabel,
	}
	testSourceRepNoType := testStruct{
		testName:      "testSourceRepNoType",
		aggConcept:    sourceRepNoType,
		returnedError: "Invalid request, no sourceRepresentation.type has been supplied",
	}
	testSourceRepNoAuthorityValue := testStruct{
		testName:      "testSourceRepNoAuthorityValue",
		aggConcept:    sourceRepNoAuthorityValue,
		returnedError: "Invalid request, no sourceRepresentation.authorityValue has been supplied",
	}
	returnNoErrorTest := testStruct{
		testName:      "returnNoErrorTest",
		aggConcept:    returnNoError,
		returnedError: "",
	}

	scenarios := []testStruct{
		testAggregateConceptNoPrefLabel,
		testAggregateConceptNoType,
		testAggregateConceptNoSourceReps,
		testSourceRepNoPrefLabel,
		testSourceRepNoType,
		testSourceRepNoAuthorityValue,
		returnNoErrorTest,
	}

	for _, scenario := range scenarios {
		err := validateObject(scenario.aggConcept, "transaction_id")
		if err != nil {
			assert.Contains(t, err.Error(), scenario.returnedError, scenario.testName)
		} else {
			assert.NoError(t, err, scenario.testName)
		}
	}
}

func TestWriteLocation(t *testing.T) {
	defer cleanDB(t)

	location := getLocation()
	_, err := conceptsDriver.Write(location, "test_tid")
	assert.NoError(t, err, "Failed to write concept")
	readConceptAndCompare(t, location, "TestWriteLocation")

	locationISO31661 := getLocationWithISO31661()
	_, err = conceptsDriver.Write(locationISO31661, "test_tid")
	assert.NoError(t, err, "Failed to write concept")
	readConceptAndCompare(t, locationISO31661, "TestWriteLocationISO31661")
}

func readConceptAndCompare(t *testing.T, payload AggregatedConcept, testName string) {
	actualIf, found, err := conceptsDriver.Read(payload.PrefUUID, "")
	actual := actualIf.(AggregatedConcept)

	actual = cleanHash(cleanConcept(actual))
	clean := cleanSourceProperties(payload)
	expected := cleanHash(cleanConcept(clean))

	assert.Equal(t, expected, actual, fmt.Sprintf("Test %s failed: Concepts were not equal", testName))
	assert.NoError(t, err, fmt.Sprintf("Test %s failed: Unexpected Error occurred", testName))
	assert.True(t, found, fmt.Sprintf("Test %s failed: Concept has not been found", testName))
}

func newURL() string {
	url := os.Getenv("NEO4J_TEST_URL")
	if url == "" {
		url = "http://localhost:7474/db/data"
	}
	return url
}

func cleanDB(t *testing.T) {
	cleanSourceNodes(t,
		parentUUID,
		anotherBasicConceptUUID,
		basicConceptUUID,
		sourceID1,
		sourceID2,
		sourceID3,
		unknownThingUUID,
		anotherUnknownThingUUID,
		yetAnotherBasicConceptUUID,
		membershipRole.RoleUUID,
		personUUID,
		organisationUUID,
		membershipUUID,
		anotherMembershipRole.RoleUUID,
		anotherOrganisationUUID,
		anotherPersonUUID,
		simpleSmartlogicTopicUUID,
		boardRoleUUID,
		financialInstrumentSameIssuerUUID,
		financialInstrumentUUID,
		financialOrgUUID,
		anotherFinancialOrgUUID,
		parentOrgUUID,
		supersededByUUID,
		testOrgUUID,
		locationUUID,
		anotherLocationUUID,
		brandUUID,
		anotherBrandUUID,
		yetAnotherBrandUUID,
		topicUUID,
		anotherTopicUUID,
		conceptHasFocusUUID,
		anotherConceptHasFocusUUID,
		naicsIndustryClassificationUUID,
	)
	deleteSourceNodes(t,
		parentUUID,
		anotherBasicConceptUUID,
		basicConceptUUID,
		sourceID1,
		sourceID2,
		sourceID3,
		unknownThingUUID,
		anotherUnknownThingUUID,
		yetAnotherBasicConceptUUID,
		membershipRole.RoleUUID,
		personUUID,
		organisationUUID,
		membershipUUID,
		anotherMembershipRole.RoleUUID,
		anotherOrganisationUUID,
		anotherPersonUUID,
		simpleSmartlogicTopicUUID,
		boardRoleUUID,
		financialInstrumentSameIssuerUUID,
		financialInstrumentUUID,
		financialOrgUUID,
		anotherFinancialOrgUUID,
		parentOrgUUID,
		supersededByUUID,
		testOrgUUID,
		locationUUID,
		anotherLocationUUID,
		brandUUID,
		anotherBrandUUID,
		yetAnotherBrandUUID,
		topicUUID,
		anotherTopicUUID,
		conceptHasFocusUUID,
		anotherConceptHasFocusUUID,
		naicsIndustryClassificationUUID,
	)
	deleteConcordedNodes(t,
		parentUUID,
		basicConceptUUID,
		anotherBasicConceptUUID,
		sourceID1,
		sourceID2,
		sourceID3,
		unknownThingUUID,
		anotherUnknownThingUUID,
		yetAnotherBasicConceptUUID,
		membershipRole.RoleUUID,
		personUUID,
		organisationUUID,
		membershipUUID,
		anotherMembershipRole.RoleUUID,
		anotherOrganisationUUID,
		anotherPersonUUID,
		simpleSmartlogicTopicUUID,
		boardRoleUUID,
		financialInstrumentSameIssuerUUID,
		financialInstrumentUUID,
		financialOrgUUID,
		anotherFinancialOrgUUID,
		parentOrgUUID,
		supersededByUUID,
		testOrgUUID,
		locationUUID,
		anotherLocationUUID,
		brandUUID,
		anotherBrandUUID,
		yetAnotherBrandUUID,
		topicUUID,
		anotherTopicUUID,
		conceptHasFocusUUID,
		anotherConceptHasFocusUUID,
		naicsIndustryClassificationUUID,
	)
}

func deleteSourceNodes(t *testing.T, uuids ...string) {
	qs := make([]*neoism.CypherQuery, len(uuids))
	for i, uuid := range uuids {
		qs[i] = &neoism.CypherQuery{
			Statement: fmt.Sprintf(`
			MATCH (a:Thing {uuid: "%s"})
			OPTIONAL MATCH (a)-[rel:IDENTIFIES]-(i)
			DETACH DELETE rel, i, a`, uuid)}
	}
	err := db.CypherBatch(qs)
	assert.NoError(t, err, "Error executing clean up cypher")
}

func cleanSourceNodes(t *testing.T, uuids ...string) {
	qs := make([]*neoism.CypherQuery, len(uuids))
	for i, uuid := range uuids {
		qs[i] = &neoism.CypherQuery{
			Statement: fmt.Sprintf(`
			MATCH (a:Thing {uuid: "%s"})
			OPTIONAL MATCH (a)-[rel:IDENTIFIES]-(i)
			OPTIONAL MATCH (a)-[hp:HAS_PARENT]-(p)
			DELETE rel, hp, i`, uuid)}
	}
	err := db.CypherBatch(qs)
	assert.NoError(t, err, "Error executing clean up cypher")
}

func deleteConcordedNodes(t *testing.T, uuids ...string) {
	qs := make([]*neoism.CypherQuery, len(uuids))
	for i, uuid := range uuids {
		qs[i] = &neoism.CypherQuery{
			Statement: fmt.Sprintf(`
			MATCH (a:Thing {prefUUID: "%s"})
			OPTIONAL MATCH (a)-[rel]-(i)
			DELETE rel, i, a`, uuid)}
	}
	err := db.CypherBatch(qs)
	assert.NoError(t, err, "Error executing clean up cypher")
}

func getIdentifierValue(t *testing.T, uuidPropertyName string, uuid string, label string) string {
	var results []struct {
		Value string `json:"i.value"`
	}

	query := &neoism.CypherQuery{
		Statement: fmt.Sprintf(`
			match (c:Concept {%s :{uuid}})-[r:IDENTIFIES]-(i:%s) return i.value
		`, uuidPropertyName, label),
		Parameters: map[string]interface{}{
			"uuid": uuid,
		},
		Result: &results,
	}
	err := db.CypherBatch([]*neoism.CypherQuery{query})
	assert.NoError(t, err, fmt.Sprintf("Error while retrieving %s", label))

	if len(results) > 0 {
		return results[0].Value
	}
	return ""
}

func verifyAggregateHashIsCorrect(t *testing.T, concept AggregatedConcept, testName string) {
	var results []struct {
		Hash string `json:"a.aggregateHash"`
	}

	query := &neoism.CypherQuery{
		Statement: `
			MATCH (a:Thing {prefUUID: {uuid}})
			RETURN a.aggregateHash`,
		Parameters: map[string]interface{}{
			"uuid": concept.PrefUUID,
		},
		Result: &results,
	}
	err := db.CypherBatch([]*neoism.CypherQuery{query})
	assert.NoError(t, err, fmt.Sprintf("Error while retrieving concept hash"))

	conceptHash, _ := hashstructure.Hash(cleanSourceProperties(concept), nil)
	hashAsString := strconv.FormatUint(conceptHash, 10)
	assert.Equal(t, hashAsString, results[0].Hash, fmt.Sprintf("Test %s failed: Concept hash %s and stored record %s are not equal!", testName, hashAsString, results[0].Hash))
}
