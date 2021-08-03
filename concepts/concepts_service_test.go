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
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/jmcvetta/neoism"
	"github.com/mitchellh/hashstructure"
	"github.com/stretchr/testify/assert"

	"github.com/Financial-Times/concepts-rw-neo4j/ontology"
	logger "github.com/Financial-Times/go-logger"
	"github.com/Financial-Times/neo-utils-go/neoutils"
)

//all uuids to be cleaned from DB
const (
	basicConceptUUID           = "bbc4f575-edb3-4f51-92f0-5ce6c708d1ea"
	anotherBasicConceptUUID    = "4c41f314-4548-4fb6-ac48-4618fcbfa84c"
	yetAnotherBasicConceptUUID = "f7e3fe2d-7496-4d42-b19f-378094efd263"
	simpleSmartlogicTopicUUID  = "abd38d90-2152-11e8-9ac1-da24cd01f044"
	parentUUID                 = "2ef39c2a-da9c-4263-8209-ebfd490d3101"

	boardRoleUUID                          = "aa9ef631-c025-43b2-b0ce-d78d394cc6e6"
	membershipRoleUUID                     = "f807193d-337b-412f-b32c-afa14b385819"
	organisationUUID                       = "7f40d291-b3cb-47c4-9bce-18413e9350cf"
	personUUID                             = "35946807-0205-4fc1-8516-bb1ae141659b"
	financialInstrumentUUID                = "475b7b59-66d5-47e2-a273-adc3d1ba8286"
	financialInstrumentSameIssuerUUID      = "08c6066c-9356-4e96-abd5-9a4f3726724a"
	financialOrgUUID                       = "4290f028-05e9-4c2d-9f11-61ec59ba081a"
	anotherFinancialOrgUUID                = "230e3a74-694a-4d94-8294-6a45ec1ced26"
	membershipUUID                         = "cbadd9a7-5da9-407a-a5ec-e379460991f2"
	anotherOrganisationUUID                = "7ccf2673-2ec0-4b42-b69e-9a2460b945c6"
	anotherPersonUUID                      = "69a8e241-2bfb-4aed-a441-8489d813c5f7"
	testOrgUUID                            = "c28fa0b4-4245-11e8-842f-0ed5f89f718b"
	parentOrgUUID                          = "c001ee9c-94c5-11e8-8f42-da24cd01f044"
	locationUUID                           = "82cba3ce-329b-3010-b29d-4282a215889f"
	anotherLocationUUID                    = "6b683eff-56c3-43d9-acfc-7511d974fc01"
	organisationWithNAICSUUID              = "b4ddd5a5-0b6c-4dc2-bb75-3eb40c1b05ed"
	naicsIndustryClassificationUUID        = "38ee195d-ebdd-48a9-af4b-c8a322e7b04d"
	naicsIndustryClassificationAnotherUUID = "49da878c-67ce-4343-9a09-a4a767e584a2"

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
	membershipRole = ontology.MembershipRole{
		RoleUUID:        "f807193d-337b-412f-b32c-afa14b385819",
		InceptionDate:   "2016-01-01",
		TerminationDate: "2017-02-02",
	}
	anotherMembershipRole = ontology.MembershipRole{
		RoleUUID:      "fe94adc6-ca44-438f-ad8f-0188d4a74987",
		InceptionDate: "2011-06-27",
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
func getAggregatedConcept(t *testing.T, name string) ontology.AggregatedConcept {
	ac := ontology.AggregatedConcept{}
	err := json.Unmarshal(helperLoadBytes(t, name), &ac)
	if err != nil {
		t.Fatal(err)
	}
	return ac
}

func getOrganisationWithAllCountries() ontology.AggregatedConcept {
	return ontology.AggregatedConcept{
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
		SourceRepresentations: []ontology.Concept{
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

func getConcept(t *testing.T, name string) ontology.Concept {
	c := ontology.Concept{}
	err := json.Unmarshal(helperLoadBytes(t, name), &c)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func getLocation() ontology.AggregatedConcept {
	return ontology.AggregatedConcept{
		PrefUUID:  locationUUID,
		PrefLabel: "Location Pref Label",
		Type:      "Location",
		SourceRepresentations: []ontology.Concept{{
			UUID:           locationUUID,
			PrefLabel:      "Location Pref Label",
			Type:           "Location",
			Authority:      "ManagedLocation",
			AuthorityValue: locationUUID,
		}},
	}
}

func getLocationWithISO31661() ontology.AggregatedConcept {
	return ontology.AggregatedConcept{
		PrefUUID:  locationUUID,
		PrefLabel: "Location Pref Label 2",
		Type:      "Location",
		Aliases: []string{
			"Bulgaria",
			"Bulgarie",
			"Bulgarien",
		},
		ISO31661: "BG",
		SourceRepresentations: []ontology.Concept{{
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

func getLocationWithISO31661AndConcordance() ontology.AggregatedConcept {
	return ontology.AggregatedConcept{
		PrefUUID:  anotherLocationUUID,
		PrefLabel: "Location Pref Label 2",
		Type:      "Location",
		Aliases: []string{
			"Bulgaria",
			"Bulgarie",
			"Bulgarien",
		},
		ISO31661: "BG",
		SourceRepresentations: []ontology.Concept{
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
		aggregatedConcept    ontology.AggregatedConcept
		otherRelatedConcepts []ontology.AggregatedConcept
		writtenNotReadFields []string
		errStr               string
		updatedConcepts      ConceptChanges
	}{
		{
			testName:          "Throws validation error for invalid concept",
			aggregatedConcept: ontology.AggregatedConcept{PrefUUID: basicConceptUUID},
			errStr:            "invalid request, no prefLabel has been supplied",
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
						AggregateHash: "11962703960608256906",
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
						AggregateHash: "10926600137775579722",
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
						AggregateHash: "632127633281490148",
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
						AggregateHash: "2583709379931978683",
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
						AggregateHash: "740867886434218715",
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
			otherRelatedConcepts: []ontology.AggregatedConcept{
				getAggregatedConcept(t, "yet-another-full-lone-aggregated-concept.json"),
			},
			updatedConcepts: ConceptChanges{
				ChangedRecords: []Event{
					{
						ConceptType:   "Section",
						ConceptUUID:   basicConceptUUID,
						AggregateHash: "15778472151266496724",
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
						AggregateHash: "16664714450548061902",
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
			otherRelatedConcepts: []ontology.AggregatedConcept{
				getAggregatedConcept(t, "yet-another-full-lone-aggregated-concept.json"),
			},
			updatedConcepts: ConceptChanges{
				ChangedRecords: []Event{
					{
						ConceptType:   "Section",
						ConceptUUID:   basicConceptUUID,
						AggregateHash: "16267515993296956365",
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
			otherRelatedConcepts: []ontology.AggregatedConcept{
				getAggregatedConcept(t, "yet-another-full-lone-aggregated-concept.json"),
			},
			updatedConcepts: ConceptChanges{
				ChangedRecords: []Event{
					{
						ConceptType:   "Section",
						ConceptUUID:   basicConceptUUID,
						AggregateHash: "10136463773554381892",
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
						AggregateHash: "16881221654944969347",
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
			otherRelatedConcepts: []ontology.AggregatedConcept{
				getAggregatedConcept(t, "yet-another-full-lone-aggregated-concept.json"),
			},
			updatedConcepts: ConceptChanges{
				ChangedRecords: []Event{
					{
						ConceptType:   "Section",
						ConceptUUID:   basicConceptUUID,
						AggregateHash: "10611495773105789085",
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
			otherRelatedConcepts: []ontology.AggregatedConcept{
				getAggregatedConcept(t, "topic.json"),
			},
			updatedConcepts: ConceptChanges{
				ChangedRecords: []Event{
					{
						ConceptType:   "Brand",
						ConceptUUID:   brandUUID,
						AggregateHash: "11685880447608683841",
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
						AggregateHash: "14718680089606136873",
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
			otherRelatedConcepts: []ontology.AggregatedConcept{
				getAggregatedConcept(t, "topic.json"),
			},
			updatedConcepts: ConceptChanges{
				ChangedRecords: []Event{
					{
						ConceptType:   "Brand",
						ConceptUUID:   brandUUID,
						AggregateHash: "11718320835668332357",
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
			otherRelatedConcepts: []ontology.AggregatedConcept{
				getAggregatedConcept(t, "topic.json"),
			},
			updatedConcepts: ConceptChanges{
				ChangedRecords: []Event{
					{
						ConceptType:   "Brand",
						ConceptUUID:   brandUUID,
						AggregateHash: "13280667139926404744",
						EventDetails: ConceptEvent{
							Type: UpdatedEvent,
						},
					},
					{
						ConceptType:   "Brand",
						ConceptUUID:   anotherBrandUUID,
						AggregateHash: "13280667139926404744",
						EventDetails: ConceptEvent{
							Type: UpdatedEvent,
						},
					},
					{
						ConceptType:   "Brand",
						ConceptUUID:   anotherBrandUUID,
						AggregateHash: "13280667139926404744",
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
			otherRelatedConcepts: []ontology.AggregatedConcept{
				getAggregatedConcept(t, "another-topic.json"),
			},
			updatedConcepts: ConceptChanges{
				ChangedRecords: []Event{
					{
						ConceptType:   "Organisation",
						ConceptUUID:   conceptHasFocusUUID,
						AggregateHash: "13449440537497481455",
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
			otherRelatedConcepts: []ontology.AggregatedConcept{
				getAggregatedConcept(t, "another-topic.json"), getAggregatedConcept(t, "organisation.json"),
			},
			updatedConcepts: ConceptChanges{
				ChangedRecords: []Event{
					{
						ConceptType:   "Brand",
						ConceptUUID:   yetAnotherBrandUUID,
						AggregateHash: "9392858139411790333",
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
						AggregateHash: "16540497880121135813",
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
			otherRelatedConcepts: []ontology.AggregatedConcept{
				getAggregatedConcept(t, "another-topic.json"), getAggregatedConcept(t, "organisation.json"),
			},
			updatedConcepts: ConceptChanges{
				ChangedRecords: []Event{
					{
						ConceptType:   "Organisation",
						ConceptUUID:   conceptHasFocusUUID,
						AggregateHash: "3410796614082946092",
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
			otherRelatedConcepts: []ontology.AggregatedConcept{
				getAggregatedConcept(t, "topic.json"), getAggregatedConcept(t, "another-topic.json"), getAggregatedConcept(t, "organisation.json"),
			},
			updatedConcepts: ConceptChanges{
				ChangedRecords: []Event{
					{
						ConceptType:   "Organisation",
						ConceptUUID:   conceptHasFocusUUID,
						AggregateHash: "12703582309208260040",
						EventDetails: ConceptEvent{
							Type: UpdatedEvent,
						},
					},
					{
						ConceptType:   "Organisation",
						ConceptUUID:   anotherConceptHasFocusUUID,
						AggregateHash: "12703582309208260040",
						EventDetails: ConceptEvent{
							Type: UpdatedEvent,
						},
					},
					{
						ConceptType:   "Organisation",
						ConceptUUID:   anotherConceptHasFocusUUID,
						AggregateHash: "12703582309208260040",
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
						AggregateHash: "4024699536717513094",
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
						AggregateHash: "15832747680085628960",
						EventDetails: ConceptEvent{
							Type: UpdatedEvent,
						},
					},
					{
						ConceptType:   "Section",
						ConceptUUID:   anotherBasicConceptUUID,
						AggregateHash: "15832747680085628960",
						EventDetails: ConcordanceEvent{
							Type:  AddedEvent,
							OldID: anotherBasicConceptUUID,
							NewID: basicConceptUUID,
						},
					},
					{
						ConceptType:   "Section",
						ConceptUUID:   basicConceptUUID,
						AggregateHash: "15832747680085628960",
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
						AggregateHash: "3185186027352954335",
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
			testName:          "Adding Organisation with all related locations in place works",
			aggregatedConcept: getOrganisationWithAllCountries(),
			otherRelatedConcepts: []ontology.AggregatedConcept{
				getLocationWithISO31661(),
			},
			updatedConcepts: ConceptChanges{
				ChangedRecords: []Event{
					{
						ConceptType:   "PublicCompany",
						ConceptUUID:   testOrgUUID,
						AggregateHash: "1083384572460927160",
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
			errStr:            "unknown authority",
			updatedConcepts: ConceptChanges{
				UpdatedIds: []string{},
			},
		},
		{
			testName:          "Concord a ManagedLocation concept with ISO code to a Smartlogic concept",
			aggregatedConcept: getLocationWithISO31661AndConcordance(),
			otherRelatedConcepts: []ontology.AggregatedConcept{
				getLocationWithISO31661(),
			},
			updatedConcepts: ConceptChanges{
				ChangedRecords: []Event{
					{
						ConceptType:   "Location",
						ConceptUUID:   locationUUID,
						AggregateHash: "14673293395653141343",
						EventDetails: ConcordanceEvent{
							Type:  AddedEvent,
							OldID: locationUUID,
							NewID: anotherLocationUUID,
						},
					},
					{
						ConceptType:   "Location",
						ConceptUUID:   anotherLocationUUID,
						AggregateHash: "14673293395653141343",
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
						AggregateHash: "1773173587993451366",
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
		{
			testName:          "Creates All Values correctly for Organisation with HAS_INDUSTRY_CLASSIFICATION relationships",
			aggregatedConcept: getAggregatedConcept(t, "organisation-with-naics.json"),
			otherRelatedConcepts: []ontology.AggregatedConcept{
				getAggregatedConcept(t, "naics-industry-classification.json"), getAggregatedConcept(t, "naics-industry-classification-internet.json"),
			},
			updatedConcepts: ConceptChanges{
				ChangedRecords: []Event{
					{
						ConceptType:   "PublicCompany",
						ConceptUUID:   organisationWithNAICSUUID,
						AggregateHash: "12721802568035065567",
						EventDetails: ConceptEvent{
							Type: UpdatedEvent,
						},
					},
				},
				UpdatedIds: []string{
					organisationWithNAICSUUID,
				},
			},
		},
		{
			testName:             "Creates All Values correctly for Organisation with HAS_INDUSTRY_CLASSIFICATION relationships to unknown",
			aggregatedConcept:    getAggregatedConcept(t, "organisation-with-naics-unknown.json"),
			writtenNotReadFields: []string{"NAICSIndustryClassifications"},
			updatedConcepts: ConceptChanges{
				ChangedRecords: []Event{
					{
						ConceptType:   "PublicCompany",
						ConceptUUID:   organisationWithNAICSUUID,
						AggregateHash: "13749833964494005",
						EventDetails: ConceptEvent{
							Type: UpdatedEvent,
						},
					},
				},
				UpdatedIds: []string{
					organisationWithNAICSUUID,
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
				readConceptAndCompare(t, test.aggregatedConcept, test.testName, test.writtenNotReadFields...)

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

				cmpOpts := cmpopts.IgnoreFields(Event{}, "AggregateHash")
				if !cmp.Equal(test.updatedConcepts, updatedConcepts, cmpOpts) {
					t.Errorf("Test %s failed: Updated uuid list differs from expected:\n%s", test.testName, cmp.Diff(test.updatedConcepts, updatedConcepts, cmpOpts))
				}
			} else {
				assert.Error(t, err, "Error was expected")
				assert.Contains(t, err.Error(), test.errStr, "Error message is not correct")
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
	ab, _ := json.Marshal(cleanHash(result.(ontology.AggregatedConcept)))

	originalMembership := ontology.AggregatedConcept{}
	json.Unmarshal(ab, &originalMembership)

	originalMembership = cleanConcept(originalMembership)

	assert.Equal(t, len(originalMembership.MembershipRoles), 2)
	assert.True(t, reflect.DeepEqual([]ontology.MembershipRole{membershipRole, anotherMembershipRole}, originalMembership.MembershipRoles))
	assert.Equal(t, organisationUUID, originalMembership.OrganisationUUID)
	assert.Equal(t, personUUID, originalMembership.PersonUUID)
	assert.Equal(t, "Mr", originalMembership.Salutation)
	assert.Equal(t, 2018, originalMembership.BirthYear)

	_, err = conceptsDriver.Write(getAggregatedConcept(t, "updated-membership.json"), "test_tid")
	assert.NoError(t, err, "Failed to write membership")

	updatedResult, _, err := conceptsDriver.Read(membershipUUID, "test_tid")
	assert.NoError(t, err, "Failed to read membership")
	cd, _ := json.Marshal(cleanHash(updatedResult.(ontology.AggregatedConcept)))

	updatedMemebership := ontology.AggregatedConcept{}
	json.Unmarshal(cd, &updatedMemebership)

	assert.Equal(t, len(updatedMemebership.MembershipRoles), 1)
	assert.Equal(t, []ontology.MembershipRole{anotherMembershipRole}, updatedMemebership.MembershipRoles)
	assert.Equal(t, anotherOrganisationUUID, updatedMemebership.OrganisationUUID)
	assert.Equal(t, anotherPersonUUID, updatedMemebership.PersonUUID)
}

func TestWriteMemberships_FixOldData(t *testing.T) {
	defer cleanDB(t)

	oldConcept := getConcept(t, "old-membership.json")
	newConcept := ontology.TransformToNewSourceConcept(oldConcept)
	queries := createNodeQueries(newConcept, membershipUUID)
	err := db.CypherBatch(queries)
	assert.NoError(t, err, "Failed to write source")

	_, err = conceptsDriver.Write(getAggregatedConcept(t, "membership.json"), "test_tid")
	assert.NoError(t, err, "Failed to write membership")

	result, _, err := conceptsDriver.Read(membershipUUID, "test_tid")
	assert.NoError(t, err, "Failed to read membership")
	ab, _ := json.Marshal(cleanHash(result.(ontology.AggregatedConcept)))

	originalMembership := ontology.AggregatedConcept{}
	json.Unmarshal(ab, &originalMembership)

	originalMembership = cleanConcept(originalMembership)

	assert.Equal(t, len(originalMembership.MembershipRoles), 2)
	assert.True(t, reflect.DeepEqual([]ontology.MembershipRole{membershipRole, anotherMembershipRole}, originalMembership.MembershipRoles))
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
		setUpConcept    ontology.AggregatedConcept
		testConcept     ontology.AggregatedConcept
		uuidsToCheck    []string
		returnedError   string
		updatedConcepts ConceptChanges
		customAssertion func(t *testing.T, concept ontology.AggregatedConcept)
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
					AggregateHash: "13050067908998386737",
					TransactionID: "test_tid",
					EventDetails: ConceptEvent{
						Type: UpdatedEvent,
					},
				},
				{
					ConceptType:   "Brand",
					ConceptUUID:   sourceID1,
					AggregateHash: "13050067908998386737",
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
					AggregateHash: "13050067908998386737",
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
					AggregateHash: "2137764349277562661",
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
					AggregateHash: "2137764349277562661",
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
					AggregateHash: "5757717515788965658",
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
					AggregateHash: "5757717515788965658",
					TransactionID: "test_tid",
					EventDetails: ConceptEvent{
						Type: UpdatedEvent,
					},
				},
				{
					ConceptType:   "Brand",
					ConceptUUID:   sourceID2,
					AggregateHash: "5757717515788965658",
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
					AggregateHash: "5757717515788965658",
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
					AggregateHash: "7725347417335166648",
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
					AggregateHash: "7725347417335166648",
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
					AggregateHash: "7725347417335166648",
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
					AggregateHash: "1825428118302879667",
					TransactionID: "test_tid",
					EventDetails: ConceptEvent{
						Type: UpdatedEvent,
					},
				},
				{
					ConceptType:   "Brand",
					ConceptUUID:   sourceID2,
					AggregateHash: "1825428118302879667",
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
					AggregateHash: "1825428118302879667",
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
					AggregateHash: "13050067908998386737",
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
					AggregateHash: "13050067908998386737",
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
					AggregateHash: "411480971478777011",
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
		testConcept: func() ontology.AggregatedConcept {
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
					AggregateHash: "17026098453454367869",
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
		testConcept: func() ontology.AggregatedConcept {
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
					AggregateHash: "13590089407881813689",
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
		customAssertion: func(t *testing.T, concept ontology.AggregatedConcept) {
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
					AggregateHash: "2137764349277562661",
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
		customAssertion: func(t *testing.T, concept ontology.AggregatedConcept) {
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

		cmpOpts := cmpopts.IgnoreFields(Event{}, "AggregateHash")
		if !cmp.Equal(scenario.updatedConcepts, actualChanges, cmpOpts) {
			t.Errorf("Scenario %s failed: Updated uuid list differs from expected:\n%s", scenario.testName, cmp.Diff(scenario.updatedConcepts, actualChanges, cmpOpts))
		}

		for _, id := range scenario.uuidsToCheck {
			conceptIf, found, err := conceptsDriver.Read(id, tid)
			concept := cleanHash(conceptIf.(ontology.AggregatedConcept))
			if found {
				assert.NotNil(t, concept, "Scenario "+scenario.testName+" failed; id: "+id+" should return a valid concept")
				assert.True(t, found, "Scenario "+scenario.testName+" failed; id: "+id+" should return a valid concept")
				assert.NoError(t, err, "Scenario "+scenario.testName+" failed; returned unexpected error")
				verifyAggregateHashIsCorrect(t, scenario.testConcept, scenario.testName)
			} else {
				assert.Equal(t, ontology.AggregatedConcept{}, concept, "Scenario "+scenario.testName+" failed; id: "+id+" should return a valid concept")
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
	concept := cleanHash(conceptIf.(ontology.AggregatedConcept))
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
		assert.Equal(t, ontology.AggregatedConcept{}, aggConcept, "Scenario "+scenario.testName+" failed; aggregate concept should be empty")
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
		returnedQueryList, err := conceptsDriver.handleTransferConcordance(scenario.updatedSourceIds, &updatedConcept, "1234", ontology.NewAggregatedConcept{}, "")
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
		targetConcordance ontology.AggregatedConcept
	}
	mergeManagedLocationCanonicalWithTwoSources := testStruct{
		testName: "mergeManagedLocationCanonicalWithTwoSources",
		updatedSourceIds: map[string]string{
			"2": "Brand"},
		returnedError: nil,
		returnResult:  true,
		targetConcordance: ontology.AggregatedConcept{
			PrefUUID: "1",
			SourceRepresentations: []ontology.Concept{
				{UUID: "1", Authority: "Smartlogic"},
				{UUID: "4", Authority: "FACTSET"},
				{UUID: "2", Authority: "ManagedLocation"},
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
		targetConcordance: ontology.AggregatedConcept{
			PrefUUID: "1",
			SourceRepresentations: []ontology.Concept{
				{UUID: "1", Authority: "Smartlogic"},
				{UUID: "4", Authority: "FACTSET"},
				{UUID: "2", Authority: "ManagedLocation"},
				{UUID: "5", Authority: "TME"},
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
		newConcordance := ontology.TransformToNewAggregateConcept(scenario.targetConcordance)
		returnedQueryList, err := conceptsDriver.handleTransferConcordance(scenario.updatedSourceIds, &updatedConcept, "1234", newConcordance, "")
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

func TestValidateObject(t *testing.T) {
	tests := []struct {
		name          string
		aggConcept    ontology.AggregatedConcept
		returnedError string
	}{
		{
			name: "aggregate concept without prefLabel should be invalid",
			aggConcept: ontology.AggregatedConcept{
				PrefUUID: basicConceptUUID,
				Type:     "Brand",
				SourceRepresentations: []ontology.Concept{
					{
						UUID:           basicConceptUUID,
						PrefLabel:      "The Best Label",
						Type:           "Brand",
						AuthorityValue: "123456-UPP",
					},
				},
			},
			returnedError: "invalid request, no prefLabel has been supplied",
		},
		{
			name: "aggregate concept without type should be invalid",
			aggConcept: ontology.AggregatedConcept{
				PrefUUID:  basicConceptUUID,
				PrefLabel: "The Best Label",
				SourceRepresentations: []ontology.Concept{
					{
						UUID:           basicConceptUUID,
						PrefLabel:      "The Best Label",
						Type:           "Brand",
						AuthorityValue: "123456-UPP",
					},
				},
			},
			returnedError: "invalid request, no type has been supplied",
		},
		{
			name: "aggregate concept without source representations should be invalid",
			aggConcept: ontology.AggregatedConcept{
				PrefUUID:  basicConceptUUID,
				PrefLabel: "The Best Label",
				Type:      "Brand",
			},
			returnedError: "invalid request, no sourceRepresentation has been supplied",
		},
		{
			name: "source representation without prefLabel should be valid",
			aggConcept: ontology.AggregatedConcept{
				PrefUUID:  basicConceptUUID,
				PrefLabel: "The Best Label",
				Type:      "Brand",
				SourceRepresentations: []ontology.Concept{
					{
						UUID:           basicConceptUUID,
						Type:           "Brand",
						AuthorityValue: "123456-UPP",
						Authority:      "UPP",
					},
				},
			},
		},
		{
			name: "source representation without type should be invalid",
			aggConcept: ontology.AggregatedConcept{
				PrefUUID:  basicConceptUUID,
				PrefLabel: "The Best Label",
				Type:      "Brand",
				SourceRepresentations: []ontology.Concept{
					{
						UUID:           basicConceptUUID,
						PrefLabel:      "The Best Label",
						Authority:      "UPP",
						AuthorityValue: "123456-UPP",
					},
				},
			},
			returnedError: "invalid request, no sourceRepresentation.type has been supplied",
		},
		{
			name: "source representation without authorityValue should be invalid",
			aggConcept: ontology.AggregatedConcept{
				PrefUUID:  basicConceptUUID,
				PrefLabel: "The Best Label",
				Type:      "Brand",
				SourceRepresentations: []ontology.Concept{
					{
						UUID:      basicConceptUUID,
						PrefLabel: "The Best Label",
						Type:      "Brand",
						Authority: "UPP",
					},
				},
			},
			returnedError: "invalid request, no sourceRepresentation.authorityValue has been supplied",
		},
		{
			name: "source representation without authority should be invalid",
			aggConcept: ontology.AggregatedConcept{
				PrefUUID:  basicConceptUUID,
				PrefLabel: "The Best Label",
				Type:      "Brand",
				SourceRepresentations: []ontology.Concept{
					{
						UUID:           basicConceptUUID,
						PrefLabel:      "The Best Label",
						Type:           "Brand",
						AuthorityValue: "123456-UPP",
					},
				},
			},
			returnedError: "invalid request, no sourceRepresentation.authority has been supplied",
		},
		{
			name: "valid concept",
			aggConcept: ontology.AggregatedConcept{
				PrefUUID:  basicConceptUUID,
				PrefLabel: "The Best Label",
				Type:      "Brand",
				SourceRepresentations: []ontology.Concept{
					{
						UUID:           basicConceptUUID,
						PrefLabel:      "The Best Label",
						Type:           "Brand",
						Authority:      "UPP",
						AuthorityValue: "123456-UPP",
					},
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			newAggConcept := ontology.TransformToNewAggregateConcept(test.aggConcept)
			err := validateObject(newAggConcept, "transaction_id")
			if err != nil {
				assert.NotEmpty(t, test.returnedError, "test.returnedError should not be empty when there is an error")
				assert.Contains(t, err.Error(), test.returnedError, test.name)
			} else {
				assert.Empty(t, test.returnedError, "test.returnedError should be empty when there is no error")
				assert.NoError(t, err, test.name)
			}
		})
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

func TestSetCanonicalProps(t *testing.T) {
	tests := []struct {
		name     string
		concept  ontology.NewAggregatedConcept
		prefUUID string
		expected map[string]interface{}
	}{
		{
			name:    "Concept with default values and no prefUUID should return default props",
			concept: ontology.NewAggregatedConcept{},
			expected: map[string]interface{}{
				"prefUUID":      "",
				"aggregateHash": "",
			},
		},
		{
			name:     "Concept with default values with prefUUID should return props with prefUUID",
			concept:  ontology.NewAggregatedConcept{},
			prefUUID: "6649aeda-0cd0-4a65-a310-77f28e88b620",
			expected: map[string]interface{}{
				"prefUUID":      "6649aeda-0cd0-4a65-a310-77f28e88b620",
				"aggregateHash": "",
			},
		},
		{
			name: "Concept with invalid values should return default props",
			concept: ontology.NewAggregatedConcept{
				Aliases:     []string{},
				FormerNames: []string{},
				TradeNames:  []string{},
			},
			prefUUID: "bbc4f575-edb3-4f51-92f0-5ce6c708d1ea",
			expected: map[string]interface{}{
				"prefUUID":      "bbc4f575-edb3-4f51-92f0-5ce6c708d1ea",
				"aggregateHash": "",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := setCanonicalProps(test.concept, test.prefUUID)

			// ignore "lastModifiedEpoch"
			delete(got, "lastModifiedEpoch")

			if !cmp.Equal(got, test.expected) {
				t.Errorf("Node props differ from expected:\n%s", cmp.Diff(got, test.expected))
			}
		})
	}
}

func TestCreateNodeQueries(t *testing.T) {
	tests := []struct {
		name               string
		concept            ontology.NewConcept
		expectedQueryCount int
	}{
		{
			name:               "Concept with default values and should produce single Cypher query",
			concept:            ontology.NewConcept{},
			expectedQueryCount: 1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := createNodeQueries(test.concept, "")

			if len(got) != test.expectedQueryCount {
				t.Errorf("Number of Cypher queries differs from expected: got %d, want:%d", len(got), test.expectedQueryCount)
			}
		})
	}
}

func TestProcessMembershipRoles(t *testing.T) {
	defer cleanDB(t)
	oldAggregatedConcept := getAggregatedConcept(t, "membership.json")
	aggregateConcept := ontology.TransformToNewAggregateConcept(oldAggregatedConcept)
	processMembershipRoles(&aggregateConcept)

	expected := membWithProcessedMembRoles()
	if !cmp.Equal(expected, aggregateConcept) {
		t.Errorf("Test %s failed: Concepts were not equal:\n%s", "TestProcessMembershipRoles", cmp.Diff(expected, aggregateConcept))
	}
}

func membWithProcessedMembRoles() ontology.NewAggregatedConcept {
	return ontology.NewAggregatedConcept{
		Properties:       map[string]interface{}{},
		PrefUUID:         "cbadd9a7-5da9-407a-a5ec-e379460991f2",
		PrefLabel:        "Membership Pref Label",
		Type:             "Membership",
		OrganisationUUID: "7f40d291-b3cb-47c4-9bce-18413e9350cf",
		PersonUUID:       "35946807-0205-4fc1-8516-bb1ae141659b",
		InceptionDate:    "2016-01-01",
		TerminationDate:  "2017-02-02",
		Salutation:       "Mr",
		BirthYear:        2018,
		SourceRepresentations: []ontology.NewConcept{
			{
				Relationships:    []ontology.Relationship{},
				UUID:             "cbadd9a7-5da9-407a-a5ec-e379460991f2",
				PrefLabel:        "Membership Pref Label",
				Type:             "Membership",
				Authority:        "Smartlogic",
				AuthorityValue:   "746464",
				OrganisationUUID: "7f40d291-b3cb-47c4-9bce-18413e9350cf",
				PersonUUID:       "35946807-0205-4fc1-8516-bb1ae141659b",
				InceptionDate:    "2016-01-01",
				TerminationDate:  "2017-02-02",
				Salutation:       "Mr",
				BirthYear:        2018,
				MembershipRoles: []ontology.MembershipRole{
					{
						RoleUUID:             "f807193d-337b-412f-b32c-afa14b385819",
						InceptionDate:        "2016-01-01",
						TerminationDate:      "2017-02-02",
						InceptionDateEpoch:   1451606400,
						TerminationDateEpoch: 1485993600,
					},
					{
						RoleUUID:           "fe94adc6-ca44-438f-ad8f-0188d4a74987",
						InceptionDate:      "2011-06-27",
						InceptionDateEpoch: 1309132800,
					},
				},
			},
		},
	}
}

func readConceptAndCompare(t *testing.T, payload ontology.AggregatedConcept, testName string, ignoredFields ...string) {
	actualIf, found, err := conceptsDriver.Read(payload.PrefUUID, "")
	actual := actualIf.(ontology.AggregatedConcept)

	newPayload := ontology.TransformToNewAggregateConcept(payload)
	clean := cleanSourceProperties(newPayload)

	newClean := ontology.TransformToOldAggregateConcept(clean)
	expected := cleanHash(cleanConcept(newClean))

	actual = cleanHash(cleanConcept(actual))

	cmpOptions := cmpopts.IgnoreFields(ontology.Concept{}, ignoredFields...)
	if !cmp.Equal(expected, actual, cmpOptions) {
		t.Errorf("Test %s failed: Concepts were not equal:\n%s", testName, cmp.Diff(expected, actual, cmpOptions))
	}

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
		naicsIndustryClassificationAnotherUUID,
		organisationWithNAICSUUID,
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
		naicsIndustryClassificationAnotherUUID,
		organisationWithNAICSUUID,
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
		naicsIndustryClassificationAnotherUUID,
		organisationWithNAICSUUID,
	)
}

func deleteSourceNodes(t *testing.T, uuids ...string) {
	qs := make([]*neoism.CypherQuery, len(uuids))
	for i, uuid := range uuids {
		qs[i] = &neoism.CypherQuery{
			Statement: fmt.Sprintf(`
			MATCH (a:Thing {uuid: "%s"})
			DETACH DELETE a`, uuid)}
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
			OPTIONAL MATCH (a)-[hp:HAS_PARENT]-(p)
			DELETE hp`, uuid)}
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

func verifyAggregateHashIsCorrect(t *testing.T, concept ontology.AggregatedConcept, testName string) {
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

	newConcept := ontology.TransformToNewAggregateConcept(concept)
	conceptHash, _ := hashstructure.Hash(cleanSourceProperties(newConcept), nil)
	hashAsString := strconv.FormatUint(conceptHash, 10)
	assert.Equal(t, hashAsString, results[0].Hash, fmt.Sprintf("Test %s failed: Concept hash %s and stored record %s are not equal!", testName, hashAsString, results[0].Hash))
}

func cleanConcept(c ontology.AggregatedConcept) ontology.AggregatedConcept {
	for j := range c.SourceRepresentations {
		c.SourceRepresentations[j].LastModifiedEpoch = 0
		for i := range c.SourceRepresentations[j].MembershipRoles {
			c.SourceRepresentations[j].MembershipRoles[i].InceptionDateEpoch = 0
			c.SourceRepresentations[j].MembershipRoles[i].TerminationDateEpoch = 0
		}
		sort.SliceStable(c.SourceRepresentations[j].MembershipRoles, func(k, l int) bool {
			return c.SourceRepresentations[j].MembershipRoles[k].RoleUUID < c.SourceRepresentations[j].MembershipRoles[l].RoleUUID
		})
		sort.SliceStable(c.SourceRepresentations[j].BroaderUUIDs, func(k, l int) bool {
			return c.SourceRepresentations[j].BroaderUUIDs[k] < c.SourceRepresentations[j].BroaderUUIDs[l]
		})
		sort.SliceStable(c.SourceRepresentations[j].RelatedUUIDs, func(k, l int) bool {
			return c.SourceRepresentations[j].RelatedUUIDs[k] < c.SourceRepresentations[j].RelatedUUIDs[l]
		})
		sort.SliceStable(c.SourceRepresentations[j].SupersededByUUIDs, func(k, l int) bool {
			return c.SourceRepresentations[j].SupersededByUUIDs[k] < c.SourceRepresentations[j].SupersededByUUIDs[l]
		})
		sort.SliceStable(c.SourceRepresentations[j].ImpliedByUUIDs, func(k, l int) bool {
			return c.SourceRepresentations[j].ImpliedByUUIDs[k] < c.SourceRepresentations[j].ImpliedByUUIDs[l]
		})
		sort.SliceStable(c.SourceRepresentations[j].HasFocusUUIDs, func(k, l int) bool {
			return c.SourceRepresentations[j].HasFocusUUIDs[k] < c.SourceRepresentations[j].HasFocusUUIDs[l]
		})
		sort.SliceStable(c.SourceRepresentations[j].NAICSIndustryClassifications, func(k, l int) bool {
			return c.SourceRepresentations[j].NAICSIndustryClassifications[k].Rank < c.SourceRepresentations[j].NAICSIndustryClassifications[l].Rank
		})
	}
	for i := range c.MembershipRoles {
		c.MembershipRoles[i].InceptionDateEpoch = 0
		c.MembershipRoles[i].TerminationDateEpoch = 0
	}
	sort.SliceStable(c.SourceRepresentations, func(k, l int) bool {
		return c.SourceRepresentations[k].UUID < c.SourceRepresentations[l].UUID
	})
	return c
}

func cleanHash(c ontology.AggregatedConcept) ontology.AggregatedConcept {
	c.AggregatedHash = ""
	return c
}
