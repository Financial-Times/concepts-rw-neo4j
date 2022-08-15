//go:build integration
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
	"github.com/mitchellh/hashstructure"
	"github.com/sirupsen/logrus"
	logTest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"

	cmneo4j "github.com/Financial-Times/cm-neo4j-driver"
	"github.com/Financial-Times/go-logger/v2"

	ontology "github.com/Financial-Times/cm-graph-ontology"
	"github.com/Financial-Times/cm-graph-ontology/neo4j"
	"github.com/Financial-Times/cm-graph-ontology/transform"
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
	membershipRole = ontology.Relationship{
		UUID:  "f807193d-337b-412f-b32c-afa14b385819",
		Label: "HAS_ROLE",
		Properties: ontology.Properties{
			"inceptionDate":   "2016-01-01",
			"terminationDate": "2017-02-02",
		},
	}
	anotherMembershipRole = ontology.Relationship{
		UUID:  "fe94adc6-ca44-438f-ad8f-0188d4a74987",
		Label: "HAS_ROLE",
		Properties: ontology.Properties{
			"inceptionDate": "2011-06-27",
		},
	}
)

//Reusable Neo4J driver
var driver *cmneo4j.Driver

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
func getAggregatedConcept(t *testing.T, name string) ontology.NewAggregatedConcept {
	ac := ontology.NewAggregatedConcept{}
	err := json.Unmarshal(helperLoadBytes(t, name), &ac)
	if err != nil {
		t.Fatal(err)
	}
	return ac
}

func getOrganisationWithAllCountries() ontology.NewAggregatedConcept {
	return ontology.NewAggregatedConcept{
		AggregateConceptFields: ontology.AggregateConceptFields{
			PrefUUID:  testOrgUUID,
			Type:      "PublicCompany",
			PrefLabel: "Strix Group Plc",
			SourceRepresentations: []ontology.NewConcept{
				{
					SourceConceptFields: ontology.SourceConceptFields{
						UUID:           testOrgUUID,
						Type:           "PublicCompany",
						Authority:      "FACTSET",
						AuthorityValue: "B000BB-S",
					}, DynamicFields: ontology.DynamicFields{
						Properties: ontology.Properties{
							"properName": "Strix Group Plc",
							"prefLabel":  "Strix Group Plc",
							"shortName":  "Strix Group",
							"tradeNames": []string{
								"STRIX GROUP PLC",
							},
							"formerNames": []string{
								"Castletown Thermostats",
								"Steam Plc",
							},
							"aliases": []string{
								"Strix Group Plc",
								"STRIX GROUP PLC",
								"Strix Group",
								"Castletown Thermostats",
								"Steam Plc",
							},
							"countryCode":                "BG",
							"countryOfIncorporation":     "GB",
							"countryOfOperations":        "FR",
							"countryOfRisk":              "BG",
							"countryOfIncorporationUUID": locationUUID,
							"countryOfOperationsUUID":    locationUUID,
							"countryOfRiskUUID":          anotherLocationUUID,
							"postalCode":                 "IM9 2RG",
							"yearFounded":                1951,
							"emailAddress":               "info@strix.com",
							"leiCode":                    "213800KZEW5W6BZMNT62",
							"parentOrganisation":         parentOrgUUID,
						}, Relationships: nil,
					},
				},
			},
		},
		DynamicFields: ontology.DynamicFields{
			Properties: ontology.Properties{
				"shortName":  "Strix Group",
				"properName": "Strix Group Plc",
				"tradeNames": []string{
					"STRIX GROUP PLC",
				},
				"formerNames": []string{
					"Castletown Thermostats",
					"Steam Plc",
				},
				"aliases": []string{
					"Strix Group Plc",
					"STRIX GROUP PLC",
					"Strix Group",
					"Castletown Thermostats",
					"Steam Plc",
				},
				"countryCode":            "BG",
				"countryOfIncorporation": "GB",
				"countryOfOperations":    "FR",
				"countryOfRisk":          "BG",
				"postalCode":             "IM9 2RG",
				"yearFounded":            1951,
				"emailAddress":           "info@strix.com",
				"leiCode":                "213800KZEW5W6BZMNT62",
			},
			Relationships: nil,
		},
	}
}

func getConcept(t *testing.T, name string) transform.OldConcept {
	c := transform.OldConcept{}
	err := json.Unmarshal(helperLoadBytes(t, name), &c)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func getLocation() ontology.NewAggregatedConcept {
	return ontology.NewAggregatedConcept{
		AggregateConceptFields: ontology.AggregateConceptFields{
			PrefUUID:  locationUUID,
			PrefLabel: "Location Pref Label",
			Type:      "Location",
			SourceRepresentations: []ontology.NewConcept{{
				SourceConceptFields: ontology.SourceConceptFields{
					UUID:           locationUUID,
					PrefLabel:      "Location Pref Label",
					Type:           "Location",
					Authority:      "ManagedLocation",
					AuthorityValue: locationUUID,
				}, DynamicFields: ontology.DynamicFields{},
			}},
		}, DynamicFields: ontology.DynamicFields{
			Properties:    ontology.Properties{},
			Relationships: nil,
		},
	}
}

func getLocationWithISO31661() ontology.NewAggregatedConcept {
	return ontology.NewAggregatedConcept{
		AggregateConceptFields: ontology.AggregateConceptFields{
			PrefUUID:  locationUUID,
			PrefLabel: "Location Pref Label 2",
			Type:      "Location",
			SourceRepresentations: []ontology.NewConcept{{
				SourceConceptFields: ontology.SourceConceptFields{
					UUID:           locationUUID,
					PrefLabel:      "Location Pref Label 2",
					Type:           "Location",
					Authority:      "ManagedLocation",
					AuthorityValue: locationUUID,
				}, DynamicFields: ontology.DynamicFields{
					Properties: ontology.Properties{
						"aliases": []string{
							"Bulgaria",
							"Bulgarie",
							"Bulgarien",
						},
						"iso31661": "BG"},
					Relationships: nil,
				},
			}},
		}, DynamicFields: ontology.DynamicFields{
			Properties: ontology.Properties{
				"aliases": []string{
					"Bulgaria",
					"Bulgarie",
					"Bulgarien",
				},
				"iso31661": "BG",
			}, Relationships: nil,
		},
	}
}

func getLocationWithISO31661AndConcordance() ontology.NewAggregatedConcept {
	return ontology.NewAggregatedConcept{
		AggregateConceptFields: ontology.AggregateConceptFields{
			PrefUUID:  anotherLocationUUID,
			PrefLabel: "Location Pref Label 2",
			Type:      "Location",
			SourceRepresentations: []ontology.NewConcept{
				{
					SourceConceptFields: ontology.SourceConceptFields{
						UUID:           locationUUID,
						PrefLabel:      "Location Pref Label 2",
						Type:           "Location",
						Authority:      "ManagedLocation",
						AuthorityValue: locationUUID,
					}, DynamicFields: ontology.DynamicFields{
						Properties: ontology.Properties{
							"aliases": []string{
								"Bulgaria",
								"Bulgarie",
								"Bulgarien",
							},
							"iso31661": "BG",
						}, Relationships: nil,
					},
				},
				{
					SourceConceptFields: ontology.SourceConceptFields{
						UUID:           anotherLocationUUID,
						PrefLabel:      "Location Pref Label 2",
						Type:           "Location",
						Authority:      "Smartlogic",
						AuthorityValue: anotherLocationUUID,
					}, DynamicFields: ontology.DynamicFields{
						Properties: ontology.Properties{
							"aliases": []string{
								"Bulgaria",
								"Bulgarie",
								"Bulgarien",
							},
						}, Relationships: nil,
					},
				},
			},
		}, DynamicFields: ontology.DynamicFields{
			Properties: ontology.Properties{
				"aliases": []string{
					"Bulgaria",
					"Bulgarie",
					"Bulgarien",
				},
				"iso31661": "BG",
			}, Relationships: nil,
		},
	}
}

func init() {
	// We are initialising a lot of constraints on an empty database therefore we need the database to be fit before
	// we run tests so initialising the service will create the constraints first

	url := os.Getenv("NEO4J_TEST_URL")
	if url == "" {
		url = "bolt://localhost:7687"
	}
	log := logger.NewUPPLogger("test-concepts-rw-neo4j", "panic")
	d, err := cmneo4j.NewDefaultDriver(url, log)
	if err != nil {
		log.WithError(err).Fatal("could not create a new cmneo4j driver")
	}
	conceptsDriver = NewConceptService(d, log)
	err = conceptsDriver.Initialise()
	if err != nil {
		log.WithError(err).Fatal("failed to initialise ConceptSerivce")
	}

	driver = d
	duration := 5 * time.Second
	time.Sleep(duration)
}

func TestWriteService(t *testing.T) {
	tests := []struct {
		testName             string
		aggregatedConcept    ontology.NewAggregatedConcept
		otherRelatedConcepts []ontology.NewAggregatedConcept
		writtenNotReadFields []string
		errStr               string
		updatedConcepts      ConceptChanges
	}{
		{
			testName:          "Throws validation error for invalid concept",
			aggregatedConcept: ontology.NewAggregatedConcept{AggregateConceptFields: ontology.AggregateConceptFields{PrefUUID: basicConceptUUID}, DynamicFields: ontology.DynamicFields{}},
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
			otherRelatedConcepts: []ontology.NewAggregatedConcept{
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
			otherRelatedConcepts: []ontology.NewAggregatedConcept{
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
			otherRelatedConcepts: []ontology.NewAggregatedConcept{
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
			otherRelatedConcepts: []ontology.NewAggregatedConcept{
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
			otherRelatedConcepts: []ontology.NewAggregatedConcept{
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
			otherRelatedConcepts: []ontology.NewAggregatedConcept{
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
			otherRelatedConcepts: []ontology.NewAggregatedConcept{
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
			otherRelatedConcepts: []ontology.NewAggregatedConcept{
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
			otherRelatedConcepts: []ontology.NewAggregatedConcept{
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
			otherRelatedConcepts: []ontology.NewAggregatedConcept{
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
			otherRelatedConcepts: []ontology.NewAggregatedConcept{
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
			otherRelatedConcepts: []ontology.NewAggregatedConcept{
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
			otherRelatedConcepts: []ontology.NewAggregatedConcept{
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
			otherRelatedConcepts: []ontology.NewAggregatedConcept{
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
				if !assert.NoError(t, err, "Failed to write related/broader/impliedBy concept") {
					return
				}
			}
			updatedConcepts, err := conceptsDriver.Write(test.aggregatedConcept, "")
			if test.errStr == "" {
				if !assert.NoError(t, err, "Failed to write concept") {
					return
				}
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
	originalMembership := result.(ontology.NewAggregatedConcept)
	originalMembership = cleanHash(originalMembership)
	originalMembership = cleanNewAggregatedConcept(originalMembership)
	memRoles := 0
	var relationships ontology.Relationships
	for i := range originalMembership.Relationships {
		if originalMembership.Relationships[i].Label == "HAS_ROLE" {
			memRoles++
			relationships = append(relationships, originalMembership.Relationships[i])
		}
	}
	assert.Equal(t, memRoles, 2)
	assert.True(t, reflect.DeepEqual(ontology.Relationships{membershipRole, anotherMembershipRole}, relationships))
	assert.Equal(t, organisationUUID, extractFieldFromRelationship(originalMembership.Relationships, "HAS_ORGANISATION"))
	assert.Equal(t, personUUID, extractFieldFromRelationship(originalMembership.Relationships, "HAS_MEMBER"))
	assert.Equal(t, "Mr", originalMembership.Properties["salutation"])
	assert.Equal(t, 2018, originalMembership.Properties["birthYear"])

	_, err = conceptsDriver.Write(getAggregatedConcept(t, "updated-membership.json"), "test_tid")
	assert.NoError(t, err, "Failed to write membership")

	updatedResult, _, err := conceptsDriver.Read(membershipUUID, "test_tid")
	assert.NoError(t, err, "Failed to read membership")
	updatedMemebership := updatedResult.(ontology.NewAggregatedConcept)
	updatedMemebership = cleanHash(updatedMemebership)
	updatedMemRoles := 0
	var updatedRelationships ontology.Relationships
	for i := range updatedMemebership.Relationships {
		if updatedMemebership.Relationships[i].Label == "HAS_ROLE" {
			updatedMemRoles++
			updatedRelationships = append(updatedRelationships, updatedMemebership.Relationships[i])
		}
	}
	for i := range updatedRelationships {
		m := make(map[string]interface{}, len(updatedRelationships[i].Properties)-1)
		for p := range updatedRelationships[i].Properties {
			if p != "inceptionDateEpoch" {
				m[p] = updatedRelationships[i].Properties[p]
			}
		}
		updatedRelationships[i].Properties = m
	}

	assert.Equal(t, updatedMemRoles, 1)
	assert.Equal(t, ontology.Relationships{anotherMembershipRole}, updatedRelationships)
	assert.Contains(t, exctractAllUUIDsForSameRelationship(updatedMemebership.Relationships, "HAS_ORGANISATION"), anotherOrganisationUUID)
	assert.Equal(t, anotherPersonUUID, extractFieldFromRelationship(updatedMemebership.Relationships, "HAS_MEMBER"))
}

func exctractAllUUIDsForSameRelationship(r ontology.Relationships, relationshipName string) []string {
	var rels []string
	for i := range r {
		if r[i].Label == relationshipName {
			rels = append(rels, r[i].UUID)
		}
	}
	return rels
}
func extractFieldFromRelationship(r ontology.Relationships, relationshipName string) string {
	for i := range r {
		if r[i].Label == relationshipName {
			return r[i].UUID
		}
	}
	return ""
}

func TestWriteMemberships_FixOldData(t *testing.T) {
	defer cleanDB(t)

	oldConcept := getConcept(t, "old-membership.json")
	newConcept, err := transform.ToNewSourceConcept(oldConcept)
	newConcept.UUID = membershipUUID
	assert.NoError(t, err)
	queries := neo4j.WriteSourceQueries(newConcept)
	err = driver.Write(queries...)
	assert.NoError(t, err, "Failed to write source")

	_, err = conceptsDriver.Write(getAggregatedConcept(t, "membership.json"), "test_tid")
	assert.NoError(t, err, "Failed to write membership")

	result, _, err := conceptsDriver.Read(membershipUUID, "test_tid")
	assert.NoError(t, err, "Failed to read membership")
	originalMembership := result.(ontology.NewAggregatedConcept)
	originalMembership = cleanHash(originalMembership)
	originalMembership = cleanNewAggregatedConcept(originalMembership)

	memRoles := 0
	var updatedRelationships ontology.Relationships
	for i := range originalMembership.Relationships {
		if originalMembership.Relationships[i].Label == "HAS_ROLE" {
			memRoles++
			updatedRelationships = append(updatedRelationships, originalMembership.Relationships[i])
		}
	}

	assert.Equal(t, memRoles, 2)
	assert.True(t, reflect.DeepEqual(ontology.Relationships{membershipRole, anotherMembershipRole}, updatedRelationships))
	assert.Equal(t, organisationUUID, extractFieldFromRelationship(originalMembership.Relationships, "HAS_ORGANISATION"))
	assert.Equal(t, personUUID, extractFieldFromRelationship(originalMembership.Relationships, "HAS_MEMBER"))
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
		setUpConcept    ontology.NewAggregatedConcept
		testConcept     ontology.NewAggregatedConcept
		uuidsToCheck    []string
		returnedError   string
		updatedConcepts ConceptChanges
		customAssertion func(t *testing.T, concept ontology.NewAggregatedConcept)
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
		testConcept: func() ontology.NewAggregatedConcept {
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
		testConcept: func() ontology.NewAggregatedConcept {
			concept := getAggregatedConcept(t, "single-concordance.json")
			concept.SourceRepresentations[0].Relationships = append(concept.SourceRepresentations[0].Relationships, ontology.Relationship{UUID: supersededByUUID, Label: "SUPERSEDED_BY"})
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
		customAssertion: func(t *testing.T, concept ontology.NewAggregatedConcept) {
			assert.Lenf(t, concept.SourceRepresentations, 1, "Test %s failed. Different number of sourceRepresentation items than expected", "singleConcordanceSupersededByAddRelationship")
			assert.Lenf(t, concept.SourceRepresentations[0].Relationships, 1, "Test %s failed. Different number of supersededByUUIDs items than expected", "singleConcordanceSupersededByAddRelationship")
			assert.Equalf(t, supersededByUUID, concept.SourceRepresentations[0].Relationships[0].UUID, "Test %s failed. Different supersededByUUID than expected", "singleConcordanceSupersededByAddRelationship")
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
		customAssertion: func(t *testing.T, concept ontology.NewAggregatedConcept) {
			assert.Lenf(t, concept.SourceRepresentations, 1, "Test %s failed. Different number of sourceRepresentation items than expected", "singleConcordanceSupersededByRemoveRelationship")
			assert.Emptyf(t, concept.SourceRepresentations[0].Relationships, "Test %s failed. No supersededByUUIDs content expected", "singleConcordanceSupersededByRemoveRelationship")
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
			concept := cleanHash(conceptIf.(ontology.NewAggregatedConcept))
			if found {
				assert.NotNil(t, concept, "Scenario "+scenario.testName+" failed; id: "+id+" should return a valid concept")
				assert.True(t, found, "Scenario "+scenario.testName+" failed; id: "+id+" should return a valid concept")
				assert.NoError(t, err, "Scenario "+scenario.testName+" failed; returned unexpected error")
				verifyAggregateHashIsCorrect(t, scenario.testConcept, scenario.testName)
			} else {
				assert.Equal(t, ontology.NewAggregatedConcept{}, concept, "Scenario "+scenario.testName+" failed; id: "+id+" should return a valid concept")
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
	concept := cleanHash(conceptIf.(ontology.NewAggregatedConcept))
	assert.NoError(t, err, "Should be able to read concept with no problems")
	assert.True(t, found, "Concept should exist")
	assert.NotNil(t, concept, "Concept should be populated")
	readConceptAndCompare(t, getAggregatedConcept(t, "transfer-multiple-source-concordance.json"), "TestMultipleConcordancesAreHandled")
}

// Test case is a concept with multiple sources, one of which has multiple Industry classifications.
// From bug, https://financialtimes.atlassian.net/browse/UPPSF-2773 on Write (property update)
// the concept in question was returning unexpected CONCORDANCE_ADDED/CONCORDANCE_REMOVED where only CONCEPT_UPDATED was expected.
func TestWriteShouldReturnCorrectConceptChanges(t *testing.T) {
	const mainConceptUUID = "13465cc7-204f-48b9-a8d6-b901d5d86c48"
	var aggregate ontology.NewAggregatedConcept
	concepts, canonicalUUIDs, sourceUUIDs := readTestSetup(t, "testdata/bug/13465cc7-204f-48b9-a8d6-b901d5d86c48.json")
	for _, concept := range concepts {
		_, err := conceptsDriver.Write(concept, "tid_init")
		if err != nil {
			t.Fatal(err)
		}
		if concept.PrefUUID == mainConceptUUID {
			aggregate = concept
		}
	}
	defer func() {
		deleteSourceNodes(t, sourceUUIDs...)
		deleteConcordedNodes(t, canonicalUUIDs...)
	}()

	expectedEvents := ConceptChanges{
		ChangedRecords: []Event{
			{
				ConceptType:   "Organisation",
				ConceptUUID:   "13465cc7-204f-48b9-a8d6-b901d5d86c48",
				TransactionID: "tid_second",
				EventDetails:  ConceptEvent{Type: UpdatedEvent},
			},
		},
		UpdatedIds: []string{
			"0eb54dff-fbe3-330e-b755-7435c4aad411",
			"374fdcea-062f-3281-81ca-7851323bcf98",
			"6259ebad-ed4c-3b13-ae66-9117fa591328",
			"13465cc7-204f-48b9-a8d6-b901d5d86c48",
		},
	}

	// force concept update
	p := make(map[string]interface{}, len(aggregate.DynamicFields.Properties)+1)
	for i := range aggregate.DynamicFields.Properties {
		p[i] = aggregate.DynamicFields.Properties[i]
	}
	p["descriptionXML"] = "testing"
	aggregate.Properties = p
	data, err := conceptsDriver.Write(aggregate, "tid_second")
	if err != nil {
		t.Fatal(err)
	}
	events, ok := data.(ConceptChanges)
	if !ok {
		t.Fatal("concept write did not return 'ConceptChanges'")
	}
	if !cmp.Equal(expectedEvents, events, cmpopts.IgnoreFields(Event{}, "AggregateHash")) {
		t.Error(cmp.Diff(expectedEvents, events, cmpopts.IgnoreFields(Event{}, "AggregateHash")))
	}
}

func TestReadReturnsErrorOnMultipleResults(t *testing.T) {
	// note the test data that this is explicitly broken setup, where multiple source concepts have HAS_ORGANISATION relationship
	// this is unsupported behaviour and will produce multiple results when reading from neo4j
	const mainConceptUUID = "13465cc7-204f-48b9-a8d6-b901d5d86c48"
	concepts, canonicalUUIDs, sourceUUIDs := readTestSetup(t, "testdata/bug/concorded-multiple-has-organisation.json")
	for _, concept := range concepts {
		_, err := conceptsDriver.Write(concept, "tid_init")
		if err != nil {
			t.Fatal(err)
		}
	}
	defer func() {
		deleteSourceNodes(t, sourceUUIDs...)
		deleteConcordedNodes(t, canonicalUUIDs...)
	}()

	_, _, err := conceptsDriver.Read(mainConceptUUID, "tid_test")
	if !errors.Is(err, ErrUnexpectedReadResult) {
		t.Fatalf("expected read result error, but got '%v'", err)
	}
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
		err := driver.Write(&cmneo4j.Query{Cypher: scenario.statementToWrite})
		assert.NoError(t, err, "Unexpected error on Write to the db")
		aggConcept, found, err := conceptsDriver.Read(scenario.prefUUID, "")
		assert.Equal(t, ontology.NewAggregatedConcept{}, aggConcept, "Scenario "+scenario.testName+" failed; aggregate concept should be empty")
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
	err := driver.Write(&cmneo4j.Query{Cypher: statement})
	assert.NoError(t, err, "Unexpected error on Write to the db")
	var updatedConcept ConceptChanges

	type testStruct struct {
		testName         string
		updatedSourceIds map[string]string
		expectedResult   []string
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
		expectedResult: []string{"6"},
		returnedError:  nil,
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
		if scenario.expectedResult != nil {
			assert.Equal(t, scenario.expectedResult, returnedQueryList, "Scenario "+scenario.testName+" results do not match")
			break
		}
		assert.Empty(t, returnedQueryList, "Scenario "+scenario.testName+" results do not match")
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
	err := driver.Write(&cmneo4j.Query{Cypher: statement})
	assert.NoError(t, err, "Unexpected error on Write to the db")

	var updatedConcept ConceptChanges

	type testStruct struct {
		testName          string
		updatedSourceIds  map[string]string
		expectedResult    []string
		returnedError     error
		targetConcordance ontology.NewAggregatedConcept
	}
	mergeManagedLocationCanonicalWithTwoSources := testStruct{
		testName: "mergeManagedLocationCanonicalWithTwoSources",
		updatedSourceIds: map[string]string{
			"2": "Brand"},
		returnedError:  nil,
		expectedResult: []string{"2"},
		targetConcordance: ontology.NewAggregatedConcept{
			AggregateConceptFields: ontology.AggregateConceptFields{
				PrefUUID: "1",
				SourceRepresentations: []ontology.NewConcept{
					{
						SourceConceptFields: ontology.SourceConceptFields{
							UUID:      "1",
							Authority: "Smartlogic",
						},
					}, {
						SourceConceptFields: ontology.SourceConceptFields{
							UUID:      "4",
							Authority: "FACTSET",
						},
					},
					{
						SourceConceptFields: ontology.SourceConceptFields{
							UUID:      "2",
							Authority: "ManagedLocation",
						},
					},
				},
			},
		},
	}
	mergeManagedLocationCanonicalWithTwoSourcesAndGeonames := testStruct{
		testName: "mergeManagedLocationCanonicalWithTwoSourcesAndGeonames",
		updatedSourceIds: map[string]string{
			"3": "Brand",
			"2": "Brand"},
		returnedError:  nil,
		expectedResult: []string{"2"},
		targetConcordance: ontology.NewAggregatedConcept{
			AggregateConceptFields: ontology.AggregateConceptFields{
				PrefUUID: "1",
				SourceRepresentations: []ontology.NewConcept{
					{
						SourceConceptFields: ontology.SourceConceptFields{
							UUID:      "1",
							Authority: "Smartlogic",
						},
					},
					{
						SourceConceptFields: ontology.SourceConceptFields{
							UUID:      "4",
							Authority: "FACTSET",
						},
					},
					{
						SourceConceptFields: ontology.SourceConceptFields{
							UUID:      "2",
							Authority: "ManagedLocation",
						},
					},
					{
						SourceConceptFields: ontology.SourceConceptFields{
							UUID:      "5",
							Authority: "TME",
						},
					},
				},
			},
		},
	}
	mergeJustASourceConcordance := testStruct{
		testName: "mergeJustASourceConcordance",
		updatedSourceIds: map[string]string{
			"4": "Brand"},
		returnedError:  nil,
		expectedResult: nil,
	}

	scenarios := []testStruct{
		mergeManagedLocationCanonicalWithTwoSources,
		mergeManagedLocationCanonicalWithTwoSourcesAndGeonames,
		mergeJustASourceConcordance,
	}

	for _, scenario := range scenarios {
		returnedQueryList, err := conceptsDriver.handleTransferConcordance(scenario.updatedSourceIds, &updatedConcept, "1234", scenario.targetConcordance, "")
		assert.Equal(t, scenario.returnedError, err, "Scenario "+scenario.testName+" returned unexpected error")
		if scenario.expectedResult != nil {
			assert.Equal(t, scenario.expectedResult, returnedQueryList, "Scenario "+scenario.testName+" results do not match")
			continue
		}
		assert.Empty(t, returnedQueryList, "Scenario "+scenario.testName+" results do not match")
	}

	defer deleteSourceNodes(t, "1", "2", "3", "5")
	defer deleteConcordedNodes(t, "1", "2")
}

func TestValidateObject(t *testing.T) {
	cacheOut := conceptsDriver.log.Logger.Out
	cacheLevel := conceptsDriver.log.Logger.Level

	conceptsDriver.log.SetLevel(logrus.DebugLevel)
	conceptsDriver.log.Logger.Out = ioutil.Discard
	defer func() {
		conceptsDriver.log.Logger.Out = cacheOut
		conceptsDriver.log.SetLevel(cacheLevel)
	}()
	tests := []struct {
		name          string
		aggConcept    ontology.NewAggregatedConcept
		returnedError string
		expectedLogs  []map[string]interface{}
	}{
		{
			name: "aggregate concept without prefLabel should be invalid",
			aggConcept: ontology.NewAggregatedConcept{
				AggregateConceptFields: ontology.AggregateConceptFields{
					PrefUUID: basicConceptUUID,
					Type:     "Brand",
					SourceRepresentations: []ontology.NewConcept{
						{
							SourceConceptFields: ontology.SourceConceptFields{
								UUID:           anotherBasicConceptUUID,
								PrefLabel:      "The Best Label",
								Type:           "Brand",
								AuthorityValue: "123456-UPP",
							},
						},
					},
				},
			},
			returnedError: "invalid request, no prefLabel has been supplied",
			expectedLogs: []map[string]interface{}{
				{
					"level":          logrus.ErrorLevel,
					"msg":            "Validation of payload failed",
					"error":          errors.New("invalid request, no prefLabel has been supplied"),
					"transaction_id": "transaction_id",
					"uuid":           basicConceptUUID,
				},
			},
		},
		{
			name: "aggregate concept without type should be invalid",
			aggConcept: ontology.NewAggregatedConcept{
				AggregateConceptFields: ontology.AggregateConceptFields{
					PrefUUID:  basicConceptUUID,
					PrefLabel: "The Best Label",
					SourceRepresentations: []ontology.NewConcept{
						{
							SourceConceptFields: ontology.SourceConceptFields{
								UUID:           anotherBasicConceptUUID,
								PrefLabel:      "The Best Label",
								Type:           "Brand",
								AuthorityValue: "123456-UPP",
							},
						},
					},
				},
			},
			returnedError: "invalid request, no type has been supplied",
			expectedLogs: []map[string]interface{}{
				{
					"level":          logrus.ErrorLevel,
					"msg":            "Validation of payload failed",
					"error":          errors.New("invalid request, no type has been supplied"),
					"transaction_id": "transaction_id",
					"uuid":           basicConceptUUID,
				},
			},
		},
		{
			name: "aggregate concept without source representations should be invalid",
			aggConcept: ontology.NewAggregatedConcept{
				AggregateConceptFields: ontology.AggregateConceptFields{
					PrefUUID:  basicConceptUUID,
					PrefLabel: "The Best Label",
					Type:      "Brand",
				},
			},
			returnedError: "invalid request, no sourceRepresentation has been supplied",
			expectedLogs: []map[string]interface{}{
				{
					"level":          logrus.ErrorLevel,
					"msg":            "Validation of payload failed",
					"error":          errors.New("invalid request, no sourceRepresentation has been supplied"),
					"transaction_id": "transaction_id",
					"uuid":           basicConceptUUID,
				},
			},
		},
		{
			name: "source representation without prefLabel should be valid",
			aggConcept: ontology.NewAggregatedConcept{
				AggregateConceptFields: ontology.AggregateConceptFields{
					PrefUUID:  basicConceptUUID,
					PrefLabel: "The Best Label",
					Type:      "Brand",
					SourceRepresentations: []ontology.NewConcept{
						{
							SourceConceptFields: ontology.SourceConceptFields{
								UUID:           anotherBasicConceptUUID,
								Type:           "Brand",
								AuthorityValue: "123456-UPP",
								Authority:      "UPP",
							},
						},
					},
				},
			},
		},
		{
			name: "source representation without type should be invalid",
			aggConcept: ontology.NewAggregatedConcept{
				AggregateConceptFields: ontology.AggregateConceptFields{
					PrefUUID:  basicConceptUUID,
					PrefLabel: "The Best Label",
					Type:      "Brand",
					SourceRepresentations: []ontology.NewConcept{
						{
							SourceConceptFields: ontology.SourceConceptFields{
								UUID:           anotherBasicConceptUUID,
								PrefLabel:      "The Best Label",
								AuthorityValue: "123456-UPP",
								Authority:      "UPP",
							},
						},
					},
				},
			},
			returnedError: "invalid request, no sourceRepresentation.type has been supplied",
			expectedLogs: []map[string]interface{}{
				{
					"level":          logrus.ErrorLevel,
					"msg":            "Validation of payload failed",
					"error":          errors.New("invalid request, no sourceRepresentation.type has been supplied"),
					"transaction_id": "transaction_id",
					"uuid":           anotherBasicConceptUUID,
				},
			},
		},
		{
			name: "source representation without authorityValue should be invalid",
			aggConcept: ontology.NewAggregatedConcept{
				AggregateConceptFields: ontology.AggregateConceptFields{
					PrefUUID:  basicConceptUUID,
					PrefLabel: "The Best Label",
					Type:      "Brand",
					SourceRepresentations: []ontology.NewConcept{
						{
							SourceConceptFields: ontology.SourceConceptFields{
								UUID:      anotherBasicConceptUUID,
								PrefLabel: "The Best Label",
								Type:      "Brand",
								Authority: "UPP",
							},
						},
					},
				},
			},
			returnedError: "invalid request, no sourceRepresentation.authorityValue has been supplied",
			expectedLogs: []map[string]interface{}{
				{
					"level":          logrus.ErrorLevel,
					"msg":            "Validation of payload failed",
					"error":          errors.New("invalid request, no sourceRepresentation.authorityValue has been supplied"),
					"transaction_id": "transaction_id",
					"uuid":           anotherBasicConceptUUID,
				},
			},
		},
		{
			name: "source representation without authority should be invalid",
			aggConcept: ontology.NewAggregatedConcept{
				AggregateConceptFields: ontology.AggregateConceptFields{
					PrefUUID:  basicConceptUUID,
					PrefLabel: "The Best Label",
					Type:      "Brand",
					SourceRepresentations: []ontology.NewConcept{
						{
							SourceConceptFields: ontology.SourceConceptFields{
								UUID:           anotherBasicConceptUUID,
								PrefLabel:      "The Best Label",
								Type:           "Brand",
								AuthorityValue: "123456-UPP",
							},
						},
					},
				},
			},
			returnedError: "invalid request, no sourceRepresentation.authority has been supplied",
			expectedLogs: []map[string]interface{}{
				{
					"level":          logrus.ErrorLevel,
					"msg":            "Validation of payload failed",
					"error":          errors.New("invalid request, no sourceRepresentation.authority has been supplied"),
					"transaction_id": "transaction_id",
					"uuid":           anotherBasicConceptUUID,
				},
			},
		},
		{
			name: "source representation with unknown authority should be invalid",
			aggConcept: ontology.NewAggregatedConcept{
				AggregateConceptFields: ontology.AggregateConceptFields{
					PrefUUID:  basicConceptUUID,
					PrefLabel: "The Best Label",
					Type:      "Brand",
					SourceRepresentations: []ontology.NewConcept{
						{
							SourceConceptFields: ontology.SourceConceptFields{
								UUID:           anotherBasicConceptUUID,
								PrefLabel:      "The Best Label",
								Type:           "Brand",
								Authority:      "Invalid",
								AuthorityValue: "123456-UPP",
							},
						},
					},
				},
			},
			returnedError: "unknown authority",
			expectedLogs: []map[string]interface{}{
				{
					"level":          logrus.DebugLevel,
					"msg":            "Unknown authority supplied in the request: Invalid",
					"transaction_id": "transaction_id",
					"uuid":           basicConceptUUID,
				},
			},
		},
		{
			name: "valid concept",
			aggConcept: ontology.NewAggregatedConcept{
				AggregateConceptFields: ontology.AggregateConceptFields{
					PrefUUID:  basicConceptUUID,
					PrefLabel: "The Best Label",
					Type:      "Brand",
					SourceRepresentations: []ontology.NewConcept{
						{
							SourceConceptFields: ontology.SourceConceptFields{
								UUID:           anotherBasicConceptUUID,
								PrefLabel:      "The Best Label",
								Type:           "Brand",
								Authority:      "UPP",
								AuthorityValue: "123456-UPP",
							},
						},
					},
				},
				DynamicFields: ontology.DynamicFields{
					Properties: ontology.Properties{
						"aliases":     []string{"alias1", "alias2"},
						"yearFounded": 2000,
					},
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			hook := new(logTest.Hook)
			conceptsDriver.log.AddHook(hook)

			err := conceptsDriver.validateObject(test.aggConcept, "transaction_id")
			if err != nil {
				assert.NotEmpty(t, test.returnedError, "test.returnedError should not be empty when there is an error")
				assert.Contains(t, err.Error(), test.returnedError, test.name)
				assertValidLogs(t, hook, test.expectedLogs)
			} else {
				assert.Empty(t, test.returnedError, "test.returnedError should be empty when there is no error")
				assert.NoError(t, err, test.name)
			}
		})
	}
}

func assertValidLogs(t *testing.T, hook *logTest.Hook, expectedLogs []map[string]interface{}) {
	t.Helper()
	entries := hook.AllEntries()
	if len(entries) != len(expectedLogs) {
		t.Fatalf("missing logs. expected %d, but logged %d", len(entries), len(expectedLogs))
	}

	opts := cmp.Options{
		cmp.Comparer(func(l, r error) bool {
			return l.Error() == r.Error()
		}),
	}
	for idx, entry := range entries {
		expectedLog := expectedLogs[idx]
		for key, expected := range expectedLog {
			var got interface{}
			var ok bool
			switch key {
			case "level":
				got = entry.Level
				ok = true
			case "msg":
				got = entry.Message
				ok = true
			default:
				got, ok = entry.Data[key]
			}

			if !ok {
				t.Fatalf("expected log entry %d to have key %s", idx, key)
			}
			if !cmp.Equal(expected, got, opts...) {
				t.Fatalf("mismatch log_%d: field '%s': %s", idx, key, cmp.Diff(expected, got, opts...))
			}
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

//nolint:gocognit
func TestConceptService_Delete(t *testing.T) {
	tests := []struct {
		testName             string
		aggregatedConcept    ontology.NewAggregatedConcept
		otherRelatedConcepts []ontology.NewAggregatedConcept
		expectedErr          error
		uuidsToDelete        []string
		affectedUUIDs        []string
	}{
		{
			testName:          "Deletes a canonical concept with a single source",
			aggregatedConcept: getAggregatedConcept(t, "single-concordance.json"),
			uuidsToDelete:     []string{basicConceptUUID},
			affectedUUIDs:     []string{basicConceptUUID},
		},
		{
			testName:          "Deletes a concept which has outgoing relationship",
			aggregatedConcept: getAggregatedConcept(t, "concept-with-multiple-related-to.json"),
			uuidsToDelete:     []string{basicConceptUUID},
			affectedUUIDs:     []string{basicConceptUUID},
		},
		{
			testName:          "Throws an error when deleting a source concept different from the canonical",
			aggregatedConcept: getAggregatedConcept(t, "tri-concordance.json"),
			expectedErr:       ErrDeleteSource,
			uuidsToDelete:     []string{sourceID1},
			affectedUUIDs:     []string{basicConceptUUID},
		},
		{
			testName:          "Throws an error when deleting a concept that has relations",
			aggregatedConcept: getAggregatedConcept(t, "concept-with-multiple-related-to.json"),
			otherRelatedConcepts: []ontology.NewAggregatedConcept{
				getAggregatedConcept(t, "yet-another-full-lone-aggregated-concept.json"),
			},
			expectedErr:   ErrDeleteRelated,
			uuidsToDelete: []string{yetAnotherBasicConceptUUID},
			affectedUUIDs: []string{basicConceptUUID},
		},
		{
			testName:          "Throws an error when deleting a concept with concordances which have relations to other things",
			aggregatedConcept: getAggregatedConcept(t, "concept-with-related-to.json"),
			otherRelatedConcepts: []ontology.NewAggregatedConcept{
				getAggregatedConcept(t, "transfer-multiple-source-concordance.json"),
			},
			expectedErr:   ErrDeleteRelated,
			uuidsToDelete: []string{simpleSmartlogicTopicUUID},
			affectedUUIDs: []string{basicConceptUUID},
		},
	}

	for _, test := range tests {
		t.Run(test.testName, func(t *testing.T) {
			defer cleanDB(t)

			// Create the related, broader than and impliedBy on concepts
			for _, relatedConcept := range test.otherRelatedConcepts {
				_, err := conceptsDriver.Write(relatedConcept, "")
				if !assert.NoError(t, err, "Failed to write related/broader/impliedBy concept") {
					return
				}
			}
			_, err := conceptsDriver.Write(test.aggregatedConcept, "")
			assert.Nil(t, err)

			// Attempt to delete the chosen UUIDs.
			for _, uuid := range test.uuidsToDelete {
				affected, err := conceptsDriver.Delete(uuid, "")
				if test.expectedErr != nil {
					assert.Equal(t, test.expectedErr, err)
				} else {
					assert.Nil(t, err)
				}
				assert.Equal(t, len(test.affectedUUIDs), len(affected))
				assert.Subset(t, test.affectedUUIDs, affected)
			}

			// Check if the deletion was actually successful if this was expected
			if test.expectedErr == nil {
				for _, uuid := range test.uuidsToDelete {
					query := &cmneo4j.Query{
						Cypher: "MATCH (n:Concept{uuid:$uuid}) RETURN n",
						Params: map[string]interface{}{"uuid": uuid},
						Result: &struct{}{},
					}
					err := conceptsDriver.driver.Read(query)
					assert.ErrorIs(t, err, cmneo4j.ErrNoResultsFound, "UUID: %s", uuid)
				}
			}
		})
	}
}

func TestConceptService_DeleteConcordedCanonical(t *testing.T) {
	defer cleanDB(t)

	aggregatedConcept := getAggregatedConcept(t, "tri-concordance.json")
	_, err := conceptsDriver.Write(aggregatedConcept, "")
	assert.Nil(t, err)

	expectedUUIDs := []string{}
	for _, concept := range aggregatedConcept.SourceRepresentations {
		expectedUUIDs = append(expectedUUIDs, concept.UUID)
	}

	affected, err := conceptsDriver.Delete(aggregatedConcept.PrefUUID, "")
	assert.Nil(t, err)
	assert.Equal(t, len(expectedUUIDs), len(affected))
	assert.Subset(t, expectedUUIDs, affected)

	// All source representations should be deleted also
	for _, c := range aggregatedConcept.SourceRepresentations {
		err := conceptsDriver.driver.Read(&cmneo4j.Query{
			Cypher: "MATCH (n:Concept{uuid:$uuid}) RETURN n",
			Params: map[string]interface{}{
				"uuid": c.UUID,
			},
			Result: &struct{}{},
		})
		assert.ErrorIs(t, err, cmneo4j.ErrNoResultsFound, "UUID: %s", c.UUID)
	}
}

func readConceptAndCompare(t *testing.T, payload ontology.NewAggregatedConcept, testName string) {
	actualIf, found, err := conceptsDriver.Read(payload.PrefUUID, "")
	actual := actualIf.(ontology.NewAggregatedConcept)

	assert.NoError(t, err, fmt.Sprintf("Test %s failed: Transformation Error occurred", testName))
	expected := cleanSourceProperties(payload)
	expected = cleanHash(expected)
	expected = cleanNewAggregatedConcept(expected)
	actual = cleanSourceProperties(actual)
	actual = cleanHash(actual)
	actual = cleanNewAggregatedConcept(actual)
	lessFunc := cmpopts.SortSlices(func(x, y interface{}) bool {
		return fmt.Sprint(x) < fmt.Sprint(y)
	})

	if !cmp.Equal(expected, actual, lessFunc) {
		t.Errorf("Test %s failed: Concepts were not equal:\n%s", testName, cmp.Diff(expected, actual, lessFunc))
	}

	assert.NoError(t, err, fmt.Sprintf("Test %s failed: Unexpected Error occurred", testName))
	assert.True(t, found, fmt.Sprintf("Test %s failed: Concept has not been found", testName))
}

func readTestSetup(t *testing.T, filename string) ([]ontology.NewAggregatedConcept, []string, []string) {
	t.Helper()
	f, err := os.Open(filename)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	result := []ontology.NewAggregatedConcept{}
	err = json.NewDecoder(f).Decode(&result)
	if err != nil {
		t.Fatal(err)
	}
	var canonicalUUIDs []string
	var sourceUUIDs []string
	for _, concept := range result {
		canonicalUUIDs = append(canonicalUUIDs, concept.PrefUUID)
		sourceUUIDs = append(sourceUUIDs, collectRelatedUUIDs(concept)...)
	}
	return result, canonicalUUIDs, sourceUUIDs
}

func collectRelatedUUIDs(concept ontology.NewAggregatedConcept) []string {
	var result []string
	for _, src := range concept.SourceRepresentations {
		result = append(result, src.UUID)
		for i := range src.Relationships {
			result = append(result, src.Relationships[i].UUID)
		}
		for i := range src.Properties {
			_, ok := src.Properties[i].(string)
			if ok {
				result = append(result, src.Properties[i].(string))
			}
			_, ok = src.Properties[i].([]string)
			if ok {
				result = append(result, src.Properties[i].([]string)...)
			}
		}
	}
	set := map[string]bool{}
	for _, uuid := range result {
		if uuid != "" {
			set[uuid] = true
		}
	}
	result = []string{}
	for uuid := range set {
		result = append(result, uuid)
	}
	return result
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
		membershipRole.UUID,
		personUUID,
		organisationUUID,
		membershipUUID,
		anotherMembershipRole.UUID,
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
		membershipRole.UUID,
		personUUID,
		organisationUUID,
		membershipUUID,
		anotherMembershipRole.UUID,
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
		membershipRole.UUID,
		personUUID,
		organisationUUID,
		membershipUUID,
		anotherMembershipRole.UUID,
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
	qs := make([]*cmneo4j.Query, len(uuids))
	for i, uuid := range uuids {
		qs[i] = &cmneo4j.Query{
			Cypher: `MATCH (a:Thing {uuid: $uuid}) DETACH DELETE a`,
			Params: map[string]interface{}{"uuid": uuid},
		}
	}
	err := driver.Write(qs...)
	assert.NoError(t, err, "Error executing clean up cypher")
}

func cleanSourceNodes(t *testing.T, uuids ...string) {
	qs := make([]*cmneo4j.Query, len(uuids))
	for i, uuid := range uuids {
		qs[i] = &cmneo4j.Query{
			Cypher: `MATCH (a:Thing {uuid: $uuid})
			         OPTIONAL MATCH (a)-[hp:HAS_PARENT]-(p)
			         DELETE hp`,
			Params: map[string]interface{}{"uuid": uuid},
		}
	}
	err := driver.Write(qs...)
	assert.NoError(t, err, "Error executing clean up cypher")
}

func deleteConcordedNodes(t *testing.T, uuids ...string) {
	qs := make([]*cmneo4j.Query, len(uuids))
	for i, uuid := range uuids {
		qs[i] = &cmneo4j.Query{
			Cypher: `MATCH (a:Thing {prefUUID: $uuid})
			         OPTIONAL MATCH (a)-[rel]-(i)
			         DELETE rel, i, a`,
			Params: map[string]interface{}{"uuid": uuid},
		}
	}
	err := driver.Write(qs...)
	assert.NoError(t, err, "Error executing clean up cypher")
}

func verifyAggregateHashIsCorrect(t *testing.T, concept ontology.NewAggregatedConcept, testName string) {
	var results []struct {
		Hash string `json:"a.aggregateHash"`
	}

	query := &cmneo4j.Query{
		Cypher: `
			MATCH (a:Thing {prefUUID: $uuid})
			RETURN a.aggregateHash`,
		Params: map[string]interface{}{
			"uuid": concept.PrefUUID,
		},
		Result: &results,
	}
	err := driver.Read(query)
	assert.NoError(t, err, fmt.Sprintf("Error while retrieving concept hash"))

	assert.NoError(t, err)
	conceptHash, _ := hashstructure.Hash(cleanSourceProperties(concept), nil)
	hashAsString := strconv.FormatUint(conceptHash, 10)
	assert.Equal(t, hashAsString, results[0].Hash, fmt.Sprintf("Test %s failed: Concept hash %s and stored record %s are not equal!", testName, hashAsString, results[0].Hash))
}

func cleanNewAggregatedConcept(c ontology.NewAggregatedConcept) ontology.NewAggregatedConcept {
	for i := range c.SourceRepresentations {
		c.SourceRepresentations[i].LastModifiedEpoch = 0
		cleanSourceRepresentationsProperties(c, i)
		cleanSourceRepresentationsRelationships(c, i)
	}

	cleanNewAggregatedConceptProperties(c)
	cleanNewAggregatedConceptRelationships(c)
	return c
}

func cleanSourceRepresentationsProperties(c ontology.NewAggregatedConcept, i int) {
	for r := range c.SourceRepresentations[i].Properties {
		cleanIntProperties(c.SourceRepresentations[i].Properties, r)
	}
}

func cleanSourceRepresentationsRelationships(c ontology.NewAggregatedConcept, i int) {
	for q := range c.SourceRepresentations[i].Relationships {
		prop := make(map[string]interface{})
		for p := range c.SourceRepresentations[i].Relationships[q].Properties {
			if p != "inceptionDateEpoch" && p != "terminationDateEpoch" {
				prop[p] = c.SourceRepresentations[i].Relationships[q].Properties[p]
			}

			if p == "rank" {
				s, ok := c.SourceRepresentations[i].Relationships[q].Properties[p].(float64)
				if ok {
					k := int(s)
					prop[p] = k
				}
			}
		}
		c.SourceRepresentations[i].Relationships[q].Properties = prop
	}
}

func cleanNewAggregatedConceptProperties(c ontology.NewAggregatedConcept) {
	for i := range c.Properties {
		cleanArrayProperties(c.Properties, i)
		cleanIntProperties(c.Properties, i)
	}
}

func cleanArrayProperties(c ontology.Properties, i string) {
	if i == "aliases" || i == "formerNames" || i == "tradeNames" {
		s, ok := c[i].([]interface{})
		if ok {
			tempArray := make([]string, 0, len(s))
			for _, str := range s {
				k, ok := str.(string)
				if ok {
					tempArray = append(tempArray, k)
				}
			}
			c[i] = tempArray
		}
	}
}

func cleanIntProperties(c ontology.Properties, i string) {
	if i == "birthYear" || i == "yearFounded" || i == "rank" {
		s, ok := c[i].(float64)
		if ok {
			k := int(s)
			c[i] = k
		}
	}
}

func cleanNewAggregatedConceptRelationships(c ontology.NewAggregatedConcept) {
	for i := range c.Relationships {
		prop := make(map[string]interface{})
		for p := range c.Relationships[i].Properties {
			if p != "inceptionDateEpoch" && p != "terminationDateEpoch" {
				prop[p] = c.Relationships[i].Properties[p]
			}
		}
		c.Relationships[i].Properties = prop
	}
}

func cleanHash(c ontology.NewAggregatedConcept) ontology.NewAggregatedConcept {
	c.AggregatedHash = ""
	return c
}
