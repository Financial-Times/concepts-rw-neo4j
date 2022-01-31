package concepts

import (
	"encoding/json"
	"sort"

	"github.com/Financial-Times/neo-model-utils-go/mapper"

	"github.com/Financial-Times/concepts-rw-neo4j/ontology"
)

type neoAggregatedConcept struct {
	AggregateHash         string                    `json:"aggregateHash,omitempty"`
	Aliases               []string                  `json:"aliases,omitempty"`
	DescriptionXML        string                    `json:"descriptionXML,omitempty"`
	EmailAddress          string                    `json:"emailAddress,omitempty"`
	FacebookPage          string                    `json:"facebookPage,omitempty"`
	FigiCode              string                    `json:"figiCode,omitempty"`
	ImageURL              string                    `json:"imageUrl,omitempty"`
	InceptionDate         string                    `json:"inceptionDate,omitempty"`
	InceptionDateEpoch    int64                     `json:"inceptionDateEpoch,omitempty"`
	IssuedBy              string                    `json:"issuedBy,omitempty"`
	LastModifiedEpoch     int                       `json:"lastModifiedEpoch,omitempty"`
	MembershipRoles       []ontology.MembershipRole `json:"membershipRoles,omitempty"`
	OrganisationUUID      string                    `json:"organisationUUID,omitempty"`
	PersonUUID            string                    `json:"personUUID,omitempty"`
	PrefLabel             string                    `json:"prefLabel"`
	PrefUUID              string                    `json:"prefUUID,omitempty"`
	ScopeNote             string                    `json:"scopeNote,omitempty"`
	ShortLabel            string                    `json:"shortLabel,omitempty"`
	SourceRepresentations []neoConcept              `json:"sourceRepresentations"`
	Strapline             string                    `json:"strapline,omitempty"`
	TerminationDate       string                    `json:"terminationDate,omitempty"`
	TerminationDateEpoch  int64                     `json:"terminationDateEpoch,omitempty"`
	TwitterHandle         string                    `json:"twitterHandle,omitempty"`
	Types                 []string                  `json:"types"`
	IsDeprecated          bool                      `json:"isDeprecated,omitempty"`
	// Organisations
	ProperName             string   `json:"properName,omitempty"`
	ShortName              string   `json:"shortName,omitempty"`
	TradeNames             []string `json:"tradeNames,omitempty"`
	FormerNames            []string `json:"formerNames,omitempty"`
	CountryCode            string   `json:"countryCode,omitempty"`
	CountryOfRisk          string   `json:"countryOfRisk,omitempty"`
	CountryOfIncorporation string   `json:"countryOfIncorporation,omitempty"`
	CountryOfOperations    string   `json:"countryOfOperations,omitempty"`
	PostalCode             string   `json:"postalCode,omitempty"`
	YearFounded            int      `json:"yearFounded,omitempty"`
	LeiCode                string   `json:"leiCode,omitempty"`
	ParentOrganisation     string   `json:"parentOrganisation,omitempty"`
	// Location
	ISO31661 string `json:"iso31661,omitempty"`
	// Person
	Salutation string `json:"salutation,omitempty"`
	BirthYear  int    `json:"birthYear,omitempty"`
	// Industry Classifications
	IndustryIdentifier string `json:"industryIdentifier,omitempty"`
}

func (nac neoAggregatedConcept) ToOntologyNewAggregateConcept(ontologyCfg ontology.Config) (ontology.NewAggregatedConcept, string, error) {
	typeName, err := mapper.MostSpecificType(nac.Types)
	if err != nil {
		return ontology.NewAggregatedConcept{}, "Returned concept had no recognized type", err
	}

	var sourceConcepts []ontology.NewConcept
	for _, srcConcept := range nac.SourceRepresentations {
		concept, err := srcConcept.ТоOntologyNewConcept(ontologyCfg.Relationships)
		if err != nil {
			return ontology.NewAggregatedConcept{}, "Returned source concept had no recognized type", err
		}

		sourceConcepts = append(sourceConcepts, concept)
	}

	nacMap := map[string]interface{}{}
	nacBytes, _ := json.Marshal(nac)
	_ = json.Unmarshal(nacBytes, &nacMap)

	props := map[string]interface{}{}
	for field, propCfg := range ontologyCfg.Fields {
		if val, ok := nacMap[propCfg.NeoProp]; ok {
			props[field] = val
		}
	}

	aggregateConcept := ontology.NewAggregatedConcept{
		Properties:            props,
		SourceRepresentations: sourceConcepts,
		AggregatedHash:        nac.AggregateHash,
		FigiCode:              nac.FigiCode,
		InceptionDate:         nac.InceptionDate,
		IssuedBy:              nac.IssuedBy,
		OrganisationUUID:      nac.OrganisationUUID,
		PersonUUID:            nac.PersonUUID,
		PrefLabel:             nac.PrefLabel,
		PrefUUID:              nac.PrefUUID,
		TerminationDate:       nac.TerminationDate,
		Type:                  typeName,
		IsDeprecated:          nac.IsDeprecated,
	}

	return sortSources(aggregateConcept), "", nil
}

type neoConcept struct {
	Authority            string                    `json:"authority,omitempty"`
	AuthorityValue       string                    `json:"authorityValue,omitempty"`
	BroaderUUIDs         []string                  `json:"broaderUUIDs,omitempty"`
	FigiCode             string                    `json:"figiCode,omitempty"`
	InceptionDate        string                    `json:"inceptionDate,omitempty"`
	InceptionDateEpoch   int64                     `json:"inceptionDateEpoch,omitempty"`
	IssuedBy             string                    `json:"issuedBy,omitempty"`
	LastModifiedEpoch    int                       `json:"lastModifiedEpoch,omitempty"`
	MembershipRoles      []ontology.MembershipRole `json:"membershipRoles,omitempty"`
	OrganisationUUID     string                    `json:"organisationUUID,omitempty"`
	ParentUUIDs          []string                  `json:"parentUUIDs,omitempty"`
	PersonUUID           string                    `json:"personUUID,omitempty"`
	PrefLabel            string                    `json:"prefLabel,omitempty"`
	PrefUUID             string                    `json:"prefUUID,omitempty"`
	RelatedUUIDs         []string                  `json:"relatedUUIDs,omitempty"`
	SupersededByUUIDs    []string                  `json:"supersededByUUIDs,omitempty"`
	ImpliedByUUIDs       []string                  `json:"impliedByUUIDs,omitempty"`
	HasFocusUUIDs        []string                  `json:"hasFocusUUIDs,omitempty"`
	TerminationDate      string                    `json:"terminationDate,omitempty"`
	TerminationDateEpoch int64                     `json:"terminationDateEpoch,omitempty"`
	Types                []string                  `json:"types,omitempty"`
	UUID                 string                    `json:"uuid,omitempty"`
	IsDeprecated         bool                      `json:"isDeprecated,omitempty"`
	// Organisations
	CountryOfRiskUUID            string                                 `json:"countryOfRiskUUID,omitempty"`
	CountryOfIncorporationUUID   string                                 `json:"countryOfIncorporationUUID,omitempty"`
	CountryOfOperationsUUID      string                                 `json:"countryOfOperationsUUID,omitempty"`
	ParentOrganisation           string                                 `json:"parentOrganisation,omitempty"`
	NAICSIndustryClassifications []ontology.NAICSIndustryClassification `json:"naicsIndustryClassifications,omitempty"`
}

func (nc neoConcept) ТоOntologyNewConcept(ontologyRels map[string]ontology.RelationshipConfig) (ontology.NewConcept, error) {
	conceptType, err := mapper.MostSpecificType(nc.Types)
	if err != nil {
		return ontology.NewConcept{}, err
	}

	ncMap := map[string]interface{}{}
	ncBytes, _ := json.Marshal(nc)
	_ = json.Unmarshal(ncBytes, &ncMap)

	rels := []ontology.Relationship{}
	for rel, relCfg := range ontologyRels {
		if _, ok := ncMap[relCfg.ConceptField]; !ok {
			continue
		}

		val := ncMap[relCfg.ConceptField]

		if relCfg.OneToOne {
			uuid := val.(string)
			rels = append(rels, ontology.Relationship{UUID: uuid, Label: rel})
		} else {
			for _, v := range val.([]interface{}) {
				if len(relCfg.Properties) > 0 {
					relMap := v.(map[string]interface{})
					uuid, ok := relMap["uuid"]
					if ok {
						delete(relMap, "uuid")

						rels = append(rels, ontology.Relationship{UUID: uuid.(string), Label: rel, Properties: relMap})
						continue
					}

					// Handle membership roles as special case
					uuid, ok = relMap["membershipRoleUUID"]
					if !ok {
						continue
					}

					delete(relMap, "membershipRoleUUID")

					if _, ok := relMap["inceptionDateEpoch"]; ok {
						relMap["inceptionDateEpoch"] = 0
					}

					if _, ok := relMap["terminationDateEpoch"]; ok {
						relMap["terminationDateEpoch"] = 0
					}

					rels = append(rels, ontology.Relationship{UUID: uuid.(string), Label: rel, Properties: relMap})
				} else {
					uuid := v.(string)
					rels = append(rels, ontology.Relationship{UUID: uuid, Label: rel})
				}
			}
		}
	}

	return ontology.NewConcept{
		Relationships:     filterRelationships(rels),
		Authority:         nc.Authority,
		AuthorityValue:    nc.AuthorityValue,
		FigiCode:          nc.FigiCode,
		IssuedBy:          nc.IssuedBy,
		LastModifiedEpoch: 0,
		MembershipRoles:   cleanMembershipRoles(nc.MembershipRoles),
		PrefLabel:         nc.PrefLabel,
		Type:              conceptType,
		UUID:              nc.UUID,
		IsDeprecated:      nc.IsDeprecated,
	}, nil
}

func filterSlice(a []string) []string {
	r := []string{}
	for _, str := range a {
		if str != "" {
			r = append(r, str)
		}
	}

	if len(r) == 0 {
		return nil
	}

	return r
}

func filterRelationships(rels []ontology.Relationship) []ontology.Relationship {
	filtered := []ontology.Relationship{}
	for _, rel := range rels {
		if rel.UUID != "" {
			filtered = append(filtered, rel)
		}
	}

	if len(filtered) == 0 {
		return nil
	}

	return filtered
}

func cleanMembershipRoles(m []ontology.MembershipRole) []ontology.MembershipRole {
	deleted := 0
	for i := range m {
		j := i - deleted
		if m[j].RoleUUID == "" {
			m = m[:j+copy(m[j:], m[j+1:])]
			deleted++
			continue
		}

		m[j].InceptionDateEpoch = 0
		m[j].TerminationDateEpoch = 0
	}

	if len(m) == 0 {
		return nil
	}

	return m
}

// cleanNAICS returns the same slice of NAICSIndustryClassification if all are valid,
// skips the invalid ones, returns nil if the input slice doesn't have valid NAICSIndustryClassification objects
func cleanNAICS(naics []ontology.NAICSIndustryClassification) []ontology.NAICSIndustryClassification {
	var res []ontology.NAICSIndustryClassification
	for _, ic := range naics {
		if ic.UUID != "" {
			res = append(res, ic)
		}
	}
	return res
}

func sortSources(c ontology.NewAggregatedConcept) ontology.NewAggregatedConcept {
	for j := range c.SourceRepresentations {
		sort.SliceStable(c.SourceRepresentations[j].MembershipRoles, func(k, l int) bool {
			return c.SourceRepresentations[j].MembershipRoles[k].RoleUUID < c.SourceRepresentations[j].MembershipRoles[l].RoleUUID
		})
	}

	sort.SliceStable(c.SourceRepresentations, func(k, l int) bool {
		return c.SourceRepresentations[k].UUID < c.SourceRepresentations[l].UUID
	})
	return c
}
