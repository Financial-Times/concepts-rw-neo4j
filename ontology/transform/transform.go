package transform

import (
	"encoding/json"
	"fmt"
	"sort"

	"github.com/Financial-Times/concepts-rw-neo4j/ontology"
)

func TransformToNewAggregateConcept(old AggregatedConcept) (ontology.NewAggregatedConcept, error) {
	var newSources []ontology.NewConcept
	for _, s := range old.SourceRepresentations {
		src, err := TransformToNewSourceConcept(s)
		if err != nil {
			return ontology.NewAggregatedConcept{}, err
		}
		newSources = append(newSources, src)
	}

	oldMap := map[string]interface{}{}
	oldBytes, _ := json.Marshal(old)
	if err := json.Unmarshal(oldBytes, &oldMap); err != nil {
		return ontology.NewAggregatedConcept{}, err
	}

	props := map[string]interface{}{}
	for field, cfg := range ontology.GetConfig().Fields {
		var v interface{}
		val, ok := oldMap[field]
		if !ok {
			continue
		}
		fieldType := cfg.FieldType
		switch fieldType {
		case "string":
			if v, ok = toString(val); !ok {
				return ontology.NewAggregatedConcept{}, ontology.InvalidPropValueError(field, v)
			}
		case "[]string":
			if v, ok = toStringSlice(val); !ok {
				return ontology.NewAggregatedConcept{}, ontology.InvalidPropValueError(field, v)
			}
		case "int":
			if v, ok = toInt(val); !ok {
				return ontology.NewAggregatedConcept{}, ontology.InvalidPropValueError(field, v)
			}
		default:
			return ontology.NewAggregatedConcept{},
				fmt.Errorf("unsupported field type '%s' for prop '%s': %w", fieldType, field, ontology.ErrUnknownProperty)
		}
		props[field] = v
	}

	return ontology.NewAggregatedConcept{
		Properties:            props,
		PrefUUID:              old.PrefUUID,
		PrefLabel:             old.PrefLabel,
		Type:                  old.Type,
		IsDeprecated:          old.IsDeprecated,
		OrganisationUUID:      old.OrganisationUUID,
		PersonUUID:            old.PersonUUID,
		AggregatedHash:        old.AggregatedHash,
		InceptionDate:         old.InceptionDate,
		TerminationDate:       old.TerminationDate,
		FigiCode:              old.FigiCode,
		IssuedBy:              old.IssuedBy,
		SourceRepresentations: newSources,
	}, nil
}

func TransformToOldAggregateConcept(new ontology.NewAggregatedConcept) (AggregatedConcept, error) {
	var oldSources []Concept
	var roles []MembershipRole
	for _, s := range new.SourceRepresentations {
		oldSource, err := TransformToOldSourceConcept(s)
		if err != nil {
			return AggregatedConcept{}, err
		}

		for _, r := range oldSource.MembershipRoles {
			if r.RoleUUID == "" {
				continue
			}
			roles = append(roles, r)
		}

		oldSources = append(oldSources, oldSource)
	}

	old := AggregatedConcept{}
	newPropsBytes, err := json.Marshal(new.Properties)
	if err != nil {
		return AggregatedConcept{}, err
	}
	err = json.Unmarshal(newPropsBytes, &old)
	if err != nil {
		return AggregatedConcept{}, err
	}

	old.PrefUUID = new.PrefUUID
	old.PrefLabel = new.PrefLabel
	old.Type = new.Type
	old.OrganisationUUID = new.OrganisationUUID
	old.PersonUUID = new.PersonUUID
	old.AggregatedHash = new.AggregatedHash
	old.MembershipRoles = roles
	old.InceptionDate = new.InceptionDate
	old.TerminationDate = new.TerminationDate
	old.FigiCode = new.FigiCode
	old.IssuedBy = new.IssuedBy
	old.IsDeprecated = new.IsDeprecated
	old.SourceRepresentations = oldSources

	return old, nil
}

// nolint: gocognit // TODO: simplify this function
func TransformToNewSourceConcept(old Concept) (ontology.NewConcept, error) {
	oldMap := map[string]interface{}{}
	oldBytes, _ := json.Marshal(old)
	if err := json.Unmarshal(oldBytes, &oldMap); err != nil {
		return ontology.NewConcept{}, err
	}

	rels := []ontology.Relationship{}
	for rel, relCfg := range ontology.GetConfig().Relationships {
		if _, ok := oldMap[relCfg.ConceptField]; !ok {
			continue
		}

		val := oldMap[relCfg.ConceptField]

		if relCfg.OneToOne {
			uuid := val.(string)
			rels = append(rels, ontology.Relationship{UUID: uuid, Label: rel})
		} else {
			for _, v := range val.([]interface{}) {
				if len(relCfg.Properties) > 0 {
					// extract uuid
					relMap := v.(map[string]interface{})
					extractUUIDFunc := func(props map[string]interface{}) (string, string, bool) {
						uuid, ok := props["uuid"]
						if ok {
							return uuid.(string), "uuid", true
						}

						// Handle membership roles as special case
						uuid, ok = relMap["membershipRoleUUID"]
						if ok {
							return uuid.(string), "membershipRoleUUID", true
						}

						return "", "", false

					}
					uuid, uuidKey, ok := extractUUIDFunc(relMap)

					reTypeProps := func(props map[string]interface{}, config ontology.RelationshipConfig) (map[string]interface{}, error) {
						for field, fieldType := range config.Properties {
							var v interface{}
							val, ok := props[field]
							if !ok {
								props[field] = nil
								continue
							}

							switch fieldType {
							case "date":
								if v, ok = toString(val); !ok {
									return nil, ontology.InvalidPropValueError(field, v)
								}
							case "string":
								if v, ok = toString(val); !ok {
									return nil, ontology.InvalidPropValueError(field, v)
								}
							case "[]string":
								if v, ok = toStringSlice(val); !ok {
									return nil, ontology.InvalidPropValueError(field, v)
								}
							case "int":
								if v, ok = toInt(val); !ok {
									return nil, ontology.InvalidPropValueError(field, v)
								}
							default:
								return nil,
									fmt.Errorf("unsupported field type '%s' for prop '%s': %w", fieldType, field, ontology.ErrUnknownProperty)
							}
							props[field] = v
						}
						return props, nil
					}

					if ok {
						delete(relMap, uuidKey)
						relMap, err := reTypeProps(relMap, relCfg)
						if err != nil {
							return ontology.NewConcept{}, err
						}
						rels = append(rels, ontology.Relationship{UUID: uuid, Label: rel, Properties: relMap})
					}
				} else {
					uuid := v.(string)
					rels = append(rels, ontology.Relationship{UUID: uuid, Label: rel})
				}
			}
		}
	}

	sort.SliceStable(rels, func(i, j int) bool {
		if rels[i].UUID != rels[j].UUID {
			return rels[i].UUID < rels[j].UUID
		}

		return rels[i].Label < rels[j].Label
	})

	return ontology.NewConcept{
		Relationships:     rels,
		UUID:              old.UUID,
		PrefLabel:         old.PrefLabel,
		Type:              old.Type,
		Authority:         old.Authority,
		AuthorityValue:    old.AuthorityValue,
		LastModifiedEpoch: old.LastModifiedEpoch,
		Hash:              old.Hash,
		FigiCode:          old.FigiCode,
		IssuedBy:          old.IssuedBy,
		IsDeprecated:      old.IsDeprecated,
	}, nil
}

// nolint: gocognit // TODO: simplify this function
func TransformToOldSourceConcept(new ontology.NewConcept) (Concept, error) {
	oldMap := map[string]interface{}{}
	for _, rel := range new.Relationships {
		if rel.UUID == "" {
			continue
		}

		if _, ok := ontology.GetConfig().Relationships[rel.Label]; !ok {
			continue
		}

		relCfg := ontology.GetConfig().Relationships[rel.Label]
		if relCfg.OneToOne {
			oldMap[relCfg.ConceptField] = rel.UUID
			continue
		}

		relVal, ok := oldMap[relCfg.ConceptField]
		if !ok {
			if len(relCfg.Properties) > 0 {
				relProps := rel.Properties
				if rel.Label == "HAS_ROLE" {
					relProps["membershipRoleUUID"] = rel.UUID
				} else {
					relProps["uuid"] = rel.UUID
				}

				oldMap[relCfg.ConceptField] = []map[string]interface{}{relProps}
			} else {
				oldMap[relCfg.ConceptField] = []string{rel.UUID}
			}
			continue
		}

		if len(relCfg.Properties) > 0 {
			relProps := rel.Properties
			if rel.Label == "HAS_ROLE" {
				relProps["membershipRoleUUID"] = rel.UUID
			} else {
				relProps["uuid"] = rel.UUID
			}

			rels := relVal.([]map[string]interface{})
			rels = append(rels, relProps)

			oldMap[relCfg.ConceptField] = rels
		} else {
			relUUIDs := relVal.([]string)
			relUUIDs = append(relUUIDs, rel.UUID)
			oldMap[relCfg.ConceptField] = relUUIDs
		}
	}

	old := Concept{}
	relMapBytes, _ := json.Marshal(oldMap)
	if err := json.Unmarshal(relMapBytes, &old); err != nil {
		return Concept{}, err
	}

	old.UUID = new.UUID
	old.PrefLabel = new.PrefLabel
	old.Type = new.Type
	old.Authority = new.Authority
	old.AuthorityValue = new.AuthorityValue
	old.LastModifiedEpoch = new.LastModifiedEpoch
	old.Hash = new.Hash
	old.FigiCode = new.FigiCode
	old.IssuedBy = new.IssuedBy
	old.IsDeprecated = new.IsDeprecated

	return old, nil
}

func toString(val interface{}) (string, bool) {
	str, ok := val.(string)
	return str, ok
}

func toInt(val interface{}) (int, bool) {
	switch v := val.(type) {
	case int:
		return v, true
	case float64:
		return int(v), true
	default:
		return 0, false
	}
}

func toStringSlice(val interface{}) ([]string, bool) {
	if vs, ok := val.([]string); ok {
		return vs, ok
	}
	vs, ok := val.([]interface{})
	if !ok {
		return nil, false
	}
	var result []string
	for _, v := range vs {
		if str, ok := v.(string); ok {
			result = append(result, str)
		}
	}
	if len(result) != len(vs) {
		return nil, false
	}
	return result, true
}

type AggregatedConcept struct {
	PrefUUID              string           `json:"prefUUID,omitempty"`
	PrefLabel             string           `json:"prefLabel,omitempty"`
	Type                  string           `json:"type,omitempty"`
	Aliases               []string         `json:"aliases,omitempty"`
	Strapline             string           `json:"strapline,omitempty"`
	DescriptionXML        string           `json:"descriptionXML,omitempty"`
	ImageURL              string           `json:"_imageUrl,omitempty"`
	EmailAddress          string           `json:"emailAddress,omitempty"`
	FacebookPage          string           `json:"facebookPage,omitempty"`
	TwitterHandle         string           `json:"twitterHandle,omitempty"`
	ScopeNote             string           `json:"scopeNote,omitempty"`
	ShortLabel            string           `json:"shortLabel,omitempty"`
	OrganisationUUID      string           `json:"organisationUUID,omitempty"`
	PersonUUID            string           `json:"personUUID,omitempty"`
	AggregatedHash        string           `json:"aggregateHash,omitempty"`
	SourceRepresentations []Concept        `json:"sourceRepresentations,omitempty"`
	MembershipRoles       []MembershipRole `json:"membershipRoles,omitempty"`
	InceptionDate         string           `json:"inceptionDate,omitempty"`
	TerminationDate       string           `json:"terminationDate,omitempty"`
	InceptionDateEpoch    int64            `json:"inceptionDateEpoch,omitempty"`
	TerminationDateEpoch  int64            `json:"terminationDateEpoch,omitempty"`
	FigiCode              string           `json:"figiCode,omitempty"`
	IssuedBy              string           `json:"issuedBy,omitempty"`
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
	IsDeprecated           bool     `json:"isDeprecated,omitempty"`
	// Location
	ISO31661 string `json:"iso31661,omitempty"`
	// Person
	Salutation string `json:"salutation,omitempty"`
	BirthYear  int    `json:"birthYear,omitempty"`
	// Industry Classifications
	IndustryIdentifier string `json:"industryIdentifier,omitempty"`
}

// Concept - could be any concept genre, subject etc
type Concept struct {
	UUID                 string           `json:"uuid,omitempty"`
	PrefLabel            string           `json:"prefLabel,omitempty"`
	Type                 string           `json:"type,omitempty"`
	Authority            string           `json:"authority,omitempty"`
	AuthorityValue       string           `json:"authorityValue,omitempty"`
	LastModifiedEpoch    int              `json:"lastModifiedEpoch,omitempty"`
	Aliases              []string         `json:"aliases,omitempty"`
	ParentUUIDs          []string         `json:"parentUUIDs,omitempty"`
	Strapline            string           `json:"strapline,omitempty"`
	DescriptionXML       string           `json:"descriptionXML,omitempty"`
	ImageURL             string           `json:"_imageUrl,omitempty"`
	EmailAddress         string           `json:"emailAddress,omitempty"`
	FacebookPage         string           `json:"facebookPage,omitempty"`
	TwitterHandle        string           `json:"twitterHandle,omitempty"`
	ScopeNote            string           `json:"scopeNote,omitempty"`
	ShortLabel           string           `json:"shortLabel,omitempty"`
	BroaderUUIDs         []string         `json:"broaderUUIDs,omitempty"`
	RelatedUUIDs         []string         `json:"relatedUUIDs,omitempty"`
	SupersededByUUIDs    []string         `json:"supersededByUUIDs,omitempty"`
	ImpliedByUUIDs       []string         `json:"impliedByUUIDs,omitempty"`
	HasFocusUUIDs        []string         `json:"hasFocusUUIDs,omitempty"`
	OrganisationUUID     string           `json:"organisationUUID,omitempty"`
	PersonUUID           string           `json:"personUUID,omitempty"`
	Hash                 string           `json:"hash,omitempty"`
	MembershipRoles      []MembershipRole `json:"membershipRoles,omitempty"`
	InceptionDate        string           `json:"inceptionDate,omitempty"`
	TerminationDate      string           `json:"terminationDate,omitempty"`
	InceptionDateEpoch   int64            `json:"inceptionDateEpoch,omitempty"`
	TerminationDateEpoch int64            `json:"terminationDateEpoch,omitempty"`
	FigiCode             string           `json:"figiCode,omitempty"`
	IssuedBy             string           `json:"issuedBy,omitempty"`
	// Organisations
	ProperName                   string                        `json:"properName,omitempty"`
	ShortName                    string                        `json:"shortName,omitempty"`
	TradeNames                   []string                      `json:"tradeNames,omitempty"`
	FormerNames                  []string                      `json:"formerNames,omitempty"`
	CountryCode                  string                        `json:"countryCode,omitempty"`
	CountryOfRisk                string                        `json:"countryOfRisk,omitempty"`
	CountryOfIncorporation       string                        `json:"countryOfIncorporation,omitempty"`
	CountryOfOperations          string                        `json:"countryOfOperations,omitempty"`
	CountryOfRiskUUID            string                        `json:"countryOfRiskUUID,omitempty"`
	CountryOfIncorporationUUID   string                        `json:"countryOfIncorporationUUID,omitempty"`
	CountryOfOperationsUUID      string                        `json:"countryOfOperationsUUID,omitempty"`
	PostalCode                   string                        `json:"postalCode,omitempty"`
	YearFounded                  int                           `json:"yearFounded,omitempty"`
	LeiCode                      string                        `json:"leiCode,omitempty"`
	ParentOrganisation           string                        `json:"parentOrganisation,omitempty"`
	NAICSIndustryClassifications []NAICSIndustryClassification `json:"naicsIndustryClassifications,omitempty"`
	IsDeprecated                 bool                          `json:"isDeprecated,omitempty"`
	// Location
	ISO31661 string `json:"iso31661,omitempty"`
	// Person
	Salutation string `json:"salutation,omitempty"`
	BirthYear  int    `json:"birthYear,omitempty"`
	// Industry Classifications
	IndustryIdentifier string `json:"industryIdentifier,omitempty"`
}

type MembershipRole struct {
	RoleUUID             string `json:"membershipRoleUUID,omitempty"`
	InceptionDate        string `json:"inceptionDate,omitempty"`
	TerminationDate      string `json:"terminationDate,omitempty"`
	InceptionDateEpoch   int64  `json:"inceptionDateEpoch,omitempty"`
	TerminationDateEpoch int64  `json:"terminationDateEpoch,omitempty"`
}

// NAICSIndustryClassification represents a pair of uuid of industry classification concept and the rank
// of that industry classification for a particular organisation
type NAICSIndustryClassification struct {
	UUID string `json:"uuid,omitempty"`
	Rank int    `json:"rank,omitempty"`
}
