package ontology

import (
	"encoding/json"
	"fmt"
	"sort"
)

func TransformToNewAggregateConcept(old AggregatedConcept) (NewAggregatedConcept, error) {
	var newSources []NewConcept
	for _, s := range old.SourceRepresentations {
		src, err := TransformToNewSourceConcept(s)
		if err != nil {
			return NewAggregatedConcept{}, err
		}
		newSources = append(newSources, src)
	}

	oldMap := map[string]interface{}{}
	oldBytes, _ := json.Marshal(old)
	if err := json.Unmarshal(oldBytes, &oldMap); err != nil {
		return NewAggregatedConcept{}, err
	}

	props := map[string]interface{}{}
	for field, cfg := range GetConfig().Fields {
		var v interface{}
		val, ok := oldMap[field]
		if !ok {
			continue
		}
		fieldType := cfg.FieldType
		switch fieldType {
		case "string":
			if v, ok = toString(val); !ok {
				return NewAggregatedConcept{}, getInvalidPropValueError(field, v)
			}
		case "[]string":
			if v, ok = toStringSlice(val); !ok {
				return NewAggregatedConcept{}, getInvalidPropValueError(field, v)
			}
		case "int":
			if v, ok = toInt(val); !ok {
				return NewAggregatedConcept{}, getInvalidPropValueError(field, v)
			}
		default:
			return NewAggregatedConcept{},
				fmt.Errorf("unsupported field type '%s' for prop '%s': %w", fieldType, field, ErrUnknownProperty)
		}
		props[field] = v
	}

	return NewAggregatedConcept{
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
		InceptionDateEpoch:    old.InceptionDateEpoch,
		TerminationDateEpoch:  old.TerminationDateEpoch,
		FigiCode:              old.FigiCode,
		IssuedBy:              old.IssuedBy,
		SourceRepresentations: newSources,
	}, nil
}

func TransformToOldAggregateConcept(new NewAggregatedConcept) (AggregatedConcept, error) {
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
	old.InceptionDateEpoch = new.InceptionDateEpoch
	old.TerminationDateEpoch = new.TerminationDateEpoch
	old.FigiCode = new.FigiCode
	old.IssuedBy = new.IssuedBy
	old.IsDeprecated = new.IsDeprecated
	old.SourceRepresentations = oldSources

	return old, nil
}

// nolint: gocognit // TODO: simplify this function
func TransformToNewSourceConcept(old Concept) (NewConcept, error) {
	oldMap := map[string]interface{}{}
	oldBytes, _ := json.Marshal(old)
	if err := json.Unmarshal(oldBytes, &oldMap); err != nil {
		return NewConcept{}, err
	}

	rels := []Relationship{}
	for rel, relCfg := range GetConfig().Relationships {
		if _, ok := oldMap[relCfg.ConceptField]; !ok {
			continue
		}

		val := oldMap[relCfg.ConceptField]

		if relCfg.OneToOne {
			uuid := val.(string)
			rels = append(rels, Relationship{UUID: uuid, Label: rel})
		} else {
			for _, v := range val.([]interface{}) {
				if len(relCfg.Properties) > 0 {
					relMap := v.(map[string]interface{})
					uuid, ok := relMap["uuid"]
					if ok {
						delete(relMap, "uuid")

						rels = append(rels, Relationship{UUID: uuid.(string), Label: rel, Properties: relMap})
						continue
					}

					// Handle membership roles as special case
					uuid, ok = relMap["membershipRoleUUID"]
					if !ok {
						continue
					}

					delete(relMap, "membershipRoleUUID")

					rels = append(rels, Relationship{UUID: uuid.(string), Label: rel, Properties: relMap})
				} else {
					uuid := v.(string)
					rels = append(rels, Relationship{UUID: uuid, Label: rel})
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

	return NewConcept{
		Relationships:                rels,
		UUID:                         old.UUID,
		PrefLabel:                    old.PrefLabel,
		Type:                         old.Type,
		Authority:                    old.Authority,
		AuthorityValue:               old.AuthorityValue,
		LastModifiedEpoch:            old.LastModifiedEpoch,
		Hash:                         old.Hash,
		MembershipRoles:              old.MembershipRoles,
		InceptionDate:                old.InceptionDate,
		TerminationDate:              old.TerminationDate,
		InceptionDateEpoch:           old.InceptionDateEpoch,
		TerminationDateEpoch:         old.TerminationDateEpoch,
		FigiCode:                     old.FigiCode,
		IssuedBy:                     old.IssuedBy,
		NAICSIndustryClassifications: old.NAICSIndustryClassifications,
		IsDeprecated:                 old.IsDeprecated,
	}, nil
}

// nolint: gocognit // TODO: simplify this function
func TransformToOldSourceConcept(new NewConcept) (Concept, error) {
	oldMap := map[string]interface{}{}
	for _, rel := range new.Relationships {
		if rel.UUID == "" {
			continue
		}

		if _, ok := GetConfig().Relationships[rel.Label]; !ok {
			continue
		}

		relCfg := GetConfig().Relationships[rel.Label]
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
	old.MembershipRoles = new.MembershipRoles
	old.InceptionDate = new.InceptionDate
	old.TerminationDate = new.TerminationDate
	old.InceptionDateEpoch = new.InceptionDateEpoch
	old.TerminationDateEpoch = new.TerminationDateEpoch
	old.FigiCode = new.FigiCode
	old.IssuedBy = new.IssuedBy
	old.NAICSIndustryClassifications = new.NAICSIndustryClassifications
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
