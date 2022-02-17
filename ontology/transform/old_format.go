package transform

import (
	"encoding/json"
	"fmt"
	"sort"

	"github.com/Financial-Times/concepts-rw-neo4j/ontology"
)

func ToNewAggregateConcept(old OldAggregatedConcept) (ontology.NewAggregatedConcept, error) {
	var newSources []ontology.NewConcept
	for _, s := range old.SourceRepresentations {
		src, err := ToNewSourceConcept(s)
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

func ToOldAggregateConcept(new ontology.NewAggregatedConcept) (OldAggregatedConcept, error) {
	var oldSources []OldConcept
	var roles []MembershipRole
	for _, s := range new.SourceRepresentations {
		oldSource, err := ToOldSourceConcept(s)
		if err != nil {
			return OldAggregatedConcept{}, err
		}

		for _, r := range oldSource.MembershipRoles {
			if r.RoleUUID == "" {
				continue
			}
			roles = append(roles, r)
		}

		oldSources = append(oldSources, oldSource)
	}

	old := OldAggregatedConcept{}
	newPropsBytes, err := json.Marshal(new.Properties)
	if err != nil {
		return OldAggregatedConcept{}, err
	}
	err = json.Unmarshal(newPropsBytes, &old)
	if err != nil {
		return OldAggregatedConcept{}, err
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
func ToNewSourceConcept(old OldConcept) (ontology.NewConcept, error) {
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
func ToOldSourceConcept(new ontology.NewConcept) (OldConcept, error) {
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

	old := OldConcept{}
	relMapBytes, _ := json.Marshal(oldMap)
	if err := json.Unmarshal(relMapBytes, &old); err != nil {
		return OldConcept{}, err
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
