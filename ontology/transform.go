package ontology

import "encoding/json"

func TransformToNewAggregateConcept(old AggregatedConcept) NewAggregatedConcept {
	var newSources []NewConcept
	for _, s := range old.SourceRepresentations {
		newSources = append(newSources, TransformToNewSourceConcept(s))
	}

	oldMap := map[string]interface{}{}
	oldBytes, _ := json.Marshal(old)
	_ = json.Unmarshal(oldBytes, &oldMap)

	props := map[string]interface{}{}
	for field := range GetConfig().Fields {
		if val, ok := oldMap[field]; ok {
			props[field] = val
		}
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
	}
}

func TransformToOldAggregateConcept(new NewAggregatedConcept) AggregatedConcept {
	var oldSources []Concept
	var roles []MembershipRole
	for _, s := range new.SourceRepresentations {
		oldSource := TransformToOldSourceConcept(s)

		for _, r := range oldSource.MembershipRoles {
			if r.RoleUUID == "" {
				continue
			}
			roles = append(roles, r)
		}

		oldSources = append(oldSources, oldSource)
	}

	old := AggregatedConcept{}
	newPropsBytes, _ := json.Marshal(new.Properties)
	_ = json.Unmarshal(newPropsBytes, &old)

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

	return old
}

func TransformToNewSourceConcept(old Concept) NewConcept {
	oldMap := map[string]interface{}{}
	oldBytes, _ := json.Marshal(old)
	_ = json.Unmarshal(oldBytes, &oldMap)

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
		CountryOfRiskUUID:            old.CountryOfRiskUUID,
		CountryOfIncorporationUUID:   old.CountryOfIncorporationUUID,
		CountryOfOperationsUUID:      old.CountryOfOperationsUUID,
		ParentOrganisation:           old.ParentOrganisation,
		NAICSIndustryClassifications: old.NAICSIndustryClassifications,
		IsDeprecated:                 old.IsDeprecated,
	}
}

func TransformToOldSourceConcept(new NewConcept) Concept {
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
	_ = json.Unmarshal(relMapBytes, &old)

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
	old.CountryOfRiskUUID = new.CountryOfRiskUUID
	old.CountryOfIncorporationUUID = new.CountryOfIncorporationUUID
	old.CountryOfOperationsUUID = new.CountryOfOperationsUUID
	old.ParentOrganisation = new.ParentOrganisation
	old.NAICSIndustryClassifications = new.NAICSIndustryClassifications
	old.IsDeprecated = new.IsDeprecated

	return old
}
