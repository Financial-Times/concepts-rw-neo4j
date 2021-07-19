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
	for field := range GetConfig().FieldToNeoProps {
		if val, ok := oldMap[field]; ok {
			props[field] = val
		}
	}

	return NewAggregatedConcept{
		Properties:             props,
		PrefUUID:               old.PrefUUID,
		PrefLabel:              old.PrefLabel,
		Type:                   old.Type,
		Aliases:                old.Aliases,
		Strapline:              old.Strapline,
		DescriptionXML:         old.DescriptionXML,
		ImageURL:               old.ImageURL,
		EmailAddress:           old.EmailAddress,
		FacebookPage:           old.FacebookPage,
		TwitterHandle:          old.TwitterHandle,
		ScopeNote:              old.ScopeNote,
		ShortLabel:             old.ShortLabel,
		OrganisationUUID:       old.OrganisationUUID,
		PersonUUID:             old.PersonUUID,
		AggregatedHash:         old.AggregatedHash,
		InceptionDate:          old.InceptionDate,
		TerminationDate:        old.TerminationDate,
		InceptionDateEpoch:     old.InceptionDateEpoch,
		TerminationDateEpoch:   old.TerminationDateEpoch,
		FigiCode:               old.FigiCode,
		IssuedBy:               old.IssuedBy,
		ProperName:             old.ProperName,
		ShortName:              old.ShortName,
		TradeNames:             old.TradeNames,
		FormerNames:            old.FormerNames,
		CountryCode:            old.CountryCode,
		CountryOfRisk:          old.CountryOfRisk,
		CountryOfIncorporation: old.CountryOfIncorporation,
		CountryOfOperations:    old.CountryOfOperations,
		PostalCode:             old.PostalCode,
		YearFounded:            old.YearFounded,
		LeiCode:                old.LeiCode,
		IsDeprecated:           old.IsDeprecated,
		ISO31661:               old.ISO31661,
		Salutation:             old.Salutation,
		BirthYear:              old.BirthYear,
		IndustryIdentifier:     old.IndustryIdentifier,
		SourceRepresentations:  newSources,
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
	old.Aliases = new.Aliases
	old.Strapline = new.Strapline
	old.DescriptionXML = new.DescriptionXML
	old.ImageURL = new.ImageURL
	old.EmailAddress = new.EmailAddress
	old.FacebookPage = new.FacebookPage
	old.TwitterHandle = new.TwitterHandle
	old.ScopeNote = new.ScopeNote
	old.ShortLabel = new.ShortLabel
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
	old.ProperName = new.ProperName
	old.ShortName = new.ShortName
	old.TradeNames = new.TradeNames
	old.FormerNames = new.FormerNames
	old.CountryCode = new.CountryCode
	old.CountryOfRisk = new.CountryOfRisk
	old.CountryOfIncorporation = new.CountryOfIncorporation
	old.CountryOfOperations = new.CountryOfOperations
	old.PostalCode = new.PostalCode
	old.YearFounded = new.YearFounded
	old.LeiCode = new.LeiCode
	old.IsDeprecated = new.IsDeprecated
	old.ISO31661 = new.ISO31661
	old.Salutation = new.Salutation
	old.BirthYear = new.BirthYear
	old.IndustryIdentifier = new.IndustryIdentifier
	old.SourceRepresentations = oldSources

	return old
}

func TransformToNewSourceConcept(old Concept) NewConcept {
	oldMap := map[string]interface{}{}
	oldBytes, _ := json.Marshal(old)
	_ = json.Unmarshal(oldBytes, &oldMap)

	rels := []Relationship{}
	for rel, relCfg := range Relationships {
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
		Aliases:                      old.Aliases,
		ParentUUIDs:                  old.ParentUUIDs,
		Strapline:                    old.Strapline,
		DescriptionXML:               old.DescriptionXML,
		ImageURL:                     old.ImageURL,
		EmailAddress:                 old.EmailAddress,
		FacebookPage:                 old.FacebookPage,
		TwitterHandle:                old.TwitterHandle,
		ScopeNote:                    old.ScopeNote,
		ShortLabel:                   old.ShortLabel,
		BroaderUUIDs:                 old.BroaderUUIDs,
		RelatedUUIDs:                 old.RelatedUUIDs,
		SupersededByUUIDs:            old.SupersededByUUIDs,
		ImpliedByUUIDs:               old.ImpliedByUUIDs,
		HasFocusUUIDs:                old.HasFocusUUIDs,
		OrganisationUUID:             old.OrganisationUUID,
		PersonUUID:                   old.PersonUUID,
		Hash:                         old.Hash,
		MembershipRoles:              old.MembershipRoles,
		InceptionDate:                old.InceptionDate,
		TerminationDate:              old.TerminationDate,
		InceptionDateEpoch:           old.InceptionDateEpoch,
		TerminationDateEpoch:         old.TerminationDateEpoch,
		FigiCode:                     old.FigiCode,
		IssuedBy:                     old.IssuedBy,
		ProperName:                   old.ProperName,
		ShortName:                    old.ShortName,
		TradeNames:                   old.TradeNames,
		FormerNames:                  old.FormerNames,
		CountryCode:                  old.CountryCode,
		CountryOfRisk:                old.CountryOfRisk,
		CountryOfIncorporation:       old.CountryOfIncorporation,
		CountryOfOperations:          old.CountryOfOperations,
		CountryOfRiskUUID:            old.CountryOfRiskUUID,
		CountryOfIncorporationUUID:   old.CountryOfIncorporationUUID,
		CountryOfOperationsUUID:      old.CountryOfOperationsUUID,
		PostalCode:                   old.PostalCode,
		YearFounded:                  old.YearFounded,
		LeiCode:                      old.LeiCode,
		ParentOrganisation:           old.ParentOrganisation,
		NAICSIndustryClassifications: old.NAICSIndustryClassifications,
		IsDeprecated:                 old.IsDeprecated,
		ISO31661:                     old.ISO31661,
		Salutation:                   old.Salutation,
		BirthYear:                    old.BirthYear,
		IndustryIdentifier:           old.IndustryIdentifier,
	}
}

func TransformToOldSourceConcept(new NewConcept) Concept {
	oldMap := map[string]interface{}{}
	for _, rel := range new.Relationships {
		if rel.UUID == "" {
			continue
		}

		if _, ok := Relationships[rel.Label]; !ok {
			continue
		}

		relCfg := Relationships[rel.Label]
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
	old.Aliases = new.Aliases
	old.ParentUUIDs = new.ParentUUIDs
	old.Strapline = new.Strapline
	old.DescriptionXML = new.DescriptionXML
	old.ImageURL = new.ImageURL
	old.EmailAddress = new.EmailAddress
	old.FacebookPage = new.FacebookPage
	old.TwitterHandle = new.TwitterHandle
	old.ScopeNote = new.ScopeNote
	old.ShortLabel = new.ShortLabel
	old.BroaderUUIDs = new.BroaderUUIDs
	old.RelatedUUIDs = new.RelatedUUIDs
	old.SupersededByUUIDs = new.SupersededByUUIDs
	old.ImpliedByUUIDs = new.ImpliedByUUIDs
	old.HasFocusUUIDs = new.HasFocusUUIDs
	old.OrganisationUUID = new.OrganisationUUID
	old.PersonUUID = new.PersonUUID
	old.Hash = new.Hash
	old.MembershipRoles = new.MembershipRoles
	old.InceptionDate = new.InceptionDate
	old.TerminationDate = new.TerminationDate
	old.InceptionDateEpoch = new.InceptionDateEpoch
	old.TerminationDateEpoch = new.TerminationDateEpoch
	old.FigiCode = new.FigiCode
	old.IssuedBy = new.IssuedBy
	old.ProperName = new.ProperName
	old.ShortName = new.ShortName
	old.TradeNames = new.TradeNames
	old.FormerNames = new.FormerNames
	old.CountryCode = new.CountryCode
	old.CountryOfRisk = new.CountryOfRisk
	old.CountryOfIncorporation = new.CountryOfIncorporation
	old.CountryOfOperations = new.CountryOfOperations
	old.CountryOfRiskUUID = new.CountryOfRiskUUID
	old.CountryOfIncorporationUUID = new.CountryOfIncorporationUUID
	old.CountryOfOperationsUUID = new.CountryOfOperationsUUID
	old.PostalCode = new.PostalCode
	old.YearFounded = new.YearFounded
	old.LeiCode = new.LeiCode
	old.ParentOrganisation = new.ParentOrganisation
	old.NAICSIndustryClassifications = new.NAICSIndustryClassifications
	old.IsDeprecated = new.IsDeprecated
	old.ISO31661 = new.ISO31661
	old.Salutation = new.Salutation
	old.BirthYear = new.BirthYear
	old.IndustryIdentifier = new.IndustryIdentifier

	return old
}
