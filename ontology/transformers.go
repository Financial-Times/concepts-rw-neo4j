package ontology

func TransformToRelationships(label string, uuids []string) Relationship {
	var connections []Connection
	for _, uuid := range uuids {
		connections = append(connections, Connection{
			UUID: uuid,
		})
	}
	return Relationship{
		Label:       label,
		Connections: connections,
	}
}

func TransformFromRelationships(relations []Relationship, label string) []string {
	for _, rel := range relations {
		if rel.Label != label {
			continue
		}

		var uuids []string
		for _, con := range rel.Connections {
			uuids = append(uuids, con.UUID)
		}
		return uuids
	}
	return nil
}

func TransformFromRelationshipsSingle(relations []Relationship, label string) string {
	for _, rel := range relations {
		if rel.Label != label {
			continue
		}
		if len(rel.Connections) == 0 {
			return ""
		}
		return rel.Connections[0].UUID
	}
	return ""
}

const naicsRankField = "rank"

func TransformNAICSToRelationship(naics []NAICSIndustryClassification) Relationship {
	var connections []Connection
	for _, n := range naics {
		connections = append(connections, Connection{
			UUID: n.UUID,
			Properties: map[string]interface{}{
				naicsRankField: n.Rank,
			},
		})
	}
	return Relationship{
		Label:       IndustryClassificationRelation,
		Connections: connections,
	}
}

func TransformRelationshipToNAICS(relations []Relationship) []NAICSIndustryClassification {
	var naics []NAICSIndustryClassification
	for _, rel := range relations {
		if rel.Label != IndustryClassificationRelation {
			continue
		}
		for _, con := range rel.Connections {
			rank := -1
			if val, ok := con.Properties[naicsRankField]; ok {
				r, ok := val.(int)
				if ok {
					rank = r
				}
			}
			naics = append(naics, NAICSIndustryClassification{
				UUID: con.UUID,
				Rank: rank,
			})
		}

	}
	return naics
}

func TransformToNewSourceConcept(c SourceConcept) NewSourceConcept {
	relations := []Relationship{}
	relations = append(relations, TransformToRelationships(BroaderRelation, c.BroaderUUIDs))
	relations = append(relations, TransformToRelationships(ParentRelation, c.ParentUUIDs))
	relations = append(relations, TransformToRelationships(ImpliedByRelation, c.ImpliedByUUIDs))
	relations = append(relations, TransformToRelationships(HasFocusRelation, c.HasFocusUUIDs))
	relations = append(relations, TransformToRelationships(IsRelatedRelation, c.RelatedUUIDs))
	relations = append(relations, TransformToRelationships(SupersededByRelation, c.SupersededByUUIDs))
	relations = append(relations, TransformToRelationships(CountryOfRiskRelation, []string{c.CountryOfRiskUUID}))
	relations = append(relations, TransformToRelationships(CountryOfIncorporationRelation, []string{c.CountryOfIncorporationUUID}))
	relations = append(relations, TransformToRelationships(CountryOfOperationsRelation, []string{c.CountryOfOperationsUUID}))
	relations = append(relations, TransformToRelationships(ParentOrganisationRelation, []string{c.ParentOrganisation}))
	relations = append(relations, TransformNAICSToRelationship(c.NAICSIndustryClassifications))
	concept := NewSourceConcept{
		GenericConcept: GenericConcept{
			Properties: map[string]interface{}{
				PrefLabelProp:              c.PrefLabel,
				AliasesProp:                c.Aliases,
				StraplineProp:              c.Strapline,
				DescriptionProp:            c.DescriptionXML,
				ImageURLProp:               c.ImageURL,
				EmailAddressProp:           c.EmailAddress,
				FacebookPageProp:           c.FacebookPage,
				TwitterHandleProp:          c.TwitterHandle,
				ScopeNoteProp:              c.ScopeNote,
				ShortLabelProp:             c.ShortLabel,
				FigiCodeProp:               c.FigiCode,
				ProperNameProp:             c.ProperName,
				ShortNameProp:              c.ShortName,
				TradeNamesProp:             c.TradeNames,
				FormerNamesProp:            c.FormerNames,
				CountryCodeProp:            c.CountryCode,
				CountryOfRiskProp:          c.CountryOfRisk,
				CountryOfIncorporationProp: c.CountryOfIncorporation,
				CountryOfOperationsProp:    c.CountryOfOperations,
				PostalCodeProp:             c.PostalCode,
				YearFoundedProp:            c.YearFounded,
				LeiCodeProp:                c.LeiCode,
				IsDeprecatedProp:           c.IsDeprecated,
				ISO31661Prop:               c.ISO31661,
				SalutationProp:             c.Salutation,
				BirthYearProp:              c.BirthYear,
				IndustryIdentifierProp:     c.IndustryIdentifier,
			},
			Relations: relations,
		},
		UUID:                 c.UUID,
		Type:                 c.Type,
		Authority:            c.Authority,
		AuthorityValue:       c.AuthorityValue,
		LastModifiedEpoch:    c.LastModifiedEpoch,
		OrganisationUUID:     c.OrganisationUUID,
		PersonUUID:           c.PersonUUID,
		Hash:                 c.Hash,
		MembershipRoles:      c.MembershipRoles,
		InceptionDate:        c.InceptionDate,
		TerminationDate:      c.TerminationDate,
		InceptionDateEpoch:   c.InceptionDateEpoch,
		TerminationDateEpoch: c.TerminationDateEpoch,
		IssuedBy:             c.IssuedBy,
	}
	return concept
}

func TransformToOldSourceConcept(c NewSourceConcept) SourceConcept {
	prefLabel, _ := c.GetPropString(PrefLabelProp)
	aliases, _ := c.GetPropStringSlice(AliasesProp)
	strapline, _ := c.GetPropString(StraplineProp)
	description, _ := c.GetPropString(DescriptionProp)
	imageURL, _ := c.GetPropString(ImageURLProp)
	email, _ := c.GetPropString(EmailAddressProp)
	facebookPage, _ := c.GetPropString(FacebookPageProp)
	twitter, _ := c.GetPropString(TwitterHandleProp)
	scopeNote, _ := c.GetPropString(ScopeNoteProp)
	shortLabel, _ := c.GetPropString(ShortLabelProp)
	figiCode, _ := c.GetPropString(FigiCodeProp)
	// organisation
	properName, _ := c.GetPropString(ProperNameProp)
	shortName, _ := c.GetPropString(ShortNameProp)
	tradeNames, _ := c.GetPropStringSlice(TradeNamesProp)
	formerNames, _ := c.GetPropStringSlice(FormerNamesProp)
	countryCode, _ := c.GetPropString(CountryCodeProp)
	countryOfRisk, _ := c.GetPropString(CountryOfRiskProp)
	countryOfOperations, _ := c.GetPropString(CountryOfOperationsProp)
	countryOfIncorporation, _ := c.GetPropString(CountryOfIncorporationProp)
	postalCode, _ := c.GetPropString(PostalCodeProp)
	yearFounded, _ := c.GetPropInt(YearFoundedProp)
	leiCode, _ := c.GetPropString(LeiCodeProp)
	deprecated, _ := c.GetPropBool(IsDeprecatedProp)
	iso31661, _ := c.GetPropString(ISO31661Prop)
	salutation, _ := c.GetPropString(SalutationProp)
	birthYear, _ := c.GetPropInt(BirthYearProp)
	industryIdentifier, _ := c.GetPropString(IndustryIdentifierProp)
	concept := SourceConcept{
		UUID:                         c.UUID,
		PrefLabel:                    prefLabel,
		Type:                         c.Type,
		Authority:                    c.Authority,
		AuthorityValue:               c.AuthorityValue,
		LastModifiedEpoch:            c.LastModifiedEpoch,
		Aliases:                      aliases,
		ParentUUIDs:                  TransformFromRelationships(c.Relations, ParentRelation),
		Strapline:                    strapline,
		DescriptionXML:               description,
		ImageURL:                     imageURL,
		EmailAddress:                 email,
		FacebookPage:                 facebookPage,
		TwitterHandle:                twitter,
		ScopeNote:                    scopeNote,
		ShortLabel:                   shortLabel,
		BroaderUUIDs:                 TransformFromRelationships(c.Relations, BroaderRelation),
		RelatedUUIDs:                 TransformFromRelationships(c.Relations, IsRelatedRelation),
		SupersededByUUIDs:            TransformFromRelationships(c.Relations, SupersededByRelation),
		ImpliedByUUIDs:               TransformFromRelationships(c.Relations, ImpliedByRelation),
		HasFocusUUIDs:                TransformFromRelationships(c.Relations, HasFocusRelation),
		OrganisationUUID:             c.OrganisationUUID,
		PersonUUID:                   c.PersonUUID,
		Hash:                         c.Hash,
		MembershipRoles:              c.MembershipRoles,
		InceptionDate:                c.InceptionDate,
		TerminationDate:              c.TerminationDate,
		InceptionDateEpoch:           c.InceptionDateEpoch,
		TerminationDateEpoch:         c.TerminationDateEpoch,
		FigiCode:                     figiCode,
		IssuedBy:                     c.IssuedBy,
		ProperName:                   properName,
		ShortName:                    shortName,
		TradeNames:                   tradeNames,
		FormerNames:                  formerNames,
		CountryCode:                  countryCode,
		CountryOfRisk:                countryOfRisk,
		CountryOfIncorporation:       countryOfIncorporation,
		CountryOfOperations:          countryOfOperations,
		CountryOfRiskUUID:            TransformFromRelationshipsSingle(c.Relations, CountryOfRiskRelation),
		CountryOfIncorporationUUID:   TransformFromRelationshipsSingle(c.Relations, CountryOfIncorporationRelation),
		CountryOfOperationsUUID:      TransformFromRelationshipsSingle(c.Relations, CountryOfOperationsRelation),
		PostalCode:                   postalCode,
		YearFounded:                  yearFounded,
		LeiCode:                      leiCode,
		ParentOrganisation:           TransformFromRelationshipsSingle(c.Relations, ParentOrganisationRelation),
		NAICSIndustryClassifications: TransformRelationshipToNAICS(c.Relations),
		IsDeprecated:                 deprecated,
		ISO31661:                     iso31661,
		Salutation:                   salutation,
		BirthYear:                    birthYear,
		IndustryIdentifier:           industryIdentifier,
	}
	return concept
}

func TransformToNewAggregateConcept(c AggregatedConcept) NewAggregatedConcept {
	var sources []NewSourceConcept
	for _, s := range c.SourceRepresentations {
		sources = append(sources, TransformToNewSourceConcept(s))
	}
	// fields missing from the new aggregate node, but present as source node
	orgUUIDFound := false
	for _, s := range c.SourceRepresentations {
		if s.OrganisationUUID == c.OrganisationUUID {
			orgUUIDFound = true
		}
	}
	if orgUUIDFound {

	}

	concept := NewAggregatedConcept{
		GenericConcept: GenericConcept{
			Properties: map[string]interface{}{
				PrefLabelProp:              c.PrefLabel,
				AliasesProp:                c.Aliases,
				StraplineProp:              c.Strapline,
				DescriptionProp:            c.DescriptionXML,
				ImageURLProp:               c.ImageURL,
				EmailAddressProp:           c.EmailAddress,
				FacebookPageProp:           c.FacebookPage,
				TwitterHandleProp:          c.TwitterHandle,
				ScopeNoteProp:              c.ScopeNote,
				ShortLabelProp:             c.ShortLabel,
				FigiCodeProp:               c.FigiCode,
				ProperNameProp:             c.ProperName,
				ShortNameProp:              c.ShortName,
				TradeNamesProp:             c.TradeNames,
				FormerNamesProp:            c.FormerNames,
				CountryCodeProp:            c.CountryCode,
				CountryOfRiskProp:          c.CountryOfRisk,
				CountryOfIncorporationProp: c.CountryOfIncorporation,
				CountryOfOperationsProp:    c.CountryOfOperations,
				PostalCodeProp:             c.PostalCode,
				YearFoundedProp:            c.YearFounded,
				LeiCodeProp:                c.LeiCode,
				IsDeprecatedProp:           c.IsDeprecated,
				ISO31661Prop:               c.ISO31661,
				SalutationProp:             c.Salutation,
				BirthYearProp:              c.BirthYear,
				IndustryIdentifierProp:     c.IndustryIdentifier,
			},
		},
		PrefUUID:              c.PrefUUID,
		Type:                  c.Type,
		PersonUUID:            c.PersonUUID,
		AggregatedHash:        c.AggregatedHash,
		SourceRepresentations: sources,
		MembershipRoles:       c.MembershipRoles,
		InceptionDate:         c.InceptionDate,
		TerminationDate:       c.TerminationDate,
		InceptionDateEpoch:    c.InceptionDateEpoch,
		TerminationDateEpoch:  c.TerminationDateEpoch,
		IssuedBy:              c.IssuedBy,
	}
	return concept
}

func TransformToOldAggregateConcept(c NewAggregatedConcept) AggregatedConcept {
	var sources []SourceConcept
	for _, s := range c.SourceRepresentations {
		sources = append(sources, TransformToOldSourceConcept(s))
	}
	prefLabel, _ := c.GetPropString(PrefLabelProp)
	aliases, _ := c.GetPropStringSlice(AliasesProp)
	strapline, _ := c.GetPropString(StraplineProp)
	description, _ := c.GetPropString(DescriptionProp)
	imageURL, _ := c.GetPropString(ImageURLProp)
	email, _ := c.GetPropString(EmailAddressProp)
	facebookPage, _ := c.GetPropString(FacebookPageProp)
	twitter, _ := c.GetPropString(TwitterHandleProp)
	scopeNote, _ := c.GetPropString(ScopeNoteProp)
	shortLabel, _ := c.GetPropString(ShortLabelProp)
	figiCode, _ := c.GetPropString(FigiCodeProp)
	// organisation
	properName, _ := c.GetPropString(ProperNameProp)
	shortName, _ := c.GetPropString(ShortNameProp)
	tradeNames, _ := c.GetPropStringSlice(TradeNamesProp)
	formerNames, _ := c.GetPropStringSlice(FormerNamesProp)
	countryCode, _ := c.GetPropString(CountryCodeProp)
	countryOfRisk, _ := c.GetPropString(CountryOfRiskProp)
	countryOfOperations, _ := c.GetPropString(CountryOfOperationsProp)
	countryOfIncorporation, _ := c.GetPropString(CountryOfIncorporationProp)
	postalCode, _ := c.GetPropString(PostalCodeProp)
	yearFounded, _ := c.GetPropInt(YearFoundedProp)
	leiCode, _ := c.GetPropString(LeiCodeProp)
	deprecated, _ := c.GetPropBool(IsDeprecatedProp)
	iso31661, _ := c.GetPropString(ISO31661Prop)
	salutation, _ := c.GetPropString(SalutationProp)
	birthYear, _ := c.GetPropInt(BirthYearProp)
	industryIdentifier, _ := c.GetPropString(IndustryIdentifierProp)

	orgUUID := ""
	for _, s := range c.SourceRepresentations {
		if s.OrganisationUUID != "" {
			orgUUID = s.OrganisationUUID
		}
	}
	concept := AggregatedConcept{
		PrefUUID:               c.PrefUUID,
		PrefLabel:              prefLabel,
		Type:                   c.Type,
		Aliases:                aliases,
		Strapline:              strapline,
		DescriptionXML:         description,
		ImageURL:               imageURL,
		EmailAddress:           email,
		FacebookPage:           facebookPage,
		TwitterHandle:          twitter,
		ScopeNote:              scopeNote,
		ShortLabel:             shortLabel,
		OrganisationUUID:       orgUUID,
		PersonUUID:             c.PersonUUID,
		AggregatedHash:         c.AggregatedHash,
		SourceRepresentations:  sources,
		MembershipRoles:        c.MembershipRoles,
		InceptionDate:          c.InceptionDate,
		TerminationDate:        c.TerminationDate,
		InceptionDateEpoch:     c.InceptionDateEpoch,
		TerminationDateEpoch:   c.TerminationDateEpoch,
		FigiCode:               figiCode,
		IssuedBy:               c.IssuedBy,
		ProperName:             properName,
		ShortName:              shortName,
		TradeNames:             tradeNames,
		FormerNames:            formerNames,
		CountryCode:            countryCode,
		CountryOfRisk:          countryOfRisk,
		CountryOfIncorporation: countryOfIncorporation,
		CountryOfOperations:    countryOfOperations,
		PostalCode:             postalCode,
		YearFounded:            yearFounded,
		LeiCode:                leiCode,
		IsDeprecated:           deprecated,
		ISO31661:               iso31661,
		Salutation:             salutation,
		BirthYear:              birthYear,
		IndustryIdentifier:     industryIdentifier,
	}
	return concept
}
