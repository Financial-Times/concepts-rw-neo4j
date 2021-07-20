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
		MembershipRoles:        old.MembershipRoles,
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
	for _, s := range new.SourceRepresentations {
		oldSources = append(oldSources, TransformToOldSourceConcept(s))
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
	return NewConcept{
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
	return Concept{
		UUID:                         new.UUID,
		PrefLabel:                    new.PrefLabel,
		Type:                         new.Type,
		Authority:                    new.Authority,
		AuthorityValue:               new.AuthorityValue,
		LastModifiedEpoch:            new.LastModifiedEpoch,
		Aliases:                      new.Aliases,
		ParentUUIDs:                  new.ParentUUIDs,
		Strapline:                    new.Strapline,
		DescriptionXML:               new.DescriptionXML,
		ImageURL:                     new.ImageURL,
		EmailAddress:                 new.EmailAddress,
		FacebookPage:                 new.FacebookPage,
		TwitterHandle:                new.TwitterHandle,
		ScopeNote:                    new.ScopeNote,
		ShortLabel:                   new.ShortLabel,
		BroaderUUIDs:                 new.BroaderUUIDs,
		RelatedUUIDs:                 new.RelatedUUIDs,
		SupersededByUUIDs:            new.SupersededByUUIDs,
		ImpliedByUUIDs:               new.ImpliedByUUIDs,
		HasFocusUUIDs:                new.HasFocusUUIDs,
		OrganisationUUID:             new.OrganisationUUID,
		PersonUUID:                   new.PersonUUID,
		Hash:                         new.Hash,
		MembershipRoles:              new.MembershipRoles,
		InceptionDate:                new.InceptionDate,
		TerminationDate:              new.TerminationDate,
		InceptionDateEpoch:           new.InceptionDateEpoch,
		TerminationDateEpoch:         new.TerminationDateEpoch,
		FigiCode:                     new.FigiCode,
		IssuedBy:                     new.IssuedBy,
		ProperName:                   new.ProperName,
		ShortName:                    new.ShortName,
		TradeNames:                   new.TradeNames,
		FormerNames:                  new.FormerNames,
		CountryCode:                  new.CountryCode,
		CountryOfRisk:                new.CountryOfRisk,
		CountryOfIncorporation:       new.CountryOfIncorporation,
		CountryOfOperations:          new.CountryOfOperations,
		CountryOfRiskUUID:            new.CountryOfRiskUUID,
		CountryOfIncorporationUUID:   new.CountryOfIncorporationUUID,
		CountryOfOperationsUUID:      new.CountryOfOperationsUUID,
		PostalCode:                   new.PostalCode,
		YearFounded:                  new.YearFounded,
		LeiCode:                      new.LeiCode,
		ParentOrganisation:           new.ParentOrganisation,
		NAICSIndustryClassifications: new.NAICSIndustryClassifications,
		IsDeprecated:                 new.IsDeprecated,
		ISO31661:                     new.ISO31661,
		Salutation:                   new.Salutation,
		BirthYear:                    new.BirthYear,
		IndustryIdentifier:           new.IndustryIdentifier,
	}
}
