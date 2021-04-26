package transform

import (
	"encoding/json"
	"time"

	"github.com/Financial-Times/concepts-rw-neo4j/ontology"
)

// TransformToNewSourceConcept creates the new source concept from the old one
// It uses json Marshal/Unmarshal to transform the old concept to generic map[string]interface{}
// Then it relays on the ontology definitions to deduce which field is a property and which is a relationship
func TransformToNewSourceConcept(c SourceConcept) ontology.NewSourceConcept {
	data, _ := json.Marshal(c)
	var store map[string]interface{}
	_ = json.Unmarshal(data, &store)

	concept := ontology.NewSourceConcept{
		GenericConcept: ontology.GenericConcept{
			Properties: map[string]interface{}{
				ontology.InceptionDateProp:        c.InceptionDate,
				ontology.TerminationDateProp:      c.TerminationDate,
				ontology.InceptionDateEpochProp:   TransformDateToUnix(c.InceptionDate),
				ontology.TerminationDateEpochProp: TransformDateToUnix(c.TerminationDate),
			},
			Relations: []ontology.Relationship{
				NAICSToRelationship(c.NAICSIndustryClassifications),
				MembershipRolesToRelationship(c.MembershipRoles),
			},
		},
		UUID:              c.UUID,
		Type:              c.Type,
		LastModifiedEpoch: c.LastModifiedEpoch,
		Hash:              c.Hash,
		IssuedBy:          c.IssuedBy,
	}
	propertySetup := ontology.GetPropertySetup()
	for prop, setup := range propertySetup {
		val, has := store[setup.ConceptField]
		if has {
			concept.Properties[prop] = val
		}
	}
	relationshipMapping := ontology.GetRelationships()
	for rel, setup := range relationshipMapping {
		if setup.SpecialField {
			continue
		}
		val, has := store[setup.ConceptField]
		if !has {
			continue
		}
		var uuids []string
		if setup.SingleField {
			uuids = append(uuids, val.(string))
		} else {
			for _, v := range val.([]interface{}) {
				uuids = append(uuids, v.(string))
			}
		}
		concept.Relations = append(concept.Relations, UUIDsToRelationships(rel, uuids))
	}
	return concept
}

// TransformToOldSourceConcept uses the same methodology as the other direction
// It relay on the ontology definition to describe how to map the fields from properties and relations
func TransformToOldSourceConcept(c ontology.NewSourceConcept) SourceConcept {
	inceptionDate, _ := c.GetPropString(ontology.InceptionDateProp)
	terminationDate, _ := c.GetPropString(ontology.TerminationDateProp)
	store := map[string]interface{}{
		"uuid":              c.UUID,
		"type":              c.Type,
		"hash":              c.Hash,
		"lastModifiedEpoch": c.LastModifiedEpoch,

		// special
		"inceptionDate":        inceptionDate,
		"terminationDate":      terminationDate,
		"inceptionDateEpoch":   TransformDateToUnix(inceptionDate),
		"terminationDateEpoch": TransformDateToUnix(terminationDate),
		"issuedBy":             c.IssuedBy,
	}
	propertySetup := ontology.GetPropertySetup()
	for prop, setup := range propertySetup {
		val, has := c.GetProp(prop)
		if has {
			store[setup.ConceptField] = val
		}
	}
	relationshipMapping := ontology.GetRelationships()
	store[relationshipMapping[ontology.IndustryClassificationRelation].ConceptField] = RelationshipsToNAICS(c.Relations)
	store[relationshipMapping[ontology.HasMembershipRoleRelation].ConceptField] = RelationshipsToMembershipRoles(c.Relations)

	for rel, setup := range relationshipMapping {
		if setup.SpecialField {
			continue
		}
		var val interface{}
		if setup.SingleField {
			val = RelationshipsToSingleUUID(c.Relations, rel)
		} else {
			val = RelationshipsToUUIDs(c.Relations, rel)
		}
		store[setup.ConceptField] = val
	}

	data, _ := json.Marshal(store)
	var concept SourceConcept
	_ = json.Unmarshal(data, &concept)

	return concept
}

// TransformToNewAggregateConcept is different than the source transformation
// It doesn't use the ontology definition. It explicitly defines how the old fields should be mapped.
func TransformToNewAggregateConcept(c AggregatedConcept) ontology.NewAggregatedConcept {
	var sources []ontology.NewSourceConcept
	for _, s := range c.SourceRepresentations {
		sources = append(sources, TransformToNewSourceConcept(s))
	}
	// this code does nothing, it is here just to remind me for properties
	// that have representation in the old format but not the new one
	for _, s := range c.SourceRepresentations {
		if s.OrganisationUUID == c.OrganisationUUID {
			continue
		}
		if s.PersonUUID == c.PersonUUID {
			continue
		}
		if len(c.MembershipRoles) != 0 {
			continue
		}
	}

	concept := ontology.NewAggregatedConcept{
		GenericConcept: ontology.GenericConcept{
			Properties: map[string]interface{}{
				ontology.PrefLabelProp:              c.PrefLabel,
				ontology.AliasesProp:                c.Aliases,
				ontology.StraplineProp:              c.Strapline,
				ontology.DescriptionProp:            c.DescriptionXML,
				ontology.ImageURLProp:               c.ImageURL,
				ontology.EmailAddressProp:           c.EmailAddress,
				ontology.FacebookPageProp:           c.FacebookPage,
				ontology.TwitterHandleProp:          c.TwitterHandle,
				ontology.ScopeNoteProp:              c.ScopeNote,
				ontology.ShortLabelProp:             c.ShortLabel,
				ontology.FigiCodeProp:               c.FigiCode,
				ontology.ProperNameProp:             c.ProperName,
				ontology.ShortNameProp:              c.ShortName,
				ontology.TradeNamesProp:             c.TradeNames,
				ontology.FormerNamesProp:            c.FormerNames,
				ontology.CountryCodeProp:            c.CountryCode,
				ontology.CountryOfRiskProp:          c.CountryOfRisk,
				ontology.CountryOfIncorporationProp: c.CountryOfIncorporation,
				ontology.CountryOfOperationsProp:    c.CountryOfOperations,
				ontology.PostalCodeProp:             c.PostalCode,
				ontology.YearFoundedProp:            c.YearFounded,
				ontology.LeiCodeProp:                c.LeiCode,
				ontology.IsDeprecatedProp:           c.IsDeprecated,
				ontology.ISO31661Prop:               c.ISO31661,
				ontology.SalutationProp:             c.Salutation,
				ontology.BirthYearProp:              c.BirthYear,
				ontology.IndustryIdentifierProp:     c.IndustryIdentifier,

				ontology.InceptionDateProp:   c.InceptionDate,
				ontology.TerminationDateProp: c.TerminationDate,
			},
		},
		PrefUUID:              c.PrefUUID,
		Type:                  c.Type,
		AggregatedHash:        c.AggregatedHash,
		SourceRepresentations: sources,
		IssuedBy:              c.IssuedBy,
	}

	// setup
	// this code needs to be performed before serialising the concept
	concept.Properties[ontology.InceptionDateEpochProp] = TransformDateToUnix(c.InceptionDate)
	concept.Properties[ontology.TerminationDateEpochProp] = TransformDateToUnix(c.TerminationDate)
	return concept
}

// TransformToOldAggregateConcept same as in the other direction
// It doesn't use the ontology definition. It explicitly defines how the old fields should be mapped.
func TransformToOldAggregateConcept(c ontology.NewAggregatedConcept) AggregatedConcept {
	var sources []SourceConcept
	for _, s := range c.SourceRepresentations {
		sources = append(sources, TransformToOldSourceConcept(s))
	}
	prefLabel, _ := c.GetPropString(ontology.PrefLabelProp)
	aliases, _ := c.GetPropStringSlice(ontology.AliasesProp)
	strapline, _ := c.GetPropString(ontology.StraplineProp)
	description, _ := c.GetPropString(ontology.DescriptionProp)
	imageURL, _ := c.GetPropString(ontology.ImageURLProp)
	email, _ := c.GetPropString(ontology.EmailAddressProp)
	facebookPage, _ := c.GetPropString(ontology.FacebookPageProp)
	twitter, _ := c.GetPropString(ontology.TwitterHandleProp)
	scopeNote, _ := c.GetPropString(ontology.ScopeNoteProp)
	shortLabel, _ := c.GetPropString(ontology.ShortLabelProp)
	figiCode, _ := c.GetPropString(ontology.FigiCodeProp)
	// organisation
	properName, _ := c.GetPropString(ontology.ProperNameProp)
	shortName, _ := c.GetPropString(ontology.ShortNameProp)
	tradeNames, _ := c.GetPropStringSlice(ontology.TradeNamesProp)
	formerNames, _ := c.GetPropStringSlice(ontology.FormerNamesProp)
	countryCode, _ := c.GetPropString(ontology.CountryCodeProp)
	countryOfRisk, _ := c.GetPropString(ontology.CountryOfRiskProp)
	countryOfOperations, _ := c.GetPropString(ontology.CountryOfOperationsProp)
	countryOfIncorporation, _ := c.GetPropString(ontology.CountryOfIncorporationProp)
	postalCode, _ := c.GetPropString(ontology.PostalCodeProp)
	yearFounded, _ := c.GetPropInt(ontology.YearFoundedProp)
	leiCode, _ := c.GetPropString(ontology.LeiCodeProp)
	deprecated, _ := c.GetPropBool(ontology.IsDeprecatedProp)
	iso31661, _ := c.GetPropString(ontology.ISO31661Prop)
	salutation, _ := c.GetPropString(ontology.SalutationProp)
	birthYear, _ := c.GetPropInt(ontology.BirthYearProp)
	industryIdentifier, _ := c.GetPropString(ontology.IndustryIdentifierProp)

	inceptionDate, _ := c.GetPropString(ontology.InceptionDateProp)
	terminationDate, _ := c.GetPropString(ontology.TerminationDateProp)

	orgUUID := ""
	for _, s := range c.SourceRepresentations {
		if !s.HasRelationships(ontology.HasOrganisationRelation) {
			continue
		}
		orgRels := s.GetRelationships(ontology.HasOrganisationRelation)
		if len(orgRels) != 0 && len(orgRels[0].Connections) != 0 {
			orgUUID = orgRels[0].Connections[0].UUID
		}
	}

	personUUID := ""
	for _, s := range c.SourceRepresentations {
		if !s.HasRelationships(ontology.HasMemberRelation) {
			continue
		}
		rels := s.GetRelationships(ontology.HasMemberRelation)
		if len(rels) != 0 && len(rels[0].Connections) != 0 {
			personUUID = rels[0].Connections[0].UUID
		}
	}
	var roles []MembershipRole
	for _, s := range c.SourceRepresentations {
		if !s.HasRelationships(ontology.HasMembershipRoleRelation) {
			continue
		}
		roles = append(roles, RelationshipsToMembershipRoles(s.Relations)...)
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
		PersonUUID:             personUUID,
		AggregatedHash:         c.AggregatedHash,
		SourceRepresentations:  sources,
		MembershipRoles:        roles,
		InceptionDate:          inceptionDate,
		TerminationDate:        terminationDate,
		InceptionDateEpoch:     TransformDateToUnix(inceptionDate),
		TerminationDateEpoch:   TransformDateToUnix(terminationDate),
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

const iso8601DateOnly = "2006-01-02"

func TransformDateToUnix(t string) int64 {
	if t == "" {
		return 0
	}
	tt, _ := time.Parse(iso8601DateOnly, t)
	return tt.Unix()
}
