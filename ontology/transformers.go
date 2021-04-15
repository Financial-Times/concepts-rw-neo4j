package ontology

import (
	"encoding/json"
	"time"
)

func TransformToRelationships(label string, uuids []string) Relationship {
	var connections []Connection
	for _, uuid := range uuids {
		if uuid == "" {
			continue
		}
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
		if n.UUID == "" {
			continue
		}
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

const (
	inceptionDateField        = "inceptionDate"
	inceptionDateEpochField   = "inceptionDateEpoch"
	terminationDateField      = "terminationDate"
	terminationDateEpochField = "terminationDateEpoch"
)

func TransformMembershipRoleToRelationship(roles []MembershipRole) Relationship {
	var connections []Connection
	for _, r := range roles {
		var (
			inceptionDateEpoch   int64
			terminationDateEpoch int64
		)

		if r.InceptionDate != "" {
			inceptionDateEpoch = TransformDateToUnix(r.InceptionDate)
		}
		if r.TerminationDate != "" {
			terminationDateEpoch = TransformDateToUnix(r.TerminationDate)
		}
		connections = append(connections, Connection{
			UUID: r.RoleUUID,
			Properties: map[string]interface{}{
				inceptionDateField:        r.InceptionDate,
				inceptionDateEpochField:   inceptionDateEpoch,
				terminationDateField:      r.TerminationDate,
				terminationDateEpochField: terminationDateEpoch,
			},
		})
	}
	return Relationship{
		Label:       HasMembershipRoleRelation,
		Connections: connections,
	}
}

func TransformRelationshipToMembershipRole(relations []Relationship) []MembershipRole {
	var roles []MembershipRole
	for _, rel := range relations {
		if rel.Label != HasMembershipRoleRelation {
			continue
		}
		for _, con := range rel.Connections {
			if con.UUID == "" {
				continue
			}

			inceptionDate, _ := con.GetPropString(inceptionDateField)
			terminationDate, _ := con.GetPropString(terminationDateField)
			roles = append(roles, MembershipRole{
				RoleUUID:        con.UUID,
				InceptionDate:   inceptionDate,
				TerminationDate: terminationDate,
			})
		}

	}
	return roles
}

var propertyLabelToConceptField = map[string]string{
	PrefLabelProp:              "prefLabel",
	AuthorityProp:              "authority",
	AuthorityValueProp:         "authorityValue",
	AliasesProp:                "aliases",
	StraplineProp:              "strapline",
	DescriptionProp:            "descriptionXML",
	ImageURLProp:               "_imageUrl",
	EmailAddressProp:           "emailAddress",
	FacebookPageProp:           "facebookPage",
	TwitterHandleProp:          "twitterHandle",
	ScopeNoteProp:              "scopeNote",
	ShortLabelProp:             "shortLabel",
	InceptionDateProp:          "inceptionDate",
	TerminationDateProp:        "terminationDate",
	InceptionDateEpochProp:     "inceptionDateEpoch",
	TerminationDateEpochProp:   "terminationDateEpoch",
	FigiCodeProp:               "figiCode",
	ProperNameProp:             "properName",
	ShortNameProp:              "shortName",
	TradeNamesProp:             "tradeNames",
	FormerNamesProp:            "formerNames",
	CountryCodeProp:            "countryCode",
	CountryOfRiskProp:          "countryOfRisk",
	CountryOfIncorporationProp: "countryOfIncorporation",
	CountryOfOperationsProp:    "countryOfOperations",
	PostalCodeProp:             "postalCode",
	YearFoundedProp:            "yearFounded",
	LeiCodeProp:                "leiCode",
	IsDeprecatedProp:           "isDeprecated",
	ISO31661Prop:               "iso31661",
	SalutationProp:             "salutation",
	BirthYearProp:              "birthYear",
	IndustryIdentifierProp:     "industryIdentifier",
}

func TransformToNewSourceConcept(c SourceConcept) NewSourceConcept {
	data, _ := json.Marshal(c)
	var store map[string]interface{}
	_ = json.Unmarshal(data, &store)

	concept := NewSourceConcept{
		GenericConcept: GenericConcept{
			Properties: map[string]interface{}{
				InceptionDateProp:        c.InceptionDate,
				TerminationDateProp:      c.TerminationDate,
				InceptionDateEpochProp:   TransformDateToUnix(c.InceptionDate),
				TerminationDateEpochProp: TransformDateToUnix(c.TerminationDate),
			},
			Relations: []Relationship{
				TransformNAICSToRelationship(c.NAICSIndustryClassifications),
				TransformMembershipRoleToRelationship(c.MembershipRoles),
			},
		},
		UUID:              c.UUID,
		Type:              c.Type,
		LastModifiedEpoch: c.LastModifiedEpoch,
		Hash:              c.Hash,
		IssuedBy:          c.IssuedBy,
	}

	for prop, label := range propertyLabelToConceptField {
		val, has := store[label]
		if has {
			concept.Properties[prop] = val
		}
	}
	relationshipMapping := GetRelationships()
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
		concept.Relations = append(concept.Relations, TransformToRelationships(rel, uuids))
	}
	return concept
}

func TransformToOldSourceConcept(c NewSourceConcept) SourceConcept {
	inceptionDate, _ := c.GetPropString(InceptionDateProp)
	terminationDate, _ := c.GetPropString(TerminationDateProp)
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
	for prop, label := range propertyLabelToConceptField {
		val, has := c.GetProp(prop)
		if has {
			store[label] = val
		}
	}
	relationshipMapping := GetRelationships()
	store[relationshipMapping[IndustryClassificationRelation].ConceptField] = TransformRelationshipToNAICS(c.Relations)
	store[relationshipMapping[HasMembershipRoleRelation].ConceptField] = TransformRelationshipToMembershipRole(c.Relations)

	for rel, setup := range relationshipMapping {
		if setup.SpecialField {
			continue
		}
		var val interface{}
		if setup.SingleField {
			val = TransformFromRelationshipsSingle(c.Relations, rel)
		} else {
			val = TransformFromRelationships(c.Relations, rel)
		}
		store[setup.ConceptField] = val
	}

	data, _ := json.Marshal(store)
	var concept SourceConcept
	_ = json.Unmarshal(data, &concept)

	return concept
}

func TransformToNewAggregateConcept(c AggregatedConcept) NewAggregatedConcept {
	var sources []NewSourceConcept
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

				InceptionDateProp:   c.InceptionDate,
				TerminationDateProp: c.TerminationDate,
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
	concept.Properties[InceptionDateEpochProp] = TransformDateToUnix(c.InceptionDate)
	concept.Properties[TerminationDateEpochProp] = TransformDateToUnix(c.TerminationDate)
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

	inceptionDate, _ := c.GetPropString(InceptionDateProp)
	terminationDate, _ := c.GetPropString(TerminationDateProp)

	orgUUID := ""
	for _, s := range c.SourceRepresentations {
		if !s.HasRelationships(HasOrganisationRelation) {
			continue
		}
		orgRels := s.GetRelationships(HasOrganisationRelation)
		if len(orgRels) != 0 && len(orgRels[0].Connections) != 0 {
			orgUUID = orgRels[0].Connections[0].UUID
		}
	}

	personUUID := ""
	for _, s := range c.SourceRepresentations {
		if !s.HasRelationships(HasMemberRelation) {
			continue
		}
		rels := s.GetRelationships(HasMemberRelation)
		if len(rels) != 0 && len(rels[0].Connections) != 0 {
			personUUID = rels[0].Connections[0].UUID
		}
	}
	var roles []MembershipRole
	for _, s := range c.SourceRepresentations {
		if !s.HasRelationships(HasMembershipRoleRelation) {
			continue
		}
		roles = append(roles, TransformRelationshipToMembershipRole(s.Relations)...)
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
