package concepts

import (
	"github.com/Financial-Times/concepts-rw-neo4j/ontology"
	"github.com/Financial-Times/neo-model-utils-go/mapper"
	"github.com/jmcvetta/neoism"
)

// Map of all the possible node types so we can ensure they all have
// constraints on the uuid
var constraintMap = map[string]string{
	"Thing":                       "uuid",
	"Concept":                     "uuid",
	"Classification":              "uuid",
	"Section":                     "uuid",
	"Subject":                     "uuid",
	"SpecialReport":               "uuid",
	"Location":                    "uuid",
	"Topic":                       "uuid",
	"Genre":                       "uuid",
	"Brand":                       "uuid",
	"AlphavilleSeries":            "uuid",
	"PublicCompany":               "uuid",
	"Person":                      "uuid",
	"Organisation":                "uuid",
	"MembershipRole":              "uuid",
	"BoardRole":                   "uuid",
	"Membership":                  "uuid",
	"FinancialInstrument":         "uuid",
	"IndustryClassification":      "uuid",
	"NAICSIndustryClassification": "uuid",
}

type neoAggregatedConcept struct {
	AggregateHash         string                    `json:"aggregateHash,omitempty"`
	Aliases               []string                  `json:"aliases,omitempty"`
	Authority             string                    `json:"authority,omitempty"`
	AuthorityValue        string                    `json:"authorityValue,omitempty"`
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

func (c neoAggregatedConcept) ToAggregateConcept() (ontology.NewAggregatedConcept, error) {
	typeName, err := mapper.MostSpecificType(c.Types)
	if err != nil {
		return ontology.NewAggregatedConcept{}, err
	}

	var sourceConcepts []ontology.NewSourceConcept
	for _, srcConcept := range c.SourceRepresentations {
		source, err := srcConcept.ToSourceConcept()
		if err != nil {
			return ontology.NewAggregatedConcept{}, err
		}
		sourceConcepts = append(sourceConcepts, source)
	}

	return ontology.NewAggregatedConcept{
		GenericConcept: ontology.GenericConcept{
			Properties: map[string]interface{}{
				ontology.PrefLabelProp:     c.PrefLabel,
				ontology.AliasesProp:       c.Aliases,
				ontology.StraplineProp:     c.Strapline,
				ontology.DescriptionProp:   c.DescriptionXML,
				ontology.ImageURLProp:      c.ImageURL,
				ontology.EmailAddressProp:  c.EmailAddress,
				ontology.FacebookPageProp:  c.FacebookPage,
				ontology.FigiCodeProp:      c.FigiCode,
				ontology.ScopeNoteProp:     c.ScopeNote,
				ontology.ShortLabelProp:    c.ShortLabel,
				ontology.TwitterHandleProp: c.TwitterHandle,
				// Organisations
				ontology.ProperNameProp:             c.ProperName,
				ontology.ShortNameProp:              c.ShortName,
				ontology.TradeNamesProp:             c.TradeNames,
				ontology.FormerNamesProp:            c.FormerNames,
				ontology.CountryCodeProp:            c.CountryCode,
				ontology.CountryOfIncorporationProp: c.CountryOfIncorporation,
				ontology.CountryOfRiskProp:          c.CountryOfRisk,
				ontology.CountryOfOperationsProp:    c.CountryOfOperations,
				ontology.PostalCodeProp:             c.PostalCode,
				ontology.YearFoundedProp:            c.YearFounded,
				ontology.LeiCodeProp:                c.LeiCode,
				ontology.IsDeprecatedProp:           c.IsDeprecated,
				ontology.SalutationProp:             c.Salutation,
				ontology.BirthYearProp:              c.BirthYear,
				ontology.ISO31661Prop:               c.ISO31661,
				ontology.IndustryIdentifierProp:     c.IndustryIdentifier,
				ontology.InceptionDateProp:          c.InceptionDate,
				ontology.TerminationDateProp:        c.TerminationDate,
			},
		},
		AggregatedHash:        c.AggregateHash,
		IssuedBy:              c.IssuedBy,
		PrefUUID:              c.PrefUUID,
		SourceRepresentations: sourceConcepts,
		Type:                  typeName,
	}, nil
}

type neoConcept struct {
	Aliases              []string                  `json:"aliases,omitempty"`
	Authority            string                    `json:"authority,omitempty"`
	AuthorityValue       string                    `json:"authorityValue,omitempty"`
	BroaderUUIDs         []string                  `json:"broaderUUIDs,omitempty"`
	DescriptionXML       string                    `json:"descriptionXML,omitempty"`
	EmailAddress         string                    `json:"emailAddress,omitempty"`
	FacebookPage         string                    `json:"facebookPage,omitempty"`
	FigiCode             string                    `json:"figiCode,omitempty"`
	ImageURL             string                    `json:"imageUrl,omitempty"`
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
	ScopeNote            string                    `json:"scopeNote,omitempty"`
	ShortLabel           string                    `json:"shortLabel,omitempty"`
	Strapline            string                    `json:"strapline,omitempty"`
	TerminationDate      string                    `json:"terminationDate,omitempty"`
	TerminationDateEpoch int64                     `json:"terminationDateEpoch,omitempty"`
	TwitterHandle        string                    `json:"twitterHandle,omitempty"`
	Types                []string                  `json:"types,omitempty"`
	UUID                 string                    `json:"uuid,omitempty"`
	IsDeprecated         bool                      `json:"isDeprecated,omitempty"`
	// Organisations
	ProperName                   string                                 `json:"properName,omitempty"`
	ShortName                    string                                 `json:"shortName,omitempty"`
	TradeNames                   []string                               `json:"tradeNames,omitempty"`
	FormerNames                  []string                               `json:"formerNames,omitempty"`
	CountryCode                  string                                 `json:"countryCode,omitempty"`
	CountryOfRisk                string                                 `json:"countryOfRisk,omitempty"`
	CountryOfIncorporation       string                                 `json:"countryOfIncorporation,omitempty"`
	CountryOfOperations          string                                 `json:"countryOfOperations,omitempty"`
	CountryOfRiskUUID            string                                 `json:"countryOfRiskUUID,omitempty"`
	CountryOfIncorporationUUID   string                                 `json:"countryOfIncorporationUUID,omitempty"`
	CountryOfOperationsUUID      string                                 `json:"countryOfOperationsUUID,omitempty"`
	PostalCode                   string                                 `json:"postalCode,omitempty"`
	YearFounded                  int                                    `json:"yearFounded,omitempty"`
	LeiCode                      string                                 `json:"leiCode,omitempty"`
	ParentOrganisation           string                                 `json:"parentOrganisation,omitempty"`
	NAICSIndustryClassifications []ontology.NAICSIndustryClassification `json:"naicsIndustryClassifications,omitempty"`
	// Location
	ISO31661 string `json:"iso31661,omitempty"`
	// Person
	Salutation string `json:"salutation,omitempty"`
	BirthYear  int    `json:"birthYear,omitempty"`
	// Industry Classifications
	IndustryIdentifier string `json:"industryIdentifier,omitempty"`
}

func (c neoConcept) ToSourceConcept() (ontology.NewSourceConcept, error) {
	conceptType, err := mapper.MostSpecificType(c.Types)
	if err != nil {
		return ontology.NewSourceConcept{}, err
	}
	var relations []ontology.Relationship
	relations = append(relations, ontology.TransformToRelationships(ontology.BroaderRelation, filterSlice(c.BroaderUUIDs)))
	relations = append(relations, ontology.TransformToRelationships(ontology.ParentRelation, filterSlice(c.ParentUUIDs)))
	relations = append(relations, ontology.TransformToRelationships(ontology.ImpliedByRelation, filterSlice(c.ImpliedByUUIDs)))
	relations = append(relations, ontology.TransformToRelationships(ontology.HasFocusRelation, filterSlice(c.HasFocusUUIDs)))
	relations = append(relations, ontology.TransformToRelationships(ontology.SupersededByRelation, filterSlice(c.SupersededByUUIDs)))
	relations = append(relations, ontology.TransformToRelationships(ontology.IsRelatedRelation, filterSlice(c.RelatedUUIDs)))
	relations = append(relations, ontology.TransformToRelationships(ontology.CountryOfRiskRelation, []string{c.CountryOfRiskUUID}))
	relations = append(relations, ontology.TransformToRelationships(ontology.CountryOfIncorporationRelation, []string{c.CountryOfIncorporationUUID}))
	relations = append(relations, ontology.TransformToRelationships(ontology.CountryOfOperationsRelation, []string{c.CountryOfOperationsUUID}))
	relations = append(relations, ontology.TransformToRelationships(ontology.ParentOrganisationRelation, []string{c.ParentOrganisation}))
	relations = append(relations, ontology.TransformNAICSToRelationship(c.NAICSIndustryClassifications))
	relations = append(relations, ontology.TransformToRelationships(ontology.HasOrganisationRelation, []string{c.OrganisationUUID}))
	relations = append(relations, ontology.TransformToRelationships(ontology.HasMemberRelation, []string{c.PersonUUID}))
	relations = append(relations, ontology.TransformMembershipRoleToRelationship(c.MembershipRoles))

	return ontology.NewSourceConcept{
		GenericConcept: ontology.GenericConcept{
			Properties: map[string]interface{}{
				ontology.PrefLabelProp:    c.PrefLabel,
				ontology.FigiCodeProp:     c.FigiCode,
				ontology.IsDeprecatedProp: c.IsDeprecated,
			},
			Relations: relations,
		},
		Authority:         c.Authority,
		AuthorityValue:    c.AuthorityValue,
		IssuedBy:          c.IssuedBy,
		LastModifiedEpoch: c.LastModifiedEpoch,
		Type:              conceptType,
		UUID:              c.UUID,
	}, nil
}

func getNeoConceptReadQuery(uuid string, results *[]neoAggregatedConcept) *neoism.CypherQuery {
	return &neoism.CypherQuery{
		Statement: `
			MATCH (canonical:Thing {prefUUID:{uuid}})<-[:EQUIVALENT_TO]-(source:Thing)
			OPTIONAL MATCH (source)-[:HAS_BROADER]->(broader:Thing)
			OPTIONAL MATCH (source)-[:HAS_MEMBER]->(person:Thing)
			OPTIONAL MATCH (source)-[:HAS_ORGANISATION]->(org:Thing)
			OPTIONAL MATCH (source)-[:HAS_PARENT]->(parent:Thing)
			OPTIONAL MATCH (source)-[:IS_RELATED_TO]->(related:Thing)
			OPTIONAL MATCH (source)-[:SUPERSEDED_BY]->(supersededBy:Thing)
			OPTIONAL MATCH (source)-[:IMPLIED_BY]->(impliedBy:Thing)
			OPTIONAL MATCH (source)-[:HAS_FOCUS]->(hasFocus:Thing)
			OPTIONAL MATCH (source)-[:ISSUED_BY]->(issuer:Thing)
			OPTIONAL MATCH (source)-[roleRel:HAS_ROLE]->(role:Thing)
			OPTIONAL MATCH (source)-[:SUB_ORGANISATION_OF]->(parentOrg:Thing)
			OPTIONAL MATCH (source)-[:COUNTRY_OF_OPERATIONS]->(coo:Thing)
			OPTIONAL MATCH (source)-[:COUNTRY_OF_RISK]->(cor:Thing)
			OPTIONAL MATCH (source)-[:COUNTRY_OF_INCORPORATION]->(coi:Thing)
			OPTIONAL MATCH (source)-[hasICRel:HAS_INDUSTRY_CLASSIFICATION]->(naics:NAICSIndustryClassification) 
			WITH
				collect(DISTINCT broader.uuid) as broaderUUIDs,
				canonical,
				issuer,
				org,
				parent,
				person,
				collect(DISTINCT related.uuid) as relatedUUIDs,
				collect(DISTINCT supersededBy.uuid) as supersededByUUIDs,
				collect(DISTINCT impliedBy.uuid) as impliedByUUIDs,
				collect(DISTINCT hasFocus.uuid) as hasFocusUUIDs,
				role,
				roleRel,
				parentOrg,
				coo,
				cor,
				coi,
				collect(DISTINCT {UUID: naics.uuid, Rank: hasICRel.rank}) as naicsIndustryClassifications,
				source
				ORDER BY
					source.uuid,
					role.uuid
			WITH
				canonical,
				issuer,
				org,
				person,
				{
					authority: source.authority,
					authorityValue: source.authorityValue,
					broaderUUIDs: broaderUUIDs,
					supersededByUUIDs: supersededByUUIDs,
					figiCode: source.figiCode,
					issuedBy: issuer.uuid,
					lastModifiedEpoch: source.lastModifiedEpoch,
					membershipRoles: collect({
						membershipRoleUUID: role.uuid,
						inceptionDate: roleRel.inceptionDate,
						terminationDate: roleRel.terminationDate,
						inceptionDateEpoch: roleRel.inceptionDateEpoch,
						terminationDateEpoch: roleRel.terminationDateEpoch
					}),
					organisationUUID: org.uuid,
					parentUUIDs: collect(parent.uuid),
					personUUID: person.uuid,
					parentOrganisation: parentOrg.uuid,
					prefLabel: source.prefLabel,
					relatedUUIDs: relatedUUIDs,
					impliedByUUIDs: impliedByUUIDs,
					hasFocusUUIDs: hasFocusUUIDs,
					types: labels(source),
					uuid: source.uuid,
					isDeprecated: source.isDeprecated,
					countryOfIncorporationUUID: coi.uuid,
					countryOfOperationsUUID: coo.uuid,
					countryOfRiskUUID: cor.uuid,
					industryIdentifier: source.industryIdentifier,
					naicsIndustryClassifications: naicsIndustryClassifications
				} as sources,
				collect({
					inceptionDate: roleRel.inceptionDate,
					inceptionDateEpoch: roleRel.inceptionDateEpoch,
					membershipRoleUUID: role.uuid,
					terminationDate: roleRel.terminationDate,
					terminationDateEpoch: roleRel.terminationDateEpoch
				}) as membershipRoles
			RETURN
				canonical.aggregateHash as aggregateHash,
				canonical.aliases as aliases,
				canonical.descriptionXML as descriptionXML,
				canonical.emailAddress as emailAddress,
				canonical.facebookPage as facebookPage,
				canonical.figiCode as figiCode,
				canonical.imageUrl as imageUrl,
				canonical.inceptionDate as inceptionDate,
				canonical.inceptionDateEpoch as inceptionDateEpoch,
				canonical.prefLabel as prefLabel,
				canonical.prefUUID as prefUUID,
				canonical.scopeNote as scopeNote,
				canonical.shortLabel as shortLabel,
				canonical.strapline as strapline,
				canonical.terminationDate as terminationDate,
				canonical.terminationDateEpoch as terminationDateEpoch,
				canonical.twitterHandle as twitterHandle,
				collect(sources) as sourceRepresentations,
				issuer.uuid as issuedBy,
				labels(canonical) as types,
				membershipRoles,
				org.uuid as organisationUUID,
				person.uuid as personUUID,
				canonical.properName as properName,
				canonical.shortName as shortName,
				canonical.tradeNames as tradeNames,
				canonical.formerNames as formerNames,
				canonical.countryCode as countryCode,
				canonical.countryOfIncorporation as countryOfIncorporation,
				canonical.countryOfOperations as countryOfOperations,
				canonical.countryOfRisk as countryOfRisk,
				canonical.postalCode as postalCode,
				canonical.yearFounded as yearFounded,
				canonical.leiCode as leiCode,
				canonical.isDeprecated as isDeprecated,
				canonical.salutation as salutation,
				canonical.birthYear as birthYear,
				canonical.iso31661 as iso31661,
				canonical.industryIdentifier as industryIdentifier
			`,
		Parameters: map[string]interface{}{
			"uuid": uuid,
		},
		Result: results,
	}
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

	return a
}
