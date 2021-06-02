package concepts

import (
	"sort"

	"github.com/Financial-Times/concepts-rw-neo4j/ontology"
	"github.com/Financial-Times/neo-model-utils-go/mapper"
)

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

func (nac neoAggregatedConcept) ToOntologyAggregateConcept() (ontology.AggregatedConcept, string, error) {
	typeName, err := mapper.MostSpecificType(nac.Types)
	if err != nil {
		return ontology.AggregatedConcept{}, "Returned concept had no recognized type", err
	}

	var sourceConcepts []ontology.Concept
	for _, srcConcept := range nac.SourceRepresentations {
		concept, err := srcConcept.ТоOntologyConcept()
		if err != nil {
			return ontology.AggregatedConcept{}, "Returned source concept had no recognized type", err
		}

		sourceConcepts = append(sourceConcepts, concept)
	}

	aggregateConcept := ontology.AggregatedConcept{
		SourceRepresentations: sourceConcepts,
		AggregatedHash:        nac.AggregateHash,
		Aliases:               nac.Aliases,
		DescriptionXML:        nac.DescriptionXML,
		EmailAddress:          nac.EmailAddress,
		FacebookPage:          nac.FacebookPage,
		FigiCode:              nac.FigiCode,
		ImageURL:              nac.ImageURL,
		InceptionDate:         nac.InceptionDate,
		IssuedBy:              nac.IssuedBy,
		MembershipRoles:       cleanMembershipRoles(nac.MembershipRoles),
		OrganisationUUID:      nac.OrganisationUUID,
		PersonUUID:            nac.PersonUUID,
		PrefLabel:             nac.PrefLabel,
		PrefUUID:              nac.PrefUUID,
		ScopeNote:             nac.ScopeNote,
		ShortLabel:            nac.ShortLabel,
		Strapline:             nac.Strapline,
		TerminationDate:       nac.TerminationDate,
		TwitterHandle:         nac.TwitterHandle,
		Type:                  typeName,
		IsDeprecated:          nac.IsDeprecated,
		// Organisations
		ProperName:             nac.ProperName,
		ShortName:              nac.ShortName,
		TradeNames:             nac.TradeNames,
		FormerNames:            nac.FormerNames,
		CountryCode:            nac.CountryCode,
		CountryOfIncorporation: nac.CountryOfIncorporation,
		CountryOfRisk:          nac.CountryOfRisk,
		CountryOfOperations:    nac.CountryOfOperations,
		PostalCode:             nac.PostalCode,
		YearFounded:            nac.YearFounded,
		LeiCode:                nac.LeiCode,
		// Person
		Salutation: nac.Salutation,
		BirthYear:  nac.BirthYear,
		// Location
		ISO31661: nac.ISO31661,
		// Industry Classification
		IndustryIdentifier: nac.IndustryIdentifier,
	}

	return cleanConcept(aggregateConcept), "", nil
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

func (nc neoConcept) ТоOntologyConcept() (ontology.Concept, error) {
	conceptType, err := mapper.MostSpecificType(nc.Types)
	if err != nil {
		return ontology.Concept{}, err
	}

	return ontology.Concept{
		Authority:                    nc.Authority,
		AuthorityValue:               nc.AuthorityValue,
		BroaderUUIDs:                 filterSlice(nc.BroaderUUIDs),
		SupersededByUUIDs:            filterSlice(nc.SupersededByUUIDs),
		FigiCode:                     nc.FigiCode,
		IssuedBy:                     nc.IssuedBy,
		LastModifiedEpoch:            nc.LastModifiedEpoch,
		MembershipRoles:              cleanMembershipRoles(nc.MembershipRoles),
		OrganisationUUID:             nc.OrganisationUUID,
		CountryOfIncorporationUUID:   nc.CountryOfIncorporationUUID,
		CountryOfRiskUUID:            nc.CountryOfRiskUUID,
		CountryOfOperationsUUID:      nc.CountryOfOperationsUUID,
		ParentUUIDs:                  filterSlice(nc.ParentUUIDs),
		PersonUUID:                   nc.PersonUUID,
		PrefLabel:                    nc.PrefLabel,
		RelatedUUIDs:                 filterSlice(nc.RelatedUUIDs),
		ImpliedByUUIDs:               filterSlice(nc.ImpliedByUUIDs),
		HasFocusUUIDs:                filterSlice(nc.HasFocusUUIDs),
		NAICSIndustryClassifications: cleanNAICS(nc.NAICSIndustryClassifications),
		Type:                         conceptType,
		UUID:                         nc.UUID,
		IsDeprecated:                 nc.IsDeprecated,
		// Organisations
		ParentOrganisation: nc.ParentOrganisation,
	}, nil
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

func cleanMembershipRoles(m []ontology.MembershipRole) []ontology.MembershipRole {
	deleted := 0
	for i := range m {
		j := i - deleted
		if m[j].RoleUUID == "" {
			m = m[:j+copy(m[j:], m[j+1:])]
			deleted++
			continue
		}
		m[j].InceptionDateEpoch = getEpoch(m[j].InceptionDate)
		m[j].TerminationDateEpoch = getEpoch(m[j].TerminationDate)
	}

	if len(m) == 0 {
		return nil
	}

	return m
}

// cleanNAICS returns the same slice of NAICSIndustryClassification if all are valid,
// skips the invalid ones, returns nil if the input slice doesn't have valid NAICSIndustryClassification objects
func cleanNAICS(naics []ontology.NAICSIndustryClassification) []ontology.NAICSIndustryClassification {
	var res []ontology.NAICSIndustryClassification
	for _, ic := range naics {
		if ic.UUID != "" {
			res = append(res, ic)
		}
	}
	return res
}

func cleanConcept(c ontology.AggregatedConcept) ontology.AggregatedConcept {
	for j := range c.SourceRepresentations {
		c.SourceRepresentations[j].LastModifiedEpoch = 0
		for i := range c.SourceRepresentations[j].MembershipRoles {
			c.SourceRepresentations[j].MembershipRoles[i].InceptionDateEpoch = 0
			c.SourceRepresentations[j].MembershipRoles[i].TerminationDateEpoch = 0
		}
		sort.SliceStable(c.SourceRepresentations[j].MembershipRoles, func(k, l int) bool {
			return c.SourceRepresentations[j].MembershipRoles[k].RoleUUID < c.SourceRepresentations[j].MembershipRoles[l].RoleUUID
		})
		sort.SliceStable(c.SourceRepresentations[j].BroaderUUIDs, func(k, l int) bool {
			return c.SourceRepresentations[j].BroaderUUIDs[k] < c.SourceRepresentations[j].BroaderUUIDs[l]
		})
		sort.SliceStable(c.SourceRepresentations[j].RelatedUUIDs, func(k, l int) bool {
			return c.SourceRepresentations[j].RelatedUUIDs[k] < c.SourceRepresentations[j].RelatedUUIDs[l]
		})
		sort.SliceStable(c.SourceRepresentations[j].SupersededByUUIDs, func(k, l int) bool {
			return c.SourceRepresentations[j].SupersededByUUIDs[k] < c.SourceRepresentations[j].SupersededByUUIDs[l]
		})
		sort.SliceStable(c.SourceRepresentations[j].ImpliedByUUIDs, func(k, l int) bool {
			return c.SourceRepresentations[j].ImpliedByUUIDs[k] < c.SourceRepresentations[j].ImpliedByUUIDs[l]
		})
		sort.SliceStable(c.SourceRepresentations[j].HasFocusUUIDs, func(k, l int) bool {
			return c.SourceRepresentations[j].HasFocusUUIDs[k] < c.SourceRepresentations[j].HasFocusUUIDs[l]
		})
		sort.SliceStable(c.SourceRepresentations[j].NAICSIndustryClassifications, func(k, l int) bool {
			return c.SourceRepresentations[j].NAICSIndustryClassifications[k].Rank < c.SourceRepresentations[j].NAICSIndustryClassifications[l].Rank
		})
	}
	for i := range c.MembershipRoles {
		c.MembershipRoles[i].InceptionDateEpoch = 0
		c.MembershipRoles[i].TerminationDateEpoch = 0
	}
	sort.SliceStable(c.SourceRepresentations, func(k, l int) bool {
		return c.SourceRepresentations[k].UUID < c.SourceRepresentations[l].UUID
	})
	return c
}
