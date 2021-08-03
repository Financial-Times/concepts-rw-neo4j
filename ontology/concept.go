package ontology

import (
	"errors"
)

type AggregatedConcept struct {
	PrefUUID              string           `json:"prefUUID,omitempty"`
	PrefLabel             string           `json:"prefLabel,omitempty"`
	Type                  string           `json:"type,omitempty"`
	Aliases               []string         `json:"aliases,omitempty"`
	Strapline             string           `json:"strapline,omitempty"`
	DescriptionXML        string           `json:"descriptionXML,omitempty"`
	ImageURL              string           `json:"_imageUrl,omitempty"`
	EmailAddress          string           `json:"emailAddress,omitempty"`
	FacebookPage          string           `json:"facebookPage,omitempty"`
	TwitterHandle         string           `json:"twitterHandle,omitempty"`
	ScopeNote             string           `json:"scopeNote,omitempty"`
	ShortLabel            string           `json:"shortLabel,omitempty"`
	OrganisationUUID      string           `json:"organisationUUID,omitempty"`
	PersonUUID            string           `json:"personUUID,omitempty"`
	AggregatedHash        string           `json:"aggregateHash,omitempty"`
	SourceRepresentations []Concept        `json:"sourceRepresentations,omitempty"`
	MembershipRoles       []MembershipRole `json:"membershipRoles,omitempty"`
	InceptionDate         string           `json:"inceptionDate,omitempty"`
	TerminationDate       string           `json:"terminationDate,omitempty"`
	InceptionDateEpoch    int64            `json:"inceptionDateEpoch,omitempty"`
	TerminationDateEpoch  int64            `json:"terminationDateEpoch,omitempty"`
	FigiCode              string           `json:"figiCode,omitempty"`
	IssuedBy              string           `json:"issuedBy,omitempty"`
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
	IsDeprecated           bool     `json:"isDeprecated,omitempty"`
	// Location
	ISO31661 string `json:"iso31661,omitempty"`
	// Person
	Salutation string `json:"salutation,omitempty"`
	BirthYear  int    `json:"birthYear,omitempty"`
	// Industry Classifications
	IndustryIdentifier string `json:"industryIdentifier,omitempty"`
}

// Concept - could be any concept genre, subject etc
type Concept struct {
	UUID                 string           `json:"uuid,omitempty"`
	PrefLabel            string           `json:"prefLabel,omitempty"`
	Type                 string           `json:"type,omitempty"`
	Authority            string           `json:"authority,omitempty"`
	AuthorityValue       string           `json:"authorityValue,omitempty"`
	LastModifiedEpoch    int              `json:"lastModifiedEpoch,omitempty"`
	Aliases              []string         `json:"aliases,omitempty"`
	ParentUUIDs          []string         `json:"parentUUIDs,omitempty"`
	Strapline            string           `json:"strapline,omitempty"`
	DescriptionXML       string           `json:"descriptionXML,omitempty"`
	ImageURL             string           `json:"_imageUrl,omitempty"`
	EmailAddress         string           `json:"emailAddress,omitempty"`
	FacebookPage         string           `json:"facebookPage,omitempty"`
	TwitterHandle        string           `json:"twitterHandle,omitempty"`
	ScopeNote            string           `json:"scopeNote,omitempty"`
	ShortLabel           string           `json:"shortLabel,omitempty"`
	BroaderUUIDs         []string         `json:"broaderUUIDs,omitempty"`
	RelatedUUIDs         []string         `json:"relatedUUIDs,omitempty"`
	SupersededByUUIDs    []string         `json:"supersededByUUIDs,omitempty"`
	ImpliedByUUIDs       []string         `json:"impliedByUUIDs,omitempty"`
	HasFocusUUIDs        []string         `json:"hasFocusUUIDs,omitempty"`
	OrganisationUUID     string           `json:"organisationUUID,omitempty"`
	PersonUUID           string           `json:"personUUID,omitempty"`
	Hash                 string           `json:"hash,omitempty"`
	MembershipRoles      []MembershipRole `json:"membershipRoles,omitempty"`
	InceptionDate        string           `json:"inceptionDate,omitempty"`
	TerminationDate      string           `json:"terminationDate,omitempty"`
	InceptionDateEpoch   int64            `json:"inceptionDateEpoch,omitempty"`
	TerminationDateEpoch int64            `json:"terminationDateEpoch,omitempty"`
	FigiCode             string           `json:"figiCode,omitempty"`
	IssuedBy             string           `json:"issuedBy,omitempty"`
	// Organisations
	ProperName                   string                        `json:"properName,omitempty"`
	ShortName                    string                        `json:"shortName,omitempty"`
	TradeNames                   []string                      `json:"tradeNames,omitempty"`
	FormerNames                  []string                      `json:"formerNames,omitempty"`
	CountryCode                  string                        `json:"countryCode,omitempty"`
	CountryOfRisk                string                        `json:"countryOfRisk,omitempty"`
	CountryOfIncorporation       string                        `json:"countryOfIncorporation,omitempty"`
	CountryOfOperations          string                        `json:"countryOfOperations,omitempty"`
	CountryOfRiskUUID            string                        `json:"countryOfRiskUUID,omitempty"`
	CountryOfIncorporationUUID   string                        `json:"countryOfIncorporationUUID,omitempty"`
	CountryOfOperationsUUID      string                        `json:"countryOfOperationsUUID,omitempty"`
	PostalCode                   string                        `json:"postalCode,omitempty"`
	YearFounded                  int                           `json:"yearFounded,omitempty"`
	LeiCode                      string                        `json:"leiCode,omitempty"`
	ParentOrganisation           string                        `json:"parentOrganisation,omitempty"`
	NAICSIndustryClassifications []NAICSIndustryClassification `json:"naicsIndustryClassifications,omitempty"`
	IsDeprecated                 bool                          `json:"isDeprecated,omitempty"`
	// Location
	ISO31661 string `json:"iso31661,omitempty"`
	// Person
	Salutation string `json:"salutation,omitempty"`
	BirthYear  int    `json:"birthYear,omitempty"`
	// Industry Classifications
	IndustryIdentifier string `json:"industryIdentifier,omitempty"`
}

type MembershipRole struct {
	RoleUUID             string `json:"membershipRoleUUID,omitempty"`
	InceptionDate        string `json:"inceptionDate,omitempty"`
	TerminationDate      string `json:"terminationDate,omitempty"`
	InceptionDateEpoch   int64  `json:"inceptionDateEpoch,omitempty"`
	TerminationDateEpoch int64  `json:"terminationDateEpoch,omitempty"`
}

// NAICSIndustryClassification represents a pair of uuid of industry classification concept and the rank
// of that industry classification for a particular organisation
type NAICSIndustryClassification struct {
	UUID string `json:"uuid,omitempty"`
	Rank int    `json:"rank,omitempty"`
}

type Relationship struct {
	UUID       string                 `json:"uuid"`
	Label      string                 `json:"label"`
	Properties map[string]interface{} `json:"properties"`
}

type NewAggregatedConcept struct {
	Properties            map[string]interface{} `json:"properties"`
	PrefUUID              string                 `json:"prefUUID,omitempty"`
	PrefLabel             string                 `json:"prefLabel,omitempty"`
	Type                  string                 `json:"type,omitempty"`
	Aliases               []string               `json:"aliases,omitempty"`
	OrganisationUUID      string                 `json:"organisationUUID,omitempty"`
	PersonUUID            string                 `json:"personUUID,omitempty"`
	AggregatedHash        string                 `json:"aggregateHash,omitempty"`
	SourceRepresentations []NewConcept           `json:"sourceRepresentations,omitempty"`
	InceptionDate         string                 `json:"inceptionDate,omitempty"`
	TerminationDate       string                 `json:"terminationDate,omitempty"`
	InceptionDateEpoch    int64                  `json:"inceptionDateEpoch,omitempty"`
	TerminationDateEpoch  int64                  `json:"terminationDateEpoch,omitempty"`
	FigiCode              string                 `json:"figiCode,omitempty"`
	IssuedBy              string                 `json:"issuedBy,omitempty"`
	// Organisations
	TradeNames   []string `json:"tradeNames,omitempty"`
	FormerNames  []string `json:"formerNames,omitempty"`
	YearFounded  int      `json:"yearFounded,omitempty"`
	IsDeprecated bool     `json:"isDeprecated,omitempty"`
	BirthYear    int      `json:"birthYear,omitempty"`
}

func (c NewAggregatedConcept) GetPropertyValue(propName string) (interface{}, bool) {
	val, found := c.Properties[propName]
	if !found {
		return nil, false
	}

	switch v := val.(type) {
	case []interface{}:
		return v, len(v) > 0
	case string:
		return v, v != ""
	case float64:
		return v, v > 0
	default: // return values of unknown type but indicate that they were not validated
		return v, false
	}
}

func (c NewAggregatedConcept) GetCanonicalAuthority() string {
	for _, source := range c.SourceRepresentations {
		if source.UUID == c.PrefUUID {
			return source.Authority
		}
	}

	return ""
}

// NewConcept - could be any concept genre, subject etc
type NewConcept struct {
	Relationships        []Relationship   `json:"relationships"`
	UUID                 string           `json:"uuid,omitempty"`
	PrefLabel            string           `json:"prefLabel,omitempty"`
	Type                 string           `json:"type,omitempty"`
	Authority            string           `json:"authority,omitempty"`
	AuthorityValue       string           `json:"authorityValue,omitempty"`
	LastModifiedEpoch    int              `json:"lastModifiedEpoch,omitempty"`
	Aliases              []string         `json:"aliases,omitempty"`
	ParentUUIDs          []string         `json:"parentUUIDs,omitempty"`
	BroaderUUIDs         []string         `json:"broaderUUIDs,omitempty"`
	RelatedUUIDs         []string         `json:"relatedUUIDs,omitempty"`
	SupersededByUUIDs    []string         `json:"supersededByUUIDs,omitempty"`
	ImpliedByUUIDs       []string         `json:"impliedByUUIDs,omitempty"`
	HasFocusUUIDs        []string         `json:"hasFocusUUIDs,omitempty"`
	OrganisationUUID     string           `json:"organisationUUID,omitempty"`
	PersonUUID           string           `json:"personUUID,omitempty"`
	Hash                 string           `json:"hash,omitempty"`
	MembershipRoles      []MembershipRole `json:"membershipRoles,omitempty"`
	InceptionDate        string           `json:"inceptionDate,omitempty"`
	TerminationDate      string           `json:"terminationDate,omitempty"`
	InceptionDateEpoch   int64            `json:"inceptionDateEpoch,omitempty"`
	TerminationDateEpoch int64            `json:"terminationDateEpoch,omitempty"`
	FigiCode             string           `json:"figiCode,omitempty"`
	IssuedBy             string           `json:"issuedBy,omitempty"`
	// Organisations
	TradeNames                   []string                      `json:"tradeNames,omitempty"`
	FormerNames                  []string                      `json:"formerNames,omitempty"`
	CountryOfRiskUUID            string                        `json:"countryOfRiskUUID,omitempty"`
	CountryOfIncorporationUUID   string                        `json:"countryOfIncorporationUUID,omitempty"`
	CountryOfOperationsUUID      string                        `json:"countryOfOperationsUUID,omitempty"`
	YearFounded                  int                           `json:"yearFounded,omitempty"`
	ParentOrganisation           string                        `json:"parentOrganisation,omitempty"`
	NAICSIndustryClassifications []NAICSIndustryClassification `json:"naicsIndustryClassifications,omitempty"`
	IsDeprecated                 bool                          `json:"isDeprecated,omitempty"`
	BirthYear                    int                           `json:"birthYear,omitempty"`
}

var ErrEmptyAuthority = errors.New("invalid request, no sourceRepresentation.authority has been supplied")
var ErrUnkownAuthority = errors.New("unknown authority")
var ErrEmptyAuthorityValue = errors.New("invalid request, no sourceRepresentation.authorityValue has been supplied")

func (c NewConcept) Validate() error {
	if c.Authority == "" {
		return ErrEmptyAuthority
	}

	if !stringInArr(c.Authority, GetConfig().Authorities) {
		return ErrUnkownAuthority
	}

	if c.AuthorityValue == "" {
		return ErrEmptyAuthorityValue
	}

	return nil
}

func stringInArr(searchFor string, values []string) bool {
	for _, val := range values {
		if searchFor == val {
			return true
		}
	}
	return false
}
