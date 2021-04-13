package ontology

type MembershipRole struct {
	RoleUUID             string `json:"membershipRoleUUID,omitempty"`
	InceptionDate        string `json:"inceptionDate,omitempty"`
	TerminationDate      string `json:"terminationDate,omitempty"`
	InceptionDateEpoch   int64  `json:"inceptionDateEpoch,omitempty"`
	TerminationDateEpoch int64  `json:"terminationDateEpoch,omitempty"`
}

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
	SourceRepresentations []SourceConcept  `json:"sourceRepresentations,omitempty"`
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

// NAICSIndustryClassification represents a pair of uuid of industry classification concept and the rank
// of that industry classification for a particular organisation
type NAICSIndustryClassification struct {
	UUID string `json:"uuid,omitempty"`
	Rank int    `json:"rank,omitempty"`
}

// SourceConcept - could be any concept genre, subject etc
type SourceConcept struct {
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

var ConceptLabels = [...]string{
	"Concept",
	"Classification",
	"Section",
	"Subject",
	"SpecialReport",
	"Topic",
	"Location",
	"Genre",
	"Brand",
	"Person",
	"Organisation",
	"MembershipRole",
	"Membership",
	"BoardRole",
	"FinancialInstrument",
	"Company",
	"PublicCompany",
	"IndustryClassification",
	"NAICSIndustryClassification",
}

var authorities = []string{
	"TME",
	"FACTSET",
	"UPP",
	"LEI",
	"Smartlogic",
	"ManagedLocation",
	"ISO-3166-1",
	"Geonames",
	"Wikidata",
	"DBPedia",
	"NAICS",
}

const (
	PrefLabelProp = "prefLabel"
)

type GenericConcept struct {
	Properties map[string]interface{} `json:"properties"`

	Relationships []struct {
		UUID       string                 `json:"uuid"`
		Label      string                 `json:"label"`
		Properties map[string]interface{} `json:"properties"`
	} `json:"relationships"`
}

func (c GenericConcept) GetProp(label string) (interface{}, bool) {
	val, has := c.Properties[label]
	return val, has
}

func (c GenericConcept) GetPropString(label string) (string, bool) {
	val, has := c.GetProp(label)
	if !has {
		return "", false
	}
	prop, is := val.(string)
	if !is {
		return "", false
	}
	return prop, true
}

type NewAggregatedConcept struct {
	GenericConcept
	PrefUUID              string             `json:"prefUUID,omitempty"`
	Type                  string             `json:"type,omitempty"`
	Aliases               []string           `json:"aliases,omitempty"`
	Strapline             string             `json:"strapline,omitempty"`
	DescriptionXML        string             `json:"descriptionXML,omitempty"`
	ImageURL              string             `json:"_imageUrl,omitempty"`
	EmailAddress          string             `json:"emailAddress,omitempty"`
	FacebookPage          string             `json:"facebookPage,omitempty"`
	TwitterHandle         string             `json:"twitterHandle,omitempty"`
	ScopeNote             string             `json:"scopeNote,omitempty"`
	ShortLabel            string             `json:"shortLabel,omitempty"`
	OrganisationUUID      string             `json:"organisationUUID,omitempty"`
	PersonUUID            string             `json:"personUUID,omitempty"`
	AggregatedHash        string             `json:"aggregateHash,omitempty"`
	SourceRepresentations []NewSourceConcept `json:"sourceRepresentations,omitempty"`
	MembershipRoles       []MembershipRole   `json:"membershipRoles,omitempty"`
	InceptionDate         string             `json:"inceptionDate,omitempty"`
	TerminationDate       string             `json:"terminationDate,omitempty"`
	InceptionDateEpoch    int64              `json:"inceptionDateEpoch,omitempty"`
	TerminationDateEpoch  int64              `json:"terminationDateEpoch,omitempty"`
	FigiCode              string             `json:"figiCode,omitempty"`
	IssuedBy              string             `json:"issuedBy,omitempty"`
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

// SourceConcept - could be any concept genre, subject etc
type NewSourceConcept struct {
	GenericConcept
	UUID                 string           `json:"uuid,omitempty"`
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
