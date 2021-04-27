package ontology

// POC: most property types here are self-explanatory only "date" is strange
// The idea is that property with "date" type will add two node properties
// one is a datetime string and another unix epoch int. And the epoch field will not be visible from the outside.
// That way we can cover "InceptionDateEpoch", "TerminationDateEpoch" fields for the Membership/MembershipRole Concepts
// Property Types
const (
	PropertyStringType      = "string"
	PropertyStringSliceType = "[]string"
	PropertyIntType         = "int"
	PropertyBoolType        = "bool"
	PropertyDateType        = "date"
)

// Property Locations
const (
	SourceProperty    = "source-property"
	CanonicalProperty = "canonical-property"
)

// Property names
const (
	UUIDProp           = "uuid"
	PrefUUIDProp       = "prefUUID"
	AuthorityProp      = "authority"
	AuthorityValueProp = "authorityValue"
	PrefLabelProp      = "prefLabel"
	AliasesProp        = "aliases"
	StraplineProp      = "strapline"
	DescriptionProp    = "descriptionXML"
	ImageURLProp       = "imageUrl"
	EmailAddressProp   = "emailAddress"
	FacebookPageProp   = "facebookPage"
	TwitterHandleProp  = "twitterHandle"
	ScopeNoteProp      = "scopeNote"
	ShortLabelProp     = "shortLabel"
	FigiCodeProp       = "figiCode"
	// Organisations
	ProperNameProp             = "properName"
	ShortNameProp              = "shortName"
	TradeNamesProp             = "tradeNames"
	FormerNamesProp            = "formerNames"
	CountryCodeProp            = "countryCode"
	CountryOfRiskProp          = "countryOfRisk"
	CountryOfIncorporationProp = "countryOfIncorporation"
	CountryOfOperationsProp    = "countryOfOperations"
	PostalCodeProp             = "postalCode"
	YearFoundedProp            = "yearFounded"
	LeiCodeProp                = "leiCode"
	IsDeprecatedProp           = "isDeprecated"
	ISO31661Prop               = "iso31661"
	SalutationProp             = "salutation"
	BirthYearProp              = "birthYear"
	IndustryIdentifierProp     = "industryIdentifier"

	InceptionDateProp        = "inceptionDate"
	TerminationDateProp      = "terminationDate"
	InceptionDateEpochProp   = "inceptionDateEpoch"
	TerminationDateEpochProp = "terminationDateEpoch"
)

type PropertyConfig struct {
	// Type should be one of the supported "Property Types"
	// Used for transformations and validations
	Type string
	// Neo4J field label
	// NeoLabel describes under what name the property will be stored in the neo node
	NeoLabel string
	// NodeRestriction allows for a way to filter out properties based on some context
	// It is used to differentiate between properties that should be on the source node or canonical node.
	NodeRestriction map[string]bool
	// UPP Concept setup

	ConceptField string
}

// GetPropertySetup returns the node property configurations allowed in the Knowledge graph
func GetPropertySetup() map[string]PropertyConfig {
	return map[string]PropertyConfig{
		PrefLabelProp: {
			Type:     PropertyStringType,
			NeoLabel: "prefLabel",
			NodeRestriction: map[string]bool{
				SourceProperty:    true,
				CanonicalProperty: true,
			},
			ConceptField: "prefLabel",
		},
		AuthorityProp: {
			Type: PropertyStringType,
			NodeRestriction: map[string]bool{
				SourceProperty: true,
			},
			NeoLabel:     "authority",
			ConceptField: "authority",
		},
		AuthorityValueProp: {
			Type: PropertyStringType,
			NodeRestriction: map[string]bool{
				SourceProperty: true,
			},
			NeoLabel:     "authorityValue",
			ConceptField: "authorityValue",
		},
		AliasesProp: {
			Type: PropertyStringSliceType,
			NodeRestriction: map[string]bool{
				CanonicalProperty: true,
			},
			NeoLabel:     "aliases",
			ConceptField: "aliases",
		},
		StraplineProp: {
			Type: PropertyStringType,
			NodeRestriction: map[string]bool{
				CanonicalProperty: true,
			},
			NeoLabel:     "strapline",
			ConceptField: "strapline",
		},
		DescriptionProp: {
			Type: PropertyStringType,
			NodeRestriction: map[string]bool{
				CanonicalProperty: true,
			},
			NeoLabel:     "descriptionXML",
			ConceptField: "descriptionXML",
		},
		ImageURLProp: {
			Type: PropertyStringType,
			NodeRestriction: map[string]bool{
				CanonicalProperty: true,
			},
			NeoLabel:     "imageUrl",
			ConceptField: "_imageUrl",
		},
		EmailAddressProp: {
			Type: PropertyStringType,
			NodeRestriction: map[string]bool{
				CanonicalProperty: true,
			},
			NeoLabel:     "emailAddress",
			ConceptField: "emailAddress",
		},
		FacebookPageProp: {
			Type: PropertyStringType,
			NodeRestriction: map[string]bool{
				CanonicalProperty: true,
			},
			NeoLabel:     "facebookPage",
			ConceptField: "facebookPage",
		},
		TwitterHandleProp: {
			Type: PropertyStringType,
			NodeRestriction: map[string]bool{
				CanonicalProperty: true,
			},
			NeoLabel:     "twitterHandle",
			ConceptField: "twitterHandle",
		},
		ScopeNoteProp: {
			Type: PropertyStringType,
			NodeRestriction: map[string]bool{
				CanonicalProperty: true,
			},
			NeoLabel:     "scopeNote",
			ConceptField: "scopeNote",
		},
		ShortLabelProp: {
			Type: PropertyStringType,
			NodeRestriction: map[string]bool{
				CanonicalProperty: true,
			},
			NeoLabel:     "shortLabel",
			ConceptField: "shortLabel",
		},
		InceptionDateProp: {
			Type: PropertyDateType,
			NodeRestriction: map[string]bool{
				CanonicalProperty: true,
			},
			NeoLabel:     "inceptionDate",
			ConceptField: "inceptionDate",
		},
		TerminationDateProp: {
			Type: PropertyDateType,
			NodeRestriction: map[string]bool{
				CanonicalProperty: true,
			},
			NeoLabel:     "terminationDate",
			ConceptField: "terminationDate",
		},
		InceptionDateEpochProp: {
			Type: PropertyDateType,
			NodeRestriction: map[string]bool{
				CanonicalProperty: true,
			},
			NeoLabel:     "inceptionDateEpoch",
			ConceptField: "inceptionDateEpoch",
		},
		TerminationDateEpochProp: {
			Type: PropertyDateType,
			NodeRestriction: map[string]bool{
				CanonicalProperty: true,
			},
			NeoLabel:     "terminationDateEpoch",
			ConceptField: "terminationDateEpoch",
		},
		FigiCodeProp: {
			Type:     PropertyStringType,
			NeoLabel: "figiCode",
			NodeRestriction: map[string]bool{
				SourceProperty:    true,
				CanonicalProperty: true,
			},
			ConceptField: "figiCode",
		},
		ProperNameProp: {
			Type: PropertyStringType,
			NodeRestriction: map[string]bool{
				CanonicalProperty: true,
			},
			NeoLabel:     "properName",
			ConceptField: "properName",
		},
		ShortNameProp: {
			Type: PropertyStringType,
			NodeRestriction: map[string]bool{
				CanonicalProperty: true,
			},
			NeoLabel:     "shortName",
			ConceptField: "shortName",
		},
		TradeNamesProp: {
			Type: PropertyStringSliceType,
			NodeRestriction: map[string]bool{
				CanonicalProperty: true,
			},
			NeoLabel:     "tradeNames",
			ConceptField: "tradeNames",
		},
		FormerNamesProp: {
			Type: PropertyStringSliceType,
			NodeRestriction: map[string]bool{
				CanonicalProperty: true,
			},
			NeoLabel:     "formerNames",
			ConceptField: "formerNames",
		},
		CountryCodeProp: {
			Type: PropertyStringType,
			NodeRestriction: map[string]bool{
				CanonicalProperty: true,
			},
			NeoLabel:     "countryCode",
			ConceptField: "countryCode",
		},
		CountryOfRiskProp: {
			Type: PropertyStringType,
			NodeRestriction: map[string]bool{
				CanonicalProperty: true,
			},
			NeoLabel:     "countryOfRisk",
			ConceptField: "countryOfRisk",
		},
		CountryOfIncorporationProp: {
			Type: PropertyStringType,
			NodeRestriction: map[string]bool{
				CanonicalProperty: true,
			},
			NeoLabel:     "countryOfIncorporation",
			ConceptField: "countryOfIncorporation",
		},
		CountryOfOperationsProp: {
			Type: PropertyStringType,
			NodeRestriction: map[string]bool{
				CanonicalProperty: true,
			},
			NeoLabel:     "countryOfOperations",
			ConceptField: "countryOfOperations",
		},
		PostalCodeProp: {
			Type: PropertyStringType,
			NodeRestriction: map[string]bool{
				CanonicalProperty: true,
			},
			NeoLabel:     "postalCode",
			ConceptField: "postalCode",
		},
		YearFoundedProp: {
			Type: PropertyIntType,
			NodeRestriction: map[string]bool{
				CanonicalProperty: true,
			},
			NeoLabel:     "yearFounded",
			ConceptField: "yearFounded",
		},
		LeiCodeProp: {
			Type: PropertyStringType,
			NodeRestriction: map[string]bool{
				CanonicalProperty: true,
			},
			NeoLabel:     "leiCode",
			ConceptField: "leiCode",
		},
		IsDeprecatedProp: {
			Type:     PropertyBoolType,
			NeoLabel: "isDeprecated",
			NodeRestriction: map[string]bool{
				SourceProperty:    true,
				CanonicalProperty: true,
			},
			ConceptField: "isDeprecated",
		},
		ISO31661Prop: {
			Type: PropertyStringType,
			NodeRestriction: map[string]bool{
				CanonicalProperty: true,
			},
			NeoLabel:     "iso31661",
			ConceptField: "iso31661",
		},
		SalutationProp: {
			Type: PropertyStringType,
			NodeRestriction: map[string]bool{
				CanonicalProperty: true,
			},
			NeoLabel:     "salutation",
			ConceptField: "salutation",
		},
		BirthYearProp: {
			Type: PropertyIntType,
			NodeRestriction: map[string]bool{
				CanonicalProperty: true,
			},
			NeoLabel:     "birthYear",
			ConceptField: "birthYear",
		},
		IndustryIdentifierProp: {
			Type: PropertyStringType,
			NodeRestriction: map[string]bool{
				CanonicalProperty: true,
			},
			NeoLabel:     "industryIdentifier",
			ConceptField: "industryIdentifier",
		},
	}
}

func GetFilteredPropertySetup(restriction string) map[string]PropertyConfig {
	config := GetPropertySetup()
	result := make(map[string]PropertyConfig)
	for prop, conf := range config {
		if conf.NodeRestriction[restriction] {
			result[prop] = conf
		}
	}
	return result
}
