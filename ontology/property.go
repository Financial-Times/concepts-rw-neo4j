package ontology

const (
	PropertyStringType      = "string"
	PropertyStringSliceType = "[]string"
	PropertyIntType         = "int"
	PropertyBoolType        = "bool"
	PropertyDateType        = "date"
)

const (
	SourceProperty    = "source-property"
	CanonicalProperty = "canonical-property"
)

type PropertyConfig struct {
	Type string
	// Neo4J field label
	NeoLabel        string
	NodeRestriction map[string]bool
	// UPP Concept setup
	ConceptField string
}

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
