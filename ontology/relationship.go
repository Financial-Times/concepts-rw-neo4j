package ontology

// Relation names
const (
	BroaderRelation         = "HAS_BROADER"
	ParentRelation          = "HAS_PARENT"
	ImpliedByRelation       = "IMPLIED_BY"
	HasFocusRelation        = "HAS_FOCUS"
	IsRelatedRelation       = "IS_RELATED_TO"
	SupersededByRelation    = "SUPERSEDED_BY"
	HasOrganisationRelation = "HAS_ORGANISATION"
	HasMemberRelation       = "HAS_MEMBER"

	HasMembershipRoleRelation = "HAS_ROLE"

	CountryOfRiskRelation          = "COUNTRY_OF_RISK"
	CountryOfIncorporationRelation = "COUNTRY_OF_INCORPORATION"
	CountryOfOperationsRelation    = "COUNTRY_OF_OPERATIONS"
	ParentOrganisationRelation     = "SUB_ORGANISATION_OF"

	IndustryClassificationRelation = "HAS_INDUSTRY_CLASSIFICATION"
)

// RelationshipConfig describes a single possible relationship in the ontology
type RelationshipConfig struct {
	// Neo4j setup
	NeoRelationship string // Neo4J Relationship type
	NeoShouldCreate bool   // if the starting Thing node does not exist create it
	// UPPConcept setup
	ConceptField string // UPP Concept field name
	SingleField  bool   // true if relation is serialized in a single string field
	SpecialField bool   // true if this field has special treatment handled in code

}

// GetRelationships returns the relationship descriptions and configuration allowed in the Knowledge graph
func GetRelationships() map[string]RelationshipConfig {
	return map[string]RelationshipConfig{
		BroaderRelation: {
			NeoRelationship: "HAS_BROADER",
			ConceptField:    "broaderUUIDs",
		},
		ParentRelation: {
			NeoRelationship: "HAS_PARENT",
			NeoShouldCreate: true,
			ConceptField:    "parentUUIDs",
		},
		ImpliedByRelation: {
			NeoRelationship: "IMPLIED_BY",
			ConceptField:    "impliedByUUIDs",
		},
		HasFocusRelation: {
			NeoRelationship: "HAS_FOCUS",
			ConceptField:    "hasFocusUUIDs",
		},
		IsRelatedRelation: {
			NeoRelationship: "IS_RELATED_TO",
			ConceptField:    "relatedUUIDs",
		},
		SupersededByRelation: {
			NeoRelationship: "SUPERSEDED_BY",
			ConceptField:    "supersededByUUIDs",
		},
		CountryOfRiskRelation: {
			NeoRelationship: "COUNTRY_OF_RISK",
			NeoShouldCreate: true,
			ConceptField:    "countryOfRiskUUID",
			SingleField:     true,
		},
		CountryOfIncorporationRelation: {
			NeoRelationship: "COUNTRY_OF_INCORPORATION",
			NeoShouldCreate: true,
			ConceptField:    "countryOfIncorporationUUID",
			SingleField:     true,
		},
		CountryOfOperationsRelation: {
			NeoRelationship: "COUNTRY_OF_OPERATIONS",
			NeoShouldCreate: true,
			ConceptField:    "countryOfOperationsUUID",
			SingleField:     true,
		},
		ParentOrganisationRelation: {
			NeoRelationship: "SUB_ORGANISATION_OF",
			NeoShouldCreate: true,
			ConceptField:    "parentOrganisation",
			SingleField:     true,
		},
		IndustryClassificationRelation: {
			NeoRelationship: "HAS_INDUSTRY_CLASSIFICATION",
			NeoShouldCreate: true,
			ConceptField:    "naicsIndustryClassifications",
			SpecialField:    true,
		},
		HasOrganisationRelation: {
			NeoRelationship: "HAS_ORGANISATION",
			NeoShouldCreate: true,
			ConceptField:    "organisationUUID",
			SingleField:     true,
		},
		HasMemberRelation: {
			NeoRelationship: "HAS_MEMBER",
			NeoShouldCreate: true,
			ConceptField:    "personUUID",
			SingleField:     true,
		},
		HasMembershipRoleRelation: {
			NeoRelationship: "HAS_ROLE",
			NeoShouldCreate: true,
			ConceptField:    "membershipRoles",
			SpecialField:    true,
		},
	}
}
