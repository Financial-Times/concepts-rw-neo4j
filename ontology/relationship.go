package ontology

type RelationshipConfig struct {
	NeoRelationship string
	NeoShouldCreate bool // if the Thing node does not exist create it
}

func GetRelationships() map[string]RelationshipConfig {
	return map[string]RelationshipConfig{
		BroaderRelation: {
			NeoRelationship: "HAS_BROADER",
			NeoShouldCreate: false,
		},
		ParentRelation: {
			NeoRelationship: "HAS_PARENT",
			NeoShouldCreate: true,
		},
		ImpliedByRelation: {
			NeoRelationship: "IMPLIED_BY",
			NeoShouldCreate: false,
		},
		HasFocusRelation: {
			NeoRelationship: "HAS_FOCUS",
			NeoShouldCreate: false,
		},
		IsRelatedRelation: {
			NeoRelationship: "IS_RELATED_TO",
			NeoShouldCreate: false,
		},
		SupersededByRelation: {
			NeoRelationship: "SUPERSEDED_BY",
			NeoShouldCreate: false,
		},
		CountryOfRiskRelation: {
			NeoRelationship: "COUNTRY_OF_RISK",
			NeoShouldCreate: true,
		},
		CountryOfIncorporationRelation: {
			NeoRelationship: "COUNTRY_OF_INCORPORATION",
			NeoShouldCreate: true,
		},
		CountryOfOperationsRelation: {
			NeoRelationship: "COUNTRY_OF_OPERATIONS",
			NeoShouldCreate: true,
		},
		ParentOrganisationRelation: {
			NeoRelationship: "SUB_ORGANISATION_OF",
			NeoShouldCreate: true,
		},
		IndustryClassificationRelation: {
			NeoRelationship: "HAS_INDUSTRY_CLASSIFICATION",
			NeoShouldCreate: true,
		},
		HasOrganisationRelation: {
			NeoRelationship: "HAS_ORGANISATION",
			NeoShouldCreate: true,
		},
		HasMemberRelation: {
			NeoRelationship: "HAS_MEMBER",
			NeoShouldCreate: true,
		},
		HasMembershipRoleRelation: {
			NeoRelationship: "HAS_ROLE",
			NeoShouldCreate: true,
		},
	}
}
