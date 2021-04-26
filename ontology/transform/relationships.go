package transform

import "github.com/Financial-Times/concepts-rw-neo4j/ontology"

func UUIDsToRelationships(label string, uuids []string) ontology.Relationship {
	var connections []ontology.Connection
	for _, uuid := range uuids {
		if uuid == "" {
			continue
		}
		connections = append(connections, ontology.Connection{
			UUID: uuid,
		})
	}
	return ontology.Relationship{
		Label:       label,
		Connections: connections,
	}
}

func RelationshipsToUUIDs(relations []ontology.Relationship, label string) []string {
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

func RelationshipsToSingleUUID(relations []ontology.Relationship, label string) string {
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

////////////////////////////////////////////////////////////////////////
// Custom transformers /////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////

const naicsRankField = "rank"

func NAICSToRelationship(naics []NAICSIndustryClassification) ontology.Relationship {
	var connections []ontology.Connection
	for _, n := range naics {
		if n.UUID == "" {
			continue
		}
		connections = append(connections, ontology.Connection{
			UUID: n.UUID,
			Properties: map[string]interface{}{
				naicsRankField: n.Rank,
			},
		})
	}
	return ontology.Relationship{
		Label:       ontology.IndustryClassificationRelation,
		Connections: connections,
	}
}

func RelationshipsToNAICS(relations []ontology.Relationship) []NAICSIndustryClassification {
	var naics []NAICSIndustryClassification
	for _, rel := range relations {
		if rel.Label != ontology.IndustryClassificationRelation {
			continue
		}
		for _, con := range rel.Connections {
			rank := -1

			switch r := con.Properties[naicsRankField].(type) {
			case int:
				rank = r
			case float64:
				rank = int(r)
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

func MembershipRolesToRelationship(roles []MembershipRole) ontology.Relationship {
	var connections []ontology.Connection
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
		connections = append(connections, ontology.Connection{
			UUID: r.RoleUUID,
			Properties: map[string]interface{}{
				inceptionDateField:        r.InceptionDate,
				inceptionDateEpochField:   inceptionDateEpoch,
				terminationDateField:      r.TerminationDate,
				terminationDateEpochField: terminationDateEpoch,
			},
		})
	}
	return ontology.Relationship{
		Label:       ontology.HasMembershipRoleRelation,
		Connections: connections,
	}
}

func RelationshipsToMembershipRoles(relations []ontology.Relationship) []MembershipRole {
	var roles []MembershipRole
	for _, rel := range relations {
		if rel.Label != ontology.HasMembershipRoleRelation {
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
