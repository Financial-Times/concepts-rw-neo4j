package transform

import (
	"errors"

	"github.com/Financial-Times/concepts-rw-neo4j/ontology"
	"github.com/jmcvetta/neoism"
)

type NeoConcept struct {
	Types      []string               `json:"types"`
	Properties map[string]interface{} `json:"properties"`
	Relations  []struct {
		UUID       string                 `json:"uuid"`
		Type       string                 `json:"type"`
		Properties map[string]interface{} `json:"properties"`
	} `json:"relations"`
}

type NeoAggregatedConcept struct {
	Types      []string               `json:"types"`
	Properties map[string]interface{} `json:"properties"`
	Sources    []NeoConcept           `json:"sources"`
}

func TransformToAggregateConcept(c NeoAggregatedConcept) (ontology.NewAggregatedConcept, error) {
	typeName, err := ontology.MostSpecificType(c.Types)
	if err != nil {
		return ontology.NewAggregatedConcept{}, err
	}
	prefUUID, ok := extractStringFromProperties(c.Properties, "prefUUID")
	if !ok {
		return ontology.NewAggregatedConcept{}, errors.New("neo aggregate concept doesn't have prefUUID property")
	}
	delete(c.Properties, "prefUUID")

	aggregateHash, ok := extractStringFromProperties(c.Properties, "aggregateHash")
	if !ok {
		return ontology.NewAggregatedConcept{}, errors.New("neo aggregate concept doesn't have aggregateHash property")
	}
	delete(c.Properties, "aggregateHash")

	var sourceConcepts []ontology.NewSourceConcept
	for _, srcConcept := range c.Sources {
		source, err := TransformToSourceConcept(srcConcept)
		if err != nil {
			return ontology.NewAggregatedConcept{}, err
		}
		sourceConcepts = append(sourceConcepts, source)
	}
	issuedByUUID := ""
	for _, source := range sourceConcepts {
		if source.IssuedBy != "" {
			issuedByUUID = source.IssuedBy
		}
	}

	return ontology.NewAggregatedConcept{
		GenericConcept: ontology.GenericConcept{
			Properties: c.Properties,
		},
		AggregatedHash:        aggregateHash,
		IssuedBy:              issuedByUUID,
		PrefUUID:              prefUUID,
		SourceRepresentations: sourceConcepts,
		Type:                  typeName,
	}, nil
}

func TransformToSourceConcept(c NeoConcept) (ontology.NewSourceConcept, error) {
	conceptType, err := ontology.MostSpecificType(c.Types)
	if err != nil {
		return ontology.NewSourceConcept{}, err
	}

	issuedByUUID := ""
	var relations []ontology.Relationship
	connections := map[string][]ontology.Connection{}
	for _, neoRelation := range c.Relations {
		if neoRelation.Type == "" {
			continue
		}
		// special cases
		if neoRelation.Type == "ISSUED_BY" {
			issuedByUUID = neoRelation.UUID
			continue
		}
		connections[neoRelation.Type] = append(connections[neoRelation.Type], ontology.Connection{
			UUID:       neoRelation.UUID,
			Properties: neoRelation.Properties,
		})
	}
	for label, con := range connections {
		relations = append(relations, ontology.Relationship{
			Label:       label,
			Connections: con,
		})
	}

	uuid, ok := extractStringFromProperties(c.Properties, "uuid")
	if !ok {
		return ontology.NewSourceConcept{}, errors.New("neo concept doesn't have uuid property")
	}
	// buggy when reading the value is float64 not int
	lastModified, _ := extractIntFromProperties(c.Properties, "lastModifiedEpoch")

	delete(c.Properties, "prefUUID")
	return ontology.NewSourceConcept{
		GenericConcept: ontology.GenericConcept{
			Properties: c.Properties,
			Relations:  relations,
		},
		IssuedBy:          issuedByUUID,
		LastModifiedEpoch: lastModified,
		Type:              conceptType,
		UUID:              uuid,
	}, nil
}

// GetNeoConceptReadQuery returns simplified version of the concept read query
// It has the potential to read even relations and properties not defined in the ontology or the old format
func GetNeoConceptReadQuery(uuid string, results *[]NeoAggregatedConcept) *neoism.CypherQuery {
	return &neoism.CypherQuery{
		Statement: `
MATCH (c:Concept{prefUUID:{uuid}})<-[:EQUIVALENT_TO]-(source)
OPTIONAL MATCH (source)-[r]->(other:Thing) WHERE NOT TYPE(r) = "EQUIVALENT_TO"
WITH COLLECT(DISTINCT{
		type: TYPE(r),
		properties: PROPERTIES(r),
		uuid: other.uuid
	}) AS relations,
	PROPERTIES(source) AS source_properties,
	LABELS(source) AS source_types,
	PROPERTIES(c) AS properties,
	LABELS(c) AS types
WITH COLLECT({
	types: source_types,
	properties: source_properties,
	relations: relations
}) AS sources,
types,
properties
RETURN types, properties, sources
			`,
		Parameters: map[string]interface{}{
			"uuid": uuid,
		},
		Result: results,
	}
}

func extractStringFromProperties(properties map[string]interface{}, label string) (string, bool) {
	if val, has := properties[label]; has {
		if str, is := val.(string); is {
			return str, true
		}
	}
	return "", false
}

func extractIntFromProperties(properties map[string]interface{}, label string) (int, bool) {
	if val, has := properties[label]; has {
		if i, is := val.(int); is {
			return i, true
		}
	}
	return 0, false
}
