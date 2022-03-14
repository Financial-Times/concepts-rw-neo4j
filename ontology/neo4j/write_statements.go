package neo4j

import (
	"fmt"
	"time"

	cmneo4j "github.com/Financial-Times/cm-neo4j-driver"
	"github.com/Financial-Times/neo-model-utils-go/mapper"

	"github.com/Financial-Times/concepts-rw-neo4j/ontology"
)

// WriteCanonicalConceptQueries generate a list of neo4j queries that will write a canonical concept.
// When executed, the queries will create a concept node for every source concept and a single canonical node for the aggregated concept.
func WriteCanonicalConceptQueries(aggregatedConcept ontology.NewAggregatedConcept) []*cmneo4j.Query {
	var result []*cmneo4j.Query
	result = append(result, createCanonicalNodeQueries(aggregatedConcept, aggregatedConcept.PrefUUID)...)

	for _, sourceConcept := range aggregatedConcept.SourceRepresentations {
		result = append(result, WriteSourceQueries(sourceConcept)...)
		result = append(result, createEquivalentToQueries(sourceConcept, aggregatedConcept)...)

		for _, rel := range sourceConcept.Relationships {
			relCfg, ok := ontology.GetConfig().Relationships[rel.Label]
			if !ok {
				continue
			}

			result = append(result, createRelQuery(sourceConcept.UUID, rel, relCfg))
		}
	}

	return result
}

// WriteSourceQueries generates a set of neo4j queries that will create a single concept node
// When executed, the queries will create a single concept node with the approptiate properties and relations.
// To keep the model structure consistent avoid using this function. Use WriteCanonicalConceptQueries in stead.
func WriteSourceQueries(concept ontology.NewConcept) []*cmneo4j.Query {
	var queryBatch []*cmneo4j.Query
	var createConceptQuery *cmneo4j.Query

	allProps := setProps(concept)
	createConceptQuery = &cmneo4j.Query{
		Cypher: fmt.Sprintf(`MERGE (n:Thing {uuid: $uuid})
											set n=$allprops
											set n :%s`, getAllLabels(concept.Type)),
		Params: map[string]interface{}{
			"uuid":     concept.UUID,
			"allprops": allProps,
		},
	}

	if concept.IssuedBy != "" {
		// Issued By needs a specific handling. That is why it is not in the config
		// But we still want to use createRelQuery, so we create dummy relationship and config
		issuedByCfg := ontology.RelationshipConfig{
			ConceptField: "issuedBy",
			OneToOne:     true,
			NeoCreate:    true,
		}
		issuedByRel := ontology.Relationship{
			UUID:       concept.IssuedBy,
			Label:      "ISSUED_BY",
			Properties: nil,
		}
		queryBatch = append(queryBatch, createRelQuery(concept.UUID, issuedByRel, issuedByCfg))
	}

	queryBatch = append(queryBatch, createConceptQuery)
	return queryBatch
}

// WriteCanonicalForUnconcordedConcept generates a neo4j query that will create canonical node for the provided source concept
// The queries will not change the source concept.
// TODO: authors thoughts
// First. If we start storing all the concept properties in the source nodes sourceToCanonical can be substituted with CreateAggregateConcept
// This will ensure that we keep the data consistent. As of now we are required to republish those concepts.
// Second. The logic of this function can be expressed with other functions within this library.
// sourceToCanonical, setCanonicalProps, createCanonicalNodeQueries and createEquivalentToQueries.
// We don't do it because it change the functionality from one to multiple queries, which has implications down the line.
// Specifically how concurrency resource locks are acquired in Neo4j. If we want we can change it down the line.
func WriteCanonicalForUnconcordedConcept(concept ontology.NewConcept) *cmneo4j.Query {
	canonical := sourceToCanonical(concept)
	allProps := setCanonicalProps(canonical, concept.UUID)
	createCanonicalNodeQuery := &cmneo4j.Query{
		Cypher: fmt.Sprintf(`
					MATCH (t:Thing{uuid:$prefUUID})
					MERGE (n:Thing {prefUUID: $prefUUID})
					MERGE (n)<-[:EQUIVALENT_TO]-(t)
					set n=$allprops
					set n :%s`, getAllLabels(canonical.Type)),
		Params: map[string]interface{}{
			"prefUUID": concept.UUID,
			"allprops": allProps,
		},
	}
	return createCanonicalNodeQuery
}

// sourceToCanonical creates Aggregates Concept from single source concept
// TODO: This needs to use the aggregation code from aggy when it is extracted
func sourceToCanonical(source ontology.NewConcept) ontology.NewAggregatedConcept {
	var inceptionDate string
	var terminationDate string
	for _, r := range source.Relationships {
		if r.Label != "HAS_ROLE" {
			continue
		}
		if v, ok := r.Properties["inceptionDate"]; ok {
			if s, ok := v.(string); ok {
				inceptionDate = s
			}
		}
		if v, ok := r.Properties["terminationDate"]; ok {
			if s, ok := v.(string); ok {
				terminationDate = s
			}
		}
		break
	}
	return ontology.NewAggregatedConcept{
		AggregatedHash:  source.Hash,
		InceptionDate:   inceptionDate,
		IssuedBy:        source.IssuedBy,
		PrefLabel:       source.PrefLabel,
		TerminationDate: terminationDate,
		Type:            source.Type,
		IsDeprecated:    source.IsDeprecated,
	}
}

func createEquivalentToQueries(sourceConcept ontology.NewConcept, aggregatedConcept ontology.NewAggregatedConcept) []*cmneo4j.Query {
	var queryBatch []*cmneo4j.Query
	equivQuery := &cmneo4j.Query{
		Cypher: `MATCH (t:Thing {uuid:$uuid}), (c:Thing {prefUUID:$prefUUID})
						MERGE (t)-[:EQUIVALENT_TO]->(c)`,
		Params: map[string]interface{}{
			"uuid":     sourceConcept.UUID,
			"prefUUID": aggregatedConcept.PrefUUID,
		},
	}

	queryBatch = append(queryBatch, equivQuery)
	return queryBatch
}

func createCanonicalNodeQueries(canonical ontology.NewAggregatedConcept, prefUUID string) []*cmneo4j.Query {
	var queryBatch []*cmneo4j.Query
	var createConceptQuery *cmneo4j.Query

	allProps := setCanonicalProps(canonical, prefUUID)
	createConceptQuery = &cmneo4j.Query{
		Cypher: fmt.Sprintf(`MERGE (n:Thing {prefUUID: $prefUUID})
								set n=$allprops
								set n :%s`, getAllLabels(canonical.Type)),
		Params: map[string]interface{}{
			"prefUUID": prefUUID,
			"allprops": allProps,
		},
	}

	queryBatch = append(queryBatch, createConceptQuery)
	return queryBatch
}

// createRelQueries creates relationships Cypher queries for concepts
func createRelQuery(sourceUUID string, rel ontology.Relationship, cfg ontology.RelationshipConfig) *cmneo4j.Query {
	const createMissing = `
		MERGE (thing:Thing {uuid: $uuid})
		MERGE (other:Thing {uuid: $id})
		MERGE (thing)-[rel:%s]->(other)
	`

	const matchExisting = `
		MATCH (concept:Concept {uuid: $uuid})
		MERGE (other:Thing {uuid: $id})
		MERGE (concept)-[rel:%s]->(other)	
	`

	cypherStatement := matchExisting
	if cfg.NeoCreate {
		cypherStatement = createMissing
	}

	params := map[string]interface{}{
		"uuid": sourceUUID,
		"id":   rel.UUID,
	}
	if cfg.Properties != nil {
		cypherStatement += `	SET rel=$relProps`
		params["relProps"] = setupRelProps(rel, cfg)
	}

	return &cmneo4j.Query{
		Cypher: fmt.Sprintf(cypherStatement, rel.Label),
		Params: params,
	}
}

func setupRelProps(rel ontology.Relationship, cfg ontology.RelationshipConfig) map[string]interface{} {
	props := map[string]interface{}{}
	for label, t := range cfg.Properties {
		val := rel.Properties[label]
		props[label] = val
		if val != nil && t == ontology.PropertyTypeDate {
			str, ok := rel.Properties[label].(string)
			if !ok {
				continue
			}
			unixTime := getEpoch(str)
			// in the old times we skipped unix timestamps with valuse less or equal 0
			if unixTime <= 0 {
				continue
			}
			props[label+"Epoch"] = unixTime
		}
	}
	return props
}

func getEpoch(t string) int64 {
	const iso8601DateOnly = "2006-01-02"
	if t == "" {
		return 0
	}

	tt, err := time.Parse(iso8601DateOnly, t)
	if err != nil {
		return 0
	}
	unixTime := tt.Unix()
	if unixTime < 0 {
		return 0
	}
	return unixTime
}

//return all concept labels
func getAllLabels(conceptType string) string {
	labels := conceptType
	parentType := mapper.ParentType(conceptType)
	for parentType != "" {
		labels += ":" + parentType
		parentType = mapper.ParentType(parentType)
	}
	return labels
}

//This function dictates which properties will be actually
//written in neo for source nodes.
func setProps(source ontology.NewConcept) map[string]interface{} {
	nodeProps := map[string]interface{}{}
	nodeProps["lastModifiedEpoch"] = time.Now().Unix()

	if source.PrefLabel != "" {
		nodeProps["prefLabel"] = source.PrefLabel
	}

	if source.FigiCode != "" {
		nodeProps["figiCode"] = source.FigiCode
	}

	if source.IsDeprecated {
		nodeProps["isDeprecated"] = true
	}

	nodeProps["uuid"] = source.UUID
	nodeProps["authority"] = source.Authority
	nodeProps["authorityValue"] = source.AuthorityValue

	return nodeProps
}

//This function dictates which properties will be actually
//written in neo for canonical nodes.
func setCanonicalProps(canonical ontology.NewAggregatedConcept, prefUUID string) map[string]interface{} {
	nodeProps := map[string]interface{}{}

	ontologyCfg := ontology.GetConfig()
	for field, propCfg := range ontologyCfg.Fields {
		if val, ok := canonical.GetPropertyValue(field); ok {
			if !ontologyCfg.IsPropValueValid(field, val) {
				continue
			}

			nodeProps[propCfg.NeoProp] = val
		}
	}

	nodeProps["lastModifiedEpoch"] = time.Now().Unix()

	if canonical.PrefLabel != "" {
		nodeProps["prefLabel"] = canonical.PrefLabel
	}

	if canonical.FigiCode != "" {
		nodeProps["figiCode"] = canonical.FigiCode
	}

	if canonical.IsDeprecated {
		nodeProps["isDeprecated"] = true
	}

	nodeProps["prefUUID"] = prefUUID
	nodeProps["aggregateHash"] = canonical.AggregatedHash

	if canonical.InceptionDate != "" {
		nodeProps["inceptionDate"] = canonical.InceptionDate
	}
	if canonical.TerminationDate != "" {
		nodeProps["terminationDate"] = canonical.TerminationDate
	}

	return nodeProps
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

	return r
}
