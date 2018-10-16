package locations

import (
	"fmt"
	"github.com/Financial-Times/concepts-rw-neo4j/concepts"
	"github.com/Financial-Times/go-logger"
	"github.com/Financial-Times/neo-model-utils-go/mapper"
	"github.com/Financial-Times/neo-utils-go/neoutils"
	"github.com/jmcvetta/neoism"
	"github.com/mitchellh/hashstructure"
	"strconv"
)

type LocationService struct {
	conn neoutils.NeoConnection
}

func NewLocationService(db neoutils.NeoConnection) *LocationService {
	return &LocationService{db}
}

func (ls *LocationService) Write(thing interface{}, transID string) (interface{}, error) {
	// Read the aggregated concept - We need read the entire model first. This is because if we unconcord a TME concept
	// then we need to add prefUUID to the lone node if it has been removed from the concordance listed against a Smartlogic concept
	var updateRecord concepts.ConceptChanges
	var updatedUUIDList []string
	aggregatedConceptToWrite := thing.(concepts.AggregatedConcept)
	aggregatedConceptToWrite = concepts.CleanSourceProperties(aggregatedConceptToWrite)
	sourceUuidsAndTypes := concepts.GetUUIDAndTypeFromSources(aggregatedConceptToWrite.SourceRepresentations)
	payloadHash, err := hashstructure.Hash(aggregatedConceptToWrite, nil)
	if err != nil {
		logger.WithError(err).WithTransactionID(transID).WithUUID(aggregatedConceptToWrite.PrefUUID).Error("read request for existing concordance resulted in error")
		return updateRecord, err
	}
	hashAsString := strconv.FormatUint(payloadHash, 10)

	if err = concepts.ValidateBasicConcept(aggregatedConceptToWrite, transID); err != nil {
		return updateRecord, err
	}

	existingConcept, exists, err := ls.Read(aggregatedConceptToWrite.PrefUUID, transID)
	if err != nil {
		logger.WithError(err).WithTransactionID(transID).WithUUID(aggregatedConceptToWrite.PrefUUID).Error("read request for existing concordance resulted in error")
		return updateRecord, err
	}

	var queryBatch []*neoism.CypherQuery
	var prefUUIDsToBeDeletedQueryBatch []*neoism.CypherQuery
	if exists {
		existingAggregateConcept := existingConcept.(concepts.AggregatedConcept)
		existingSourceUuidsAndTypes := concepts.GetUUIDAndTypeFromSources(existingAggregateConcept.SourceRepresentations)

		//Concept has been updated since last write, so need to send notification of all affected ids
		for _, source := range aggregatedConceptToWrite.SourceRepresentations {
			updatedUUIDList = append(updatedUUIDList, source.UUID)
		}

		//This filter will leave us with ids that were members of existing concordance but are NOT members of current concordance
		//They will need a new prefUUID node written
		conceptsToUnconcord := concepts.FilterIdsThatAreUniqueToFirstMap(existingSourceUuidsAndTypes, sourceUuidsAndTypes)

		//This filter will leave us with ids that are members of current concordance payload but were not previously concorded to this concordance
		conceptsToTransferConcordance := concepts.FilterIdsThatAreUniqueToFirstMap(sourceUuidsAndTypes, existingSourceUuidsAndTypes)

		//Handle scenarios for transferring source id from an existing concordance to this concordance
		if len(conceptsToTransferConcordance) > 0 {
			prefUUIDsToBeDeletedQueryBatch, err = concepts.HandleTransferConcordance(conceptsToTransferConcordance, ls.conn, &updateRecord, hashAsString, aggregatedConceptToWrite.PrefUUID, transID)
			if err != nil {
				return updateRecord, err
			}

		}

		clearDownQuery := clearDownExistingNodes(aggregatedConceptToWrite)
		for _, query := range clearDownQuery {
			queryBatch = append(queryBatch, query)
		}

		for idToUnconcord := range conceptsToUnconcord {
			for _, concept := range existingAggregateConcept.SourceRepresentations {
				if idToUnconcord == concept.UUID {
					unconcordQuery := concepts.WriteCanonicalNodeForUnconcordedConcepts(concept)
					queryBatch = append(queryBatch, unconcordQuery)

					//We will need to send a notification of ids that have been removed from current concordance
					updatedUUIDList = append(updatedUUIDList, idToUnconcord)

					//Unconcordance event for new concept notifications
					updateRecord.ChangedRecords = append(updateRecord.ChangedRecords, concepts.Event{
						ConceptType:   conceptsToUnconcord[idToUnconcord],
						ConceptUUID:   idToUnconcord,
						AggregateHash: hashAsString,
						TransactionID: transID,
						EventDetails: concepts.ConcordanceEvent{
							Type:  concepts.RemovedEvent,
							OldID: aggregatedConceptToWrite.PrefUUID,
							NewID: idToUnconcord,
						},
					})
				}
			}
		}
	} else {
		var conceptsToCheckForExistingConcordance []string
		for _, sr := range aggregatedConceptToWrite.SourceRepresentations {
			conceptsToCheckForExistingConcordance = append(conceptsToCheckForExistingConcordance, sr.UUID)
		}

		prefUUIDsToBeDeletedQueryBatch, err = concepts.HandleTransferConcordance(sourceUuidsAndTypes, ls.conn, &updateRecord, hashAsString, aggregatedConceptToWrite.PrefUUID, transID)
		if err != nil {
			return updateRecord, err
		}

		clearDownQuery := clearDownExistingNodes(aggregatedConceptToWrite)
		for _, query := range clearDownQuery {
			queryBatch = append(queryBatch, query)
		}

		//Concept is new, send notification of all source ids
		for _, source := range aggregatedConceptToWrite.SourceRepresentations {
			updatedUUIDList = append(updatedUUIDList, source.UUID)
		}
	}

	queryBatch = populateConceptQueries(queryBatch, aggregatedConceptToWrite)
	for _, query := range prefUUIDsToBeDeletedQueryBatch {
		queryBatch = append(queryBatch, query)
	}

	updateRecord.UpdatedIds = updatedUUIDList
	updateRecord.ChangedRecords = append(updateRecord.ChangedRecords, concepts.Event{
		ConceptType:   aggregatedConceptToWrite.Type,
		ConceptUUID:   aggregatedConceptToWrite.PrefUUID,
		AggregateHash: hashAsString,
		TransactionID: transID,
		EventDetails: concepts.ConceptEvent{
			Type: concepts.UpdatedEvent,
		},
	})

	logger.WithTransactionID(transID).WithUUID(aggregatedConceptToWrite.PrefUUID).Debug("Executing " + strconv.Itoa(len(queryBatch)) + " queries")
	for _, query := range queryBatch {
		logger.WithTransactionID(transID).WithUUID(aggregatedConceptToWrite.PrefUUID).Debug(fmt.Sprintf("Query: %v", query))
	}

	if err = ls.conn.CypherBatch(queryBatch); err != nil {
		logger.WithError(err).WithTransactionID(transID).WithUUID(aggregatedConceptToWrite.PrefUUID).Error("Error executing neo4j write queries. Concept NOT written.")
		return updateRecord, err
	}

	logger.WithTransactionID(transID).WithUUID(aggregatedConceptToWrite.PrefUUID).Info("Concept written to db")
	return updateRecord, nil
}

func clearDownExistingNodes(ac concepts.AggregatedConcept) []*neoism.CypherQuery {
	var queryBatch []*neoism.CypherQuery

	for _, sr := range ac.SourceRepresentations {
		deletePreviousSourceIdentifiersLabelsAndPropertiesQuery := &neoism.CypherQuery{
			Statement: fmt.Sprintf(`MATCH (t:Thing {uuid:{id}})
			OPTIONAL MATCH (t)<-[iden:IDENTIFIES]-(i)
			OPTIONAL MATCH (t)-[eq:EQUIVALENT_TO]->(a:Thing)
			OPTIONAL MATCH (t)-[brd:HAS_BROADER]->()
			OPTIONAL MATCH (t)-[rel:IS_RELATED_TO]->()
			OPTIONAL MATCH (t)-[sup:SUPERSEDED_BY]->()
			REMOVE t:%s
			SET t={uuid:{id}}
			DELETE iden, i, eq, brd, rel, sup`, concepts.GetLabelsToRemove()),
			Parameters: map[string]interface{}{
				"id": sr.UUID,
			},
		}
		queryBatch = append(queryBatch, deletePreviousSourceIdentifiersLabelsAndPropertiesQuery)
	}

	//cleanUP all the previous Equivalent to relationships
	deletePreviousCanonicalIdentifiersLabelsAndPropertiesQuery := &neoism.CypherQuery{
		Statement: fmt.Sprintf(`MATCH (t:Thing {prefUUID:{acUUID}})
			OPTIONAL MATCH (t)<-[rel:EQUIVALENT_TO]-(s)
			REMOVE t:%s
			SET t={prefUUID:{acUUID}}
			DELETE rel`, concepts.GetLabelsToRemove()),
		Parameters: map[string]interface{}{
			"acUUID": ac.PrefUUID,
		},
	}
	queryBatch = append(queryBatch, deletePreviousCanonicalIdentifiersLabelsAndPropertiesQuery)

	return queryBatch
}

//Curate all queries to populate concept nodes
func populateConceptQueries(queryBatch []*neoism.CypherQuery, aggregatedConcept concepts.AggregatedConcept) []*neoism.CypherQuery {
	// Create a sourceConcept from the canonical information - WITH NO Uuid
	concept := concepts.Concept{
		PrefLabel:    aggregatedConcept.PrefLabel,
		Type:         aggregatedConcept.Type,
		Aliases:      aggregatedConcept.Aliases,
		ScopeNote:    aggregatedConcept.ScopeNote,
		IsDeprecated: aggregatedConcept.IsDeprecated,
	}

	queryBatch = append(queryBatch, concepts.CreateNodeQueries(concept, aggregatedConcept.PrefUUID, "")...)

	// Repopulate
	for _, sourceConcept := range aggregatedConcept.SourceRepresentations {
		queryBatch = append(queryBatch, concepts.CreateNodeQueries(sourceConcept, "", sourceConcept.UUID)...)

		equivQuery := &neoism.CypherQuery{
			Statement: `MATCH (t:Thing {uuid:{uuid}}), (c:Thing {prefUUID:{prefUUID}})
						MERGE (t)-[:EQUIVALENT_TO]->(c)`,
			Parameters: map[string]interface{}{
				"uuid":     sourceConcept.UUID,
				"prefUUID": aggregatedConcept.PrefUUID,
			},
		}
		queryBatch = append(queryBatch, equivQuery)

		if len(sourceConcept.BroaderUUIDs) > 0 {
			queryBatch = concepts.AddRelationship(sourceConcept.UUID, sourceConcept.BroaderUUIDs, "HAS_BROADER", queryBatch)
		}

		if len(sourceConcept.RelatedUUIDs) > 0 {
			queryBatch = concepts.AddRelationship(sourceConcept.UUID, sourceConcept.RelatedUUIDs, "IS_RELATED_TO", queryBatch)
		}

		if len(sourceConcept.SupersededByUUIDs) > 0 {
			queryBatch = concepts.AddRelationship(sourceConcept.UUID, sourceConcept.SupersededByUUIDs, "SUPERSEDED_BY", queryBatch)
		}
	}
	return queryBatch
}

func (ls *LocationService) Read(uuid string, transID string) (interface{}, bool, error) {
	var results []concepts.NeoAggregatedConcept

	query := &neoism.CypherQuery{
		Statement: `
			MATCH (canonical:Thing {prefUUID:{uuid}})<-[:EQUIVALENT_TO]-(source:Thing)
			OPTIONAL MATCH (source)-[:HAS_BROADER]->(broader:Thing)
			OPTIONAL MATCH (source)-[:IS_RELATED_TO]->(related:Thing)
			OPTIONAL MATCH (source)-[:SUPERSEDED_BY]->(superseded:Thing)
			WITH
				canonical,
				broader,
				related,
				superseded,
				source
				ORDER BY
					source.uuid
			WITH
				canonical,
				{
					authority: source.authority,
					authorityValue: source.authorityValue,
					lastModifiedEpoch: source.lastModifiedEpoch,
					prefLabel: source.prefLabel,
					broaderUUIDs: collect(DISTINCT broader.uuid),
					relatedUUIDs: collect(DISTINCT related.uuid),
					supersededByUUIDs: collect(DISTINCT superseded.uuid),
					types: labels(source),
					uuid: source.uuid,
					isDeprecated: source.isDeprecated
				} as sources
			RETURN
				canonical.aliases as aliases,
				canonical.prefLabel as prefLabel,
				canonical.prefUUID as prefUUID,
				canonical.scopeNote as scopeNote,
				labels(canonical) as types,
				collect(sources) as sourceRepresentations,
				canonical.isDeprecated as isDeprecated
			`,
		Parameters: map[string]interface{}{
			"uuid": uuid,
		},
		Result: &results,
	}

	err := ls.conn.CypherBatch([]*neoism.CypherQuery{query})
	if err != nil {
		logger.WithError(err).WithTransactionID(transID).WithUUID(uuid).Error("error executing neo4j read query")
		return concepts.AggregatedConcept{}, false, err
	}

	if len(results) == 0 {
		logger.WithTransactionID(transID).WithUUID(uuid).Info("concept not found in db")
		return concepts.AggregatedConcept{}, false, nil
	}
	typeName, err := mapper.MostSpecificType(results[0].Types)
	if err != nil {
		logger.WithError(err).WithTransactionID(transID).WithUUID(uuid).Error("returned concept had no recognized type")
		return concepts.AggregatedConcept{}, false, err
	}

	aggregatedConcept := concepts.AggregatedConcept{
		Aliases:      results[0].Aliases,
		PrefLabel:    results[0].PrefLabel,
		PrefUUID:     results[0].PrefUUID,
		ScopeNote:    results[0].ScopeNote,
		Type:         typeName,
		IsDeprecated: results[0].IsDeprecated,
	}

	var sourceConcepts []concepts.Concept
	for _, srcConcept := range results[0].SourceRepresentations {
		sourceType, err := mapper.MostSpecificType(srcConcept.Types)
		if err != nil {
			logger.WithError(err).WithTransactionID(transID).WithUUID(uuid).Error("returned source concept had no recognized type")
			return concepts.AggregatedConcept{}, false, err
		}

		concept := concepts.Concept{
			UUID:              srcConcept.UUID,
			PrefLabel:         srcConcept.PrefLabel,
			Type:              sourceType,
			Authority:         srcConcept.Authority,
			AuthorityValue:    srcConcept.AuthorityValue,
			BroaderUUIDs:      concepts.FilterSlice(srcConcept.BroaderUUIDs),
			RelatedUUIDs:      concepts.FilterSlice(srcConcept.RelatedUUIDs),
			SupersededByUUIDs: concepts.FilterSlice(srcConcept.SupersededUUIDs),
			LastModifiedEpoch: srcConcept.LastModifiedEpoch,
			IsDeprecated:      srcConcept.IsDeprecated,
		}
		sourceConcepts = append(sourceConcepts, concept)
	}

	aggregatedConcept.SourceRepresentations = sourceConcepts
	logger.WithTransactionID(transID).WithUUID(uuid).Debugf("returned concept is %v", aggregatedConcept)
	return concepts.CleanConcept(aggregatedConcept), true, nil
}
