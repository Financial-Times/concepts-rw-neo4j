package concepts

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/Financial-Times/concepts-rw-neo4j/ontology"
	"github.com/Financial-Times/concepts-rw-neo4j/ontology/transform"
	"github.com/Financial-Times/go-logger"
	"github.com/Financial-Times/neo-model-utils-go/mapper"
	"github.com/Financial-Times/neo-utils-go/neoutils"
	"github.com/jmcvetta/neoism"
	"github.com/mitchellh/hashstructure"
)

const (
	iso8601DateOnly = "2006-01-02"
	//Event types
	UpdatedEvent = "CONCEPT_UPDATED"
	AddedEvent   = "CONCORDANCE_ADDED"
	RemovedEvent = "CONCORDANCE_REMOVED"
)

var concordancesSources = []string{"ManagedLocation", "Smartlogic"}

// ConceptService - CypherDriver - CypherDriver
type ConceptService struct {
	conn neoutils.NeoConnection
}

// ConceptServicer defines the functions any read-write application needs to implement
type ConceptServicer interface {
	Write(thing interface{}, transID string) (updatedIds interface{}, err error)
	Read(uuid string, transID string) (thing interface{}, found bool, err error)
	DecodeJSON(*json.Decoder) (thing interface{}, identity string, err error)
	Check() error
	Initialise() error
}

// NewConceptService instantiate driver
func NewConceptService(cypherRunner neoutils.NeoConnection) ConceptService {
	return ConceptService{cypherRunner}
}

// Initialise - Would this be better as an extension in Neo4j? i.e. that any Thing has this constraint added on creation
func (s *ConceptService) Initialise() error {
	err := s.conn.EnsureIndexes(map[string]string{
		"Concept": "leiCode",
	})
	if err != nil {
		logger.WithError(err).Error("Could not run db index")
		return err
	}

	err = s.conn.EnsureIndexes(map[string]string{
		"Thing":   "authorityValue",
		"Concept": "authorityValue",
	})
	if err != nil {
		logger.WithError(err).Error("Could not run DB constraints")
		return err
	}

	err = s.conn.EnsureConstraints(map[string]string{
		"Thing":                       "prefUUID",
		"Concept":                     "prefUUID",
		"Location":                    "iso31661",
		"NAICSIndustryClassification": "industryIdentifier",
	})
	if err != nil {
		logger.WithError(err).Error("Could not run db constraints")
		return err
	}
	return s.conn.EnsureConstraints(constraintMap)
}

type equivalenceResult struct {
	SourceUUID  string   `json:"sourceUuid"`
	PrefUUID    string   `json:"prefUuid"`
	Types       []string `json:"types"`
	Equivalence int      `json:"count"`
	Authority   string   `json:"authority"`
}

//Read - read service
func (s *ConceptService) Read(uuid string, transID string) (interface{}, bool, error) {
	concept, exist, err := s.readNew(uuid, transID)
	if err != nil {
		return transform.AggregatedConcept{}, false, err
	}
	if !exist {
		return transform.AggregatedConcept{}, false, nil
	}
	aggregateConcept := transform.TransformToOldAggregateConcept(concept)
	logger.WithTransactionID(transID).WithUUID(uuid).Debugf("Returned concept is %v", aggregateConcept)
	return cleanConcept(aggregateConcept), true, nil
}

func (s *ConceptService) readNew(uuid string, transID string) (ontology.NewAggregatedConcept, bool, error) {
	var results []transform.NeoAggregatedConcept

	query := transform.GetNeoConceptReadQuery(uuid, &results)

	err := s.conn.CypherBatch([]*neoism.CypherQuery{query})
	if err != nil {
		logger.WithError(err).WithTransactionID(transID).WithUUID(uuid).Error("Error executing neo4j read query")
		return ontology.NewAggregatedConcept{}, false, err
	}

	if len(results) == 0 {
		logger.WithTransactionID(transID).WithUUID(uuid).Info("Concept not found in db")
		return ontology.NewAggregatedConcept{}, false, nil
	}

	newAggregatedConcept, err := transform.TransformToAggregateConcept(results[0])
	if err != nil {
		logger.WithError(err).WithTransactionID(transID).WithUUID(uuid).Error("Returned concept had no recognized type")
		return ontology.NewAggregatedConcept{}, false, err
	}
	return newAggregatedConcept, true, nil
}

func (s *ConceptService) Write(thing interface{}, transID string) (interface{}, error) {
	// Read the aggregated concept - We need read the entire model first. This is because if we unconcord a TME concept
	// then we need to add prefUUID to the lone node if it has been removed from the concordance listed against a Smartlogic concept
	updateRecord := ConceptChanges{}
	var updatedUUIDList []string

	aggregatedConceptToWrite := transform.TransformToNewAggregateConcept(thing.(transform.AggregatedConcept))

	aggregatedConceptToWrite = cleanSourceProperties(aggregatedConceptToWrite)
	requestSourceData := getSourceData(aggregatedConceptToWrite.SourceRepresentations)

	requestHash, err := hashstructure.Hash(aggregatedConceptToWrite, nil)
	if err != nil {
		logger.WithError(err).WithTransactionID(transID).WithUUID(aggregatedConceptToWrite.PrefUUID).Error("Error hashing json from request")
		return updateRecord, err
	}

	hashAsString := strconv.FormatUint(requestHash, 10)

	if err = validateObject(aggregatedConceptToWrite, transID); err != nil {
		return updateRecord, err
	}

	existingAggregateConcept, exists, err := s.readNew(aggregatedConceptToWrite.PrefUUID, transID)
	if err != nil {
		logger.WithError(err).WithTransactionID(transID).WithUUID(aggregatedConceptToWrite.PrefUUID).Error("Read request for existing concordance resulted in error")
		return updateRecord, err
	}

	var queryBatch []*neoism.CypherQuery
	var prefUUIDsToBeDeletedQueryBatch []*neoism.CypherQuery
	if exists {
		if existingAggregateConcept.AggregatedHash == "" {
			existingAggregateConcept.AggregatedHash = "0"
		}
		currentHash, err := strconv.ParseUint(existingAggregateConcept.AggregatedHash, 10, 64)
		if err != nil {
			logger.WithError(err).WithTransactionID(transID).WithUUID(aggregatedConceptToWrite.PrefUUID).Info("Error whilst parsing existing concept hash")
			return updateRecord, nil
		}
		logger.WithTransactionID(transID).WithUUID(aggregatedConceptToWrite.PrefUUID).Debugf("Currently stored concept has hash of %d", currentHash)
		logger.WithTransactionID(transID).WithUUID(aggregatedConceptToWrite.PrefUUID).Debugf("Aggregated concept has hash of %d", requestHash)
		if currentHash == requestHash {
			logger.WithTransactionID(transID).WithUUID(aggregatedConceptToWrite.PrefUUID).Info("This concept has not changed since most recent update")
			return updateRecord, nil
		}
		logger.WithTransactionID(transID).WithUUID(aggregatedConceptToWrite.PrefUUID).Info("This concept is different to record stored in db, updating...")

		existingSourceData := getSourceData(existingAggregateConcept.SourceRepresentations)

		//Concept has been updated since last write, so need to send notification of all affected ids
		for _, source := range aggregatedConceptToWrite.SourceRepresentations {
			updatedUUIDList = append(updatedUUIDList, source.UUID)
		}

		//This filter will leave us with ids that were members of existing concordance but are NOT members of current concordance
		//They will need a new prefUUID node written
		conceptsToUnconcord := filterIdsThatAreUniqueToFirstMap(existingSourceData, requestSourceData)

		//This filter will leave us with ids that are members of current concordance payload but were not previously concorded to this concordance
		conceptsToTransferConcordance := filterIdsThatAreUniqueToFirstMap(requestSourceData, existingSourceData)

		//Handle scenarios for transferring source id from an existing concordance to this concordance
		if len(conceptsToTransferConcordance) > 0 {
			prefUUIDsToBeDeletedQueryBatch, err = s.handleTransferConcordance(conceptsToTransferConcordance, &updateRecord, hashAsString, aggregatedConceptToWrite, transID)
			if err != nil {
				return updateRecord, err
			}

		}

		clearDownQuery := s.clearDownExistingNodes(aggregatedConceptToWrite)
		for _, query := range clearDownQuery {
			queryBatch = append(queryBatch, query)
		}

		for idToUnconcord := range conceptsToUnconcord {
			for _, concept := range existingAggregateConcept.SourceRepresentations {
				if idToUnconcord == concept.UUID {
					//aggConcept := buildAggregateConcept(concept)
					//set this to 0 as otherwise it is empty
					//TODO fix this up at some point to do it properly?
					concept.Hash = "0"
					unconcordQuery := s.writeCanonicalNodeForUnconcordedConcepts(concept)
					queryBatch = append(queryBatch, unconcordQuery)

					//We will need to send a notification of ids that have been removed from current concordance
					updatedUUIDList = append(updatedUUIDList, idToUnconcord)

					//Unconcordance event for new concept notifications
					updateRecord.ChangedRecords = append(updateRecord.ChangedRecords, Event{
						ConceptType:   conceptsToUnconcord[idToUnconcord],
						ConceptUUID:   idToUnconcord,
						AggregateHash: hashAsString,
						TransactionID: transID,
						EventDetails: ConcordanceEvent{
							Type:  RemovedEvent,
							OldID: aggregatedConceptToWrite.PrefUUID,
							NewID: idToUnconcord,
						},
					})
				}
			}
		}
	} else {
		prefUUIDsToBeDeletedQueryBatch, err = s.handleTransferConcordance(requestSourceData, &updateRecord, hashAsString, aggregatedConceptToWrite, transID)
		if err != nil {
			return updateRecord, err
		}

		clearDownQuery := s.clearDownExistingNodes(aggregatedConceptToWrite)
		for _, query := range clearDownQuery {
			queryBatch = append(queryBatch, query)
		}

		//Concept is new, send notification of all source ids
		for _, source := range aggregatedConceptToWrite.SourceRepresentations {
			updatedUUIDList = append(updatedUUIDList, source.UUID)
		}
	}

	for _, query := range prefUUIDsToBeDeletedQueryBatch {
		queryBatch = append(queryBatch, query)
	}
	aggregatedConceptToWrite.AggregatedHash = hashAsString
	queryBatch = populateConceptQueries(queryBatch, aggregatedConceptToWrite)

	updateRecord.UpdatedIds = updatedUUIDList
	updateRecord.ChangedRecords = append(updateRecord.ChangedRecords, Event{
		ConceptType:   aggregatedConceptToWrite.Type,
		ConceptUUID:   aggregatedConceptToWrite.PrefUUID,
		AggregateHash: hashAsString,
		TransactionID: transID,
		EventDetails: ConceptEvent{
			Type: UpdatedEvent,
		},
	})

	logger.WithTransactionID(transID).WithUUID(aggregatedConceptToWrite.PrefUUID).Debug("Executing " + strconv.Itoa(len(queryBatch)) + " queries")
	for _, query := range queryBatch {
		logger.WithTransactionID(transID).WithUUID(aggregatedConceptToWrite.PrefUUID).Debug(fmt.Sprintf("Query: %v", query))
	}

	// check that the issuer is not already related to a different org
	if aggregatedConceptToWrite.IssuedBy != "" {
		var fiRes []map[string]string
		issuerQuery := &neoism.CypherQuery{
			Statement: `
					MATCH (issuer:Thing {uuid: {issuerUUID}})<-[:ISSUED_BY]-(fi)
					RETURN fi.uuid AS fiUUID
				`,
			Parameters: map[string]interface{}{
				"issuerUUID": aggregatedConceptToWrite.IssuedBy,
			},
			Result: &fiRes,
		}
		if err := s.conn.CypherBatch([]*neoism.CypherQuery{issuerQuery}); err != nil {
			logger.WithError(err).
				WithTransactionID(transID).
				WithUUID(aggregatedConceptToWrite.PrefUUID).
				Error("Could not get existing issuer.")
			return updateRecord, err
		}

		if len(fiRes) > 0 {
			for _, fi := range fiRes {
				fiUUID, ok := fi["fiUUID"]
				if !ok {
					continue
				}

				if fiUUID == aggregatedConceptToWrite.PrefUUID {
					continue
				}

				msg := fmt.Sprintf(
					"Issuer for %s was changed from %s to %s",
					aggregatedConceptToWrite.IssuedBy,
					fiUUID,
					aggregatedConceptToWrite.PrefUUID,
				)
				logger.WithTransactionID(transID).
					WithUUID(aggregatedConceptToWrite.PrefUUID).
					WithField("alert_tag", "ConceptLoadingLedToDifferentIssuer").Info(msg)

				deleteIssuerRelations := &neoism.CypherQuery{
					Statement: `
					MATCH (issuer:Thing {uuid: {issuerUUID}})
					MATCH (fi:Thing {uuid: {fiUUID}})
					MATCH (issuer)<-[issuerRel:ISSUED_BY]-(fi)
					DELETE issuerRel
				`,
					Parameters: map[string]interface{}{
						"issuerUUID": aggregatedConceptToWrite.IssuedBy,
						"fiUUID":     fiUUID,
					},
				}
				queryBatch = append(queryBatch, deleteIssuerRelations)
			}
		}
	}

	if err = s.conn.CypherBatch(queryBatch); err != nil {
		logger.WithError(err).WithTransactionID(transID).WithUUID(aggregatedConceptToWrite.PrefUUID).Error("Error executing neo4j write queries. Concept NOT written.")
		return updateRecord, err
	}

	logger.WithTransactionID(transID).WithUUID(aggregatedConceptToWrite.PrefUUID).Info("Concept written to db")
	return updateRecord, nil
}

func validateObject(aggConcept ontology.NewAggregatedConcept, transID string) error {

	prefLabel, _ := aggConcept.GetPropString(ontology.PrefLabelProp)
	if prefLabel == "" {
		return requestError{formatError("prefLabel", aggConcept.PrefUUID, transID)}
	}
	if _, ok := constraintMap[aggConcept.Type]; !ok {
		return requestError{formatError("type", aggConcept.PrefUUID, transID)}
	}
	if aggConcept.SourceRepresentations == nil {
		return requestError{formatError("sourceRepresentation", aggConcept.PrefUUID, transID)}
	}
	for _, concept := range aggConcept.SourceRepresentations {
		authority, _ := concept.GetPropString(ontology.AuthorityProp)
		if authority == "" {
			return requestError{formatError("sourceRepresentation.authority", concept.UUID, transID)}
		}
		if !stringInArr(authority, authorities) {
			logger.WithTransactionID(transID).WithUUID(aggConcept.PrefUUID).Debugf("Unknown authority supplied in the request: %s", authority)
		}
		if concept.Type == "" {
			return requestError{formatError("sourceRepresentation.type", concept.UUID, transID)}
		}
		authorityValue, _ := concept.GetPropString(ontology.AuthorityProp)
		if authorityValue == "" {
			return requestError{formatError("sourceRepresentation.authorityValue", concept.UUID, transID)}
		}
		if _, ok := constraintMap[concept.Type]; !ok {
			return requestError{formatError("type", aggConcept.PrefUUID, transID)}
		}
	}
	return nil
}

func formatError(field string, uuid string, transID string) string {
	err := errors.New("Invalid request, no " + field + " has been supplied")
	logger.WithError(err).WithTransactionID(transID).WithUUID(uuid).Error("Validation of payload failed")
	return err.Error()
}

func filterIdsThatAreUniqueToFirstMap(firstMapConcepts map[string]string, secondMapConcepts map[string]string) map[string]string {
	//Loop through both lists to find id which is present in first list but not in the second
	filteredMap := make(map[string]string)

	for conceptID := range firstMapConcepts {
		if _, ok := secondMapConcepts[conceptID]; !ok {
			filteredMap[conceptID] = firstMapConcepts[conceptID]
		}
	}
	return filteredMap
}

//Handle new source nodes that have been added to current concordance
func (s *ConceptService) handleTransferConcordance(conceptData map[string]string, updateRecord *ConceptChanges, aggregateHash string, newAggregatedConcept ontology.NewAggregatedConcept, transID string) ([]*neoism.CypherQuery, error) {
	var result []equivalenceResult
	var deleteLonePrefUUIDQueries []*neoism.CypherQuery

	for updatedSourceID := range conceptData {
		equivQuery := &neoism.CypherQuery{
			Statement: `
					MATCH (t:Thing {uuid:{id}})
					OPTIONAL MATCH (t)-[:EQUIVALENT_TO]->(c)
					OPTIONAL MATCH (c)<-[eq:EQUIVALENT_TO]-(x:Thing)
					RETURN t.uuid as sourceUuid, labels(t) as types, c.prefUUID as prefUuid, t.authority as authority, COUNT(DISTINCT eq) as count`,
			Parameters: map[string]interface{}{
				"id": updatedSourceID,
			},
			Result: &result,
		}
		err := s.conn.CypherBatch([]*neoism.CypherQuery{equivQuery})
		if err != nil {
			logger.WithError(err).WithTransactionID(transID).WithUUID(newAggregatedConcept.PrefUUID).Error("Requests for source nodes canonical information resulted in error")
			return deleteLonePrefUUIDQueries, err
		}

		//source node does not currently exist in neo4j, nothing to tidy up
		if len(result) == 0 {
			logger.WithTransactionID(transID).WithUUID(newAggregatedConcept.PrefUUID).Info("No existing concordance record found")
			if updatedSourceID != newAggregatedConcept.PrefUUID {
				//concept does not exist, need update event
				updateRecord.ChangedRecords = append(updateRecord.ChangedRecords, Event{
					ConceptType:   conceptData[updatedSourceID],
					ConceptUUID:   updatedSourceID,
					AggregateHash: aggregateHash,
					TransactionID: transID,
					EventDetails: ConceptEvent{
						Type: UpdatedEvent,
					},
				})

				//create concordance event for non concorded concept
				updateRecord.ChangedRecords = append(updateRecord.ChangedRecords, Event{
					ConceptType:   conceptData[updatedSourceID],
					ConceptUUID:   updatedSourceID,
					AggregateHash: aggregateHash,
					TransactionID: transID,
					EventDetails: ConcordanceEvent{
						Type:  AddedEvent,
						OldID: updatedSourceID,
						NewID: newAggregatedConcept.PrefUUID,
					},
				})
			}
			continue
		} else if len(result) > 1 {
			//this scenario should never happen
			err = fmt.Errorf("Multiple source concepts found with matching uuid: %s", updatedSourceID)
			logger.WithTransactionID(transID).WithUUID(newAggregatedConcept.PrefUUID).Error(err.Error())
			return deleteLonePrefUUIDQueries, err
		}

		entityEquivalence := result[0]
		conceptType, err := mapper.MostSpecificType(entityEquivalence.Types)
		if err != nil {
			logger.WithError(err).WithTransactionID(transID).WithUUID(newAggregatedConcept.PrefUUID).Errorf("could not return most specific type from source node: %v", entityEquivalence.Types)
			return deleteLonePrefUUIDQueries, err
		}

		logger.WithField("UUID", updatedSourceID).Debug("Existing prefUUID is " + entityEquivalence.PrefUUID + " equivalence count is " + strconv.Itoa(entityEquivalence.Equivalence))
		if entityEquivalence.Equivalence == 0 {
			// Source is old as exists in Neo4j without a prefNode. It can be transferred without issue
			continue
		} else if entityEquivalence.Equivalence == 1 {
			// Source exists in neo4j but is not concorded. It can be transferred without issue but its prefNode should be deleted
			if updatedSourceID == entityEquivalence.PrefUUID {
				logger.WithTransactionID(transID).WithUUID(newAggregatedConcept.PrefUUID).Debugf("Pref uuid node for source %s will need to be deleted as its source will be removed", updatedSourceID)
				deleteLonePrefUUIDQueries = append(deleteLonePrefUUIDQueries, deleteLonePrefUUID(entityEquivalence.PrefUUID))
				//concordance added
				updateRecord.ChangedRecords = append(updateRecord.ChangedRecords, Event{
					ConceptType:   conceptType,
					ConceptUUID:   updatedSourceID,
					AggregateHash: aggregateHash,
					TransactionID: transID,
					EventDetails: ConcordanceEvent{
						Type:  AddedEvent,
						OldID: updatedSourceID,
						NewID: newAggregatedConcept.PrefUUID,
					},
				})
				continue
			} else {
				// Source is only source concorded to non-matching prefUUID; scenario should NEVER happen
				err := fmt.Errorf("This source id: %s the only concordance to a non-matching node with prefUuid: %s", updatedSourceID, entityEquivalence.PrefUUID)
				logger.WithTransactionID(transID).WithUUID(newAggregatedConcept.PrefUUID).WithField("alert_tag", "ConceptLoadingDodgyData").Error(err)
				return deleteLonePrefUUIDQueries, err
			}
		} else {
			if updatedSourceID == entityEquivalence.PrefUUID {
				if updatedSourceID != newAggregatedConcept.PrefUUID {
					authority := getCanonicalAuthority(newAggregatedConcept)
					if entityEquivalence.Authority != authority && stringInArr(entityEquivalence.Authority, concordancesSources) {
						logger.WithTransactionID(transID).WithUUID(newAggregatedConcept.PrefUUID).Debugf("Canonical node for main source %s will need to be deleted and all concordances will be transfered to the new concordance", updatedSourceID)
						// just delete the lone prefUUID node because the other concordances to
						// this node should already be in the new sourceRepresentations (aggregate-concept-transformer responsability)
						deleteLonePrefUUIDQueries = append(deleteLonePrefUUIDQueries, deleteLonePrefUUID(entityEquivalence.PrefUUID))
						updateRecord.ChangedRecords = append(updateRecord.ChangedRecords, Event{
							ConceptType:   conceptType,
							ConceptUUID:   updatedSourceID,
							AggregateHash: aggregateHash,
							TransactionID: transID,
							EventDetails: ConcordanceEvent{
								Type:  AddedEvent,
								OldID: updatedSourceID,
								NewID: newAggregatedConcept.PrefUUID,
							},
						})
						continue
					}
					// Source is prefUUID for a different concordance
					err := fmt.Errorf("Cannot currently process this record as it will break an existing concordance with prefUuid: %s", updatedSourceID)
					logger.WithTransactionID(transID).WithUUID(newAggregatedConcept.PrefUUID).WithField("alert_tag", "ConceptLoadingInvalidConcordance").Error(err)
					return deleteLonePrefUUIDQueries, err
				}
			} else {
				// Source was concorded to different concordance. Data on existing concordance is now out of date
				logger.WithTransactionID(transID).WithUUID(newAggregatedConcept.PrefUUID).WithField("alert_tag", "ConceptLoadingStaleData").Infof("Need to re-ingest concordance record for prefUuid: %s as source: %s has been removed.", entityEquivalence.PrefUUID, updatedSourceID)

				updateRecord.ChangedRecords = append(updateRecord.ChangedRecords, Event{
					ConceptType:   conceptType,
					ConceptUUID:   updatedSourceID,
					AggregateHash: aggregateHash,
					TransactionID: transID,
					EventDetails: ConcordanceEvent{
						Type:  RemovedEvent,
						OldID: entityEquivalence.PrefUUID,
						NewID: updatedSourceID,
					},
				})

				updateRecord.ChangedRecords = append(updateRecord.ChangedRecords, Event{
					ConceptType:   conceptType,
					ConceptUUID:   updatedSourceID,
					AggregateHash: aggregateHash,
					TransactionID: transID,
					EventDetails: ConcordanceEvent{
						Type:  AddedEvent,
						OldID: updatedSourceID,
						NewID: newAggregatedConcept.PrefUUID,
					},
				})
				continue
			}
		}
	}
	return deleteLonePrefUUIDQueries, nil
}

//Clean up canonical nodes of a concept that has become a source of current concept
func deleteLonePrefUUID(prefUUID string) *neoism.CypherQuery {
	logger.WithField("UUID", prefUUID).Debug("Deleting orphaned prefUUID node")
	equivQuery := &neoism.CypherQuery{
		Statement: `MATCH (t:Thing {prefUUID:{id}}) DETACH DELETE t`,
		Parameters: map[string]interface{}{
			"id": prefUUID,
		},
	}
	return equivQuery
}

//Clear down current concept node
func (s *ConceptService) clearDownExistingNodes(ac ontology.NewAggregatedConcept) []*neoism.CypherQuery {
	acUUID := ac.PrefUUID

	var queryBatch []*neoism.CypherQuery

	relationsToRemove := []string{
		"EQUIVALENT_TO",
		"ISSUED_BY",
	}
	relationMap := ontology.GetRelationships()
	for _, setup := range relationMap {
		relationsToRemove = append(relationsToRemove, setup.NeoRelationship)
	}
	labelsToRemove := strings.Join(ontology.GetRemovableConceptTypeLabels(), ":")
	for _, sr := range ac.SourceRepresentations {
		deletePreviousSourceLabelsAndPropertiesQuery := &neoism.CypherQuery{
			Statement: fmt.Sprintf(`MATCH (t:Thing {uuid:{id}})
			MATCH (t)-[r]->(other)
			WHERE TYPE(r) IN {relations}
			REMOVE t:%s
			SET t={uuid:{id}}
			DELETE  r`, labelsToRemove),
			Parameters: map[string]interface{}{
				"id":        sr.UUID,
				"relations": relationsToRemove,
			},
		}
		queryBatch = append(queryBatch, deletePreviousSourceLabelsAndPropertiesQuery)
	}

	//cleanUP all the previous Equivalent to relationships
	deletePreviousCanonicalLabelsAndPropertiesQuery := &neoism.CypherQuery{
		Statement: fmt.Sprintf(`MATCH (t:Thing {prefUUID:{acUUID}})
			OPTIONAL MATCH (t)<-[rel:EQUIVALENT_TO]-(s)
			REMOVE t:%s
			SET t={prefUUID:{acUUID}}
			DELETE rel`, labelsToRemove),
		Parameters: map[string]interface{}{
			"acUUID": acUUID,
		},
	}
	queryBatch = append(queryBatch, deletePreviousCanonicalLabelsAndPropertiesQuery)

	return queryBatch
}

//Curate all queries to populate concept nodes
func populateConceptQueries(queryBatch []*neoism.CypherQuery, aggregatedConcept ontology.NewAggregatedConcept) []*neoism.CypherQuery {
	// Create a sourceConcept from the canonical information - WITH NO UUID
	concept := ontology.NewSourceConcept{
		GenericConcept: ontology.GenericConcept{
			Properties: map[string]interface{}{},
		},
		Hash:     aggregatedConcept.AggregatedHash,
		IssuedBy: aggregatedConcept.IssuedBy,
		Type:     aggregatedConcept.Type,
	}

	canonicalNodeProperties := ontology.GetFilteredPropertySetup(ontology.CanonicalProperty)
	for label := range canonicalNodeProperties {
		concept.Properties[label] = aggregatedConcept.Properties[label]
	}
	// Canonical node that doesn't have UUID
	canonicalProps := setProps(concept, aggregatedConcept.PrefUUID, false)
	labels := strings.Join(ontology.GetConceptTypeLabels(concept.Type), ":")
	createConceptQuery := &neoism.CypherQuery{
		Statement: fmt.Sprintf(`MERGE (n:Thing {prefUUID: {prefUUID}})
											set n={allprops}
											set n :%s`, labels),
		Parameters: map[string]interface{}{
			"prefUUID": aggregatedConcept.PrefUUID,
			"allprops": canonicalProps,
		},
	}
	queryBatch = append(queryBatch, createConceptQuery)

	// Repopulate
	relationMap := ontology.GetRelationships()
	for _, sourceConcept := range aggregatedConcept.SourceRepresentations {
		queryBatch = append(queryBatch, createNodeQueries(sourceConcept, sourceConcept.UUID)...)

		equivQuery := &neoism.CypherQuery{
			Statement: `MATCH (t:Thing {uuid:{uuid}}), (c:Thing {prefUUID:{prefUUID}})
						MERGE (t)-[:EQUIVALENT_TO]->(c)`,
			Parameters: map[string]interface{}{
				"uuid":     sourceConcept.UUID,
				"prefUUID": aggregatedConcept.PrefUUID,
			},
		}
		queryBatch = append(queryBatch, equivQuery)

		for _, relation := range sourceConcept.Relations {
			setup, has := relationMap[relation.Label]
			if !has {
				continue
			}
			q := addRelationship(sourceConcept.UUID, relation.Connections, setup.NeoRelationship, setup.NeoShouldCreate)
			queryBatch = append(queryBatch, q...)
		}
	}
	return queryBatch
}

func createNodeQueries(concept ontology.NewSourceConcept, uuid string) []*neoism.CypherQuery {
	var queryBatch []*neoism.CypherQuery
	var createConceptQuery *neoism.CypherQuery

	allProps := setProps(concept, uuid, true)
	labels := strings.Join(ontology.GetConceptTypeLabels(concept.Type), ":")
	createConceptQuery = &neoism.CypherQuery{
		Statement: fmt.Sprintf(`MERGE (n:Thing {uuid: {uuid}})
											set n={allprops}
											set n :%s`, labels),
		Parameters: map[string]interface{}{
			"uuid":     uuid,
			"allprops": allProps,
		},
	}

	if uuid != "" && concept.IssuedBy != "" {
		writeFinIns := &neoism.CypherQuery{
			Statement: `MERGE (fi:Thing {uuid: {fiUUID}})
						MERGE (org:Thing {uuid: {orgUUID}})
						MERGE (fi)-[:ISSUED_BY]->(org)
						`,
			Parameters: neoism.Props{
				"fiUUID":  concept.UUID,
				"orgUUID": concept.IssuedBy,
			},
		}
		queryBatch = append(queryBatch, writeFinIns)
	}
	queryBatch = append(queryBatch, createConceptQuery)
	return queryBatch
}

//Add relationships to concepts
func addRelationship(conceptID string, connections []ontology.Connection, relationshipType string, createOnMissing bool) []*neoism.CypherQuery {

	const (
		findConceptNode  = `MATCH (this:Concept {uuid: {uuid}})`
		mergeThingNode   = `MERGE (this:Thing {uuid: {uuid}})`
		mergeOtherNode   = `MERGE (other:Thing {uuid: {other_uuid}})`
		createRelation   = `MERGE (this)-[rel:%s]->(other)`
		setRelationProps = `set rel={relation_props}`
	)
	var query string
	if createOnMissing {
		query = findConceptNode
	} else {
		query = mergeThingNode
	}
	query += "\n"
	query += mergeOtherNode
	query += "\n"
	query += createRelation
	query += "\n"

	var queryBatch []*neoism.CypherQuery
	for _, con := range connections {
		statement := fmt.Sprintf(query, relationshipType)
		if con.Properties != nil {
			statement += setRelationProps
		}
		addRelationshipQuery := &neoism.CypherQuery{
			Statement: statement,
			Parameters: map[string]interface{}{
				"uuid":           conceptID,
				"other_uuid":     con.UUID,
				"relation_props": con.Properties,
			},
		}
		queryBatch = append(queryBatch, addRelationshipQuery)
	}
	return queryBatch
}

//Create canonical node for any concepts that were removed from a concordance and thus would become lone
func (s *ConceptService) writeCanonicalNodeForUnconcordedConcepts(concept ontology.NewSourceConcept) *neoism.CypherQuery {
	allProps := setProps(concept, concept.UUID, false)
	logger.WithField("UUID", concept.UUID).Debug("Creating prefUUID node for unconcorded concept")
	labels := strings.Join(ontology.GetConceptTypeLabels(concept.Type), ":")
	createCanonicalNodeQuery := &neoism.CypherQuery{
		Statement: fmt.Sprintf(`
					MATCH (t:Thing{uuid:{prefUUID}})
					MERGE (n:Thing {prefUUID: {prefUUID}})
					MERGE (n)<-[:EQUIVALENT_TO]-(t)
					set n={allprops}
					set n :%s`, labels),
		Parameters: map[string]interface{}{
			"prefUUID": concept.UUID,
			"allprops": allProps,
		},
	}
	return createCanonicalNodeQuery
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

//extract uuids of the source concepts
func getSourceData(sourceConcepts []ontology.NewSourceConcept) map[string]string {
	conceptData := make(map[string]string)
	for _, concept := range sourceConcepts {
		conceptData[concept.UUID] = concept.Type
	}
	return conceptData
}

//This function dictates which properties will be actually
//written in neo for both canonical and source nodes.
func setProps(concept ontology.NewSourceConcept, id string, isSource bool) map[string]interface{} {
	nodeProps := map[string]interface{}{}
	// TODO: Check if props are empty not just that they exist

	//common props
	sourceNodePropertiesToStore := ontology.GetFilteredPropertySetup(ontology.SourceProperty)
	for label, setup := range sourceNodePropertiesToStore {
		val, has := concept.GetProp(label)
		if !has {
			continue
		}
		nodeProps[setup.NeoLabel] = val
	}

	nodeProps["lastModifiedEpoch"] = time.Now().Unix()
	//source specific props
	if isSource {
		nodeProps["uuid"] = id
		return nodeProps
	}
	nodeProps["prefUUID"] = id
	nodeProps["aggregateHash"] = concept.Hash
	//canonical specific props
	canonicalNodePropertiesToStore := ontology.GetFilteredPropertySetup(ontology.CanonicalProperty)
	for label, setup := range canonicalNodePropertiesToStore {
		val, has := concept.GetProp(label)
		if !has {
			continue
		}
		nodeProps[setup.NeoLabel] = val
	}

	return nodeProps
}

//DecodeJSON - decode json
func (s *ConceptService) DecodeJSON(dec *json.Decoder) (interface{}, string, error) {
	sub := transform.AggregatedConcept{}
	err := dec.Decode(&sub)
	return sub, sub.PrefUUID, err
}

//Check - checker
func (s *ConceptService) Check() error {
	if err := neoutils.CheckWritable(s.conn); err != nil {
		return err
	}
	return neoutils.Check(s.conn)
}

type requestError struct {
	details string
}

//Error - Error
func (re requestError) Error() string {
	return re.details
}

//InvalidRequestDetails - Specific error for providing bad request (400) back
func (re requestError) InvalidRequestDetails() string {
	return re.details
}

func processMembershipRoles(v interface{}) interface{} {
	switch c := v.(type) {
	case transform.AggregatedConcept:
		c.InceptionDateEpoch = getEpoch(c.InceptionDate)
		c.TerminationDateEpoch = getEpoch(c.TerminationDate)
		c.MembershipRoles = cleanMembershipRoles(c.MembershipRoles)
		for _, s := range c.SourceRepresentations {
			processMembershipRoles(s)
		}
	case transform.SourceConcept:
		c.InceptionDateEpoch = getEpoch(c.InceptionDate)
		c.TerminationDateEpoch = getEpoch(c.TerminationDate)
		c.MembershipRoles = cleanMembershipRoles(c.MembershipRoles)
	case transform.MembershipRole:
		c.InceptionDateEpoch = getEpoch(c.InceptionDate)
		c.TerminationDateEpoch = getEpoch(c.TerminationDate)
	}
	return v
}

func cleanMembershipRoles(m []transform.MembershipRole) []transform.MembershipRole {
	deleted := 0
	for i := range m {
		j := i - deleted
		if m[j].RoleUUID == "" {
			m = m[:j+copy(m[j:], m[j+1:])]
			deleted++
			continue
		}
		m[j].InceptionDateEpoch = getEpoch(m[j].InceptionDate)
		m[j].TerminationDateEpoch = getEpoch(m[j].TerminationDate)
	}

	if len(m) == 0 {
		return nil
	}

	return m
}

func getEpoch(t string) int64 {
	if t == "" {
		return 0
	}

	tt, _ := time.Parse(iso8601DateOnly, t)
	return tt.Unix()
}

// cleanNAICS returns the same slice of NAICSIndustryClassification if all are valid,
// skips the invalid ones, returns nil if the input slice doesn't have valid NAICSIndustryClassification objects
func cleanNAICS(naics []transform.NAICSIndustryClassification) []transform.NAICSIndustryClassification {
	var res []transform.NAICSIndustryClassification
	for _, ic := range naics {
		if ic.UUID != "" {
			res = append(res, ic)
		}
	}
	return res
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

	return a
}

func cleanConcept(c transform.AggregatedConcept) transform.AggregatedConcept {
	for j := range c.SourceRepresentations {
		c.SourceRepresentations[j].LastModifiedEpoch = 0
		for i := range c.SourceRepresentations[j].MembershipRoles {
			c.SourceRepresentations[j].MembershipRoles[i].InceptionDateEpoch = 0
			c.SourceRepresentations[j].MembershipRoles[i].TerminationDateEpoch = 0
		}
		sort.SliceStable(c.SourceRepresentations[j].MembershipRoles, func(k, l int) bool {
			return c.SourceRepresentations[j].MembershipRoles[k].RoleUUID < c.SourceRepresentations[j].MembershipRoles[l].RoleUUID
		})
		sort.SliceStable(c.SourceRepresentations[j].BroaderUUIDs, func(k, l int) bool {
			return c.SourceRepresentations[j].BroaderUUIDs[k] < c.SourceRepresentations[j].BroaderUUIDs[l]
		})
		sort.SliceStable(c.SourceRepresentations[j].RelatedUUIDs, func(k, l int) bool {
			return c.SourceRepresentations[j].RelatedUUIDs[k] < c.SourceRepresentations[j].RelatedUUIDs[l]
		})
		sort.SliceStable(c.SourceRepresentations[j].SupersededByUUIDs, func(k, l int) bool {
			return c.SourceRepresentations[j].SupersededByUUIDs[k] < c.SourceRepresentations[j].SupersededByUUIDs[l]
		})
		sort.SliceStable(c.SourceRepresentations[j].ImpliedByUUIDs, func(k, l int) bool {
			return c.SourceRepresentations[j].ImpliedByUUIDs[k] < c.SourceRepresentations[j].ImpliedByUUIDs[l]
		})
		sort.SliceStable(c.SourceRepresentations[j].HasFocusUUIDs, func(k, l int) bool {
			return c.SourceRepresentations[j].HasFocusUUIDs[k] < c.SourceRepresentations[j].HasFocusUUIDs[l]
		})
		sort.SliceStable(c.SourceRepresentations[j].NAICSIndustryClassifications, func(k, l int) bool {
			return c.SourceRepresentations[j].NAICSIndustryClassifications[k].Rank < c.SourceRepresentations[j].NAICSIndustryClassifications[l].Rank
		})
	}
	for i := range c.MembershipRoles {
		c.MembershipRoles[i].InceptionDateEpoch = 0
		c.MembershipRoles[i].TerminationDateEpoch = 0
	}
	sort.SliceStable(c.SourceRepresentations, func(k, l int) bool {
		return c.SourceRepresentations[k].UUID < c.SourceRepresentations[l].UUID
	})
	return c
}

func cleanHash(c transform.AggregatedConcept) transform.AggregatedConcept {
	c.AggregatedHash = ""
	return c
}

func cleanSourceProperties(c ontology.NewAggregatedConcept) ontology.NewAggregatedConcept {
	var cleanSources []ontology.NewSourceConcept
	sourceProperties := ontology.GetFilteredPropertySetup(ontology.SourceProperty)
	relations := ontology.GetRelationships()

	for _, source := range c.SourceRepresentations {
		cleanProps := map[string]interface{}{}
		for label := range sourceProperties {
			cleanProps[label] = source.Properties[label]
		}

		var cleanRelations []ontology.Relationship
		for _, rel := range source.Relations {
			_, hasRelation := relations[rel.Label]
			if hasRelation {
				cleanRelations = append(cleanRelations, rel)
			}
		}

		cleanConcept := ontology.NewSourceConcept{
			GenericConcept: ontology.GenericConcept{
				Properties: cleanProps,
				Relations:  cleanRelations,
			},
			UUID:     source.UUID,
			Type:     source.Type,
			IssuedBy: source.IssuedBy,
		}
		cleanSources = append(cleanSources, cleanConcept)
	}
	c.SourceRepresentations = cleanSources
	return c
}

func getCanonicalAuthority(aggregate ontology.NewAggregatedConcept) string {
	for _, source := range aggregate.SourceRepresentations {
		if source.UUID == aggregate.PrefUUID {
			authority, _ := source.GetPropString(ontology.AuthorityProp)
			return authority
		}
	}
	return ""
}

func stringInArr(searchFor string, values []string) bool {
	for _, val := range values {
		if searchFor == val {
			return true
		}
	}
	return false
}
