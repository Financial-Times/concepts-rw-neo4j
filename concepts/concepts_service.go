package concepts

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/Financial-Times/concepts-rw-neo4j/ontology"
	logger "github.com/Financial-Times/go-logger"
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

var relationships = map[string]ontology.RelationshipConfig{
	"HAS_MEMBER": {
		ConceptField: "personUUID",
		OneToOne:     true,
	},
	"HAS_ORGANISATION": {
		ConceptField: "organisationUUID",
		OneToOne:     true,
	},
	"SUB_ORGANISATION_OF": {
		ConceptField: "parentOrganisation",
		OneToOne:     true,
	},
	"COUNTRY_OF_OPERATIONS": {
		ConceptField: "countryOfOperationsUUID",
		OneToOne:     true,
	},
	"COUNTRY_OF_INCORPORATION": {
		ConceptField: "countryOfIncorporationUUID",
		OneToOne:     true,
	},
	"COUNTRY_OF_RISK": {
		ConceptField: "countryOfRiskUUID",
		OneToOne:     true,
	},
	"HAS_PARENT": {
		ConceptField: "parentUUIDs",
	},
	"IS_RELATED_TO": {
		ConceptField: "relatedUUIDs",
	},
	"SUPERSEDED_BY": {
		ConceptField: "supersededByUUIDs",
	},
	"HAS_BROADER": {
		ConceptField: "broaderUUIDs",
	},
	"IMPLIED_BY": {
		ConceptField: "impliedByUUIDs",
	},
	"HAS_FOCUS": {
		ConceptField: "hasFocusUUIDs",
	},
	"HAS_ROLE": {
		ConceptField: "membershipRoles",
		Properties: []string{
			"inceptionDate",
			"terminationDate",
			"inceptionDateEpoch",
			"terminationDateEpoch",
		},
	},
	"HAS_INDUSTRY_CLASSIFICATION": {
		ConceptField:    "naicsIndustryClassifications",
		Properties:      []string{"rank"},
		ToNodeWithLabel: "NAICSIndustryClassification",
	},
}

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

func (s *ConceptService) Read(uuid string, transID string) (interface{}, bool, error) {
	newAggregatedConcept, exists, err := s.read(uuid, transID)
	aggregatedConcept := ontology.TransformToOldAggregateConcept(newAggregatedConcept)

	logger.WithTransactionID(transID).WithUUID(uuid).Debugf("Returned concept is %v", aggregatedConcept)
	return aggregatedConcept, exists, err
}

func (s *ConceptService) read(uuid string, transID string) (ontology.NewAggregatedConcept, bool, error) {
	var results []neoAggregatedConcept
	query := &neoism.CypherQuery{
		Statement: getReadStatement(),
		Parameters: map[string]interface{}{
			"uuid": uuid,
		},
		Result: &results,
	}

	err := s.conn.CypherBatch([]*neoism.CypherQuery{query})
	if err != nil {
		logger.WithError(err).WithTransactionID(transID).WithUUID(uuid).Error("Error executing neo4j read query")
		return ontology.NewAggregatedConcept{}, false, err
	}

	if len(results) == 0 {
		logger.WithTransactionID(transID).WithUUID(uuid).Info("Concept not found in db")
		return ontology.NewAggregatedConcept{}, false, nil
	}

	neoAggregateConcept := results[0]
	newAggregatedConcept, logMsg, err := neoAggregateConcept.ToOntologyNewAggregateConcept(ontology.GetConfig())
	if err != nil {
		logger.WithError(err).WithTransactionID(transID).WithUUID(uuid).Error(logMsg)
		return ontology.NewAggregatedConcept{}, false, err
	}

	return newAggregatedConcept, true, nil
}

func (s *ConceptService) Write(thing interface{}, transID string) (interface{}, error) {
	// Read the aggregated concept - We need read the entire model first. This is because if we unconcord a TME concept
	// then we need to add prefUUID to the lone node if it has been removed from the concordance listed against a Smartlogic concept
	oldAggregatedConcept := thing.(ontology.AggregatedConcept)
	aggregatedConceptToWrite := ontology.TransformToNewAggregateConcept(oldAggregatedConcept)

	aggregatedConceptToWrite = cleanSourceProperties(aggregatedConceptToWrite)
	requestSourceData := getSourceData(aggregatedConceptToWrite.SourceRepresentations)

	requestHash, err := hashstructure.Hash(aggregatedConceptToWrite, nil)
	if err != nil {
		logger.WithError(err).WithTransactionID(transID).WithUUID(aggregatedConceptToWrite.PrefUUID).Error("Error hashing json from request")
		return ConceptChanges{}, err
	}

	hashAsString := strconv.FormatUint(requestHash, 10)

	if err = validateObject(aggregatedConceptToWrite, transID); err != nil {
		return ConceptChanges{}, err
	}

	existingAggregateConcept, exists, err := s.read(aggregatedConceptToWrite.PrefUUID, transID)
	if err != nil {
		logger.WithError(err).WithTransactionID(transID).WithUUID(aggregatedConceptToWrite.PrefUUID).Error("Read request for existing concordance resulted in error")
		return ConceptChanges{}, err
	}

	processMembershipRoles(&aggregatedConceptToWrite)

	var queryBatch []*neoism.CypherQuery
	var prefUUIDsToBeDeletedQueryBatch []*neoism.CypherQuery
	var updatedUUIDList []string
	updateRecord := ConceptChanges{}
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

					canonical := sourceToCanonical(concept)
					unconcordQuery := s.writeCanonicalNodeForUnconcordedConcepts(canonical, concept.UUID)
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
	if aggConcept.PrefLabel == "" {
		return requestError{formatError("prefLabel", aggConcept.PrefUUID, transID)}
	}
	if _, ok := constraintMap[aggConcept.Type]; !ok {
		return requestError{formatError("type", aggConcept.PrefUUID, transID)}
	}
	if aggConcept.SourceRepresentations == nil {
		return requestError{formatError("sourceRepresentation", aggConcept.PrefUUID, transID)}
	}
	for _, sourceConcept := range aggConcept.SourceRepresentations {
		if err := sourceConcept.Validate(); err != nil {
			if errors.Is(err, ontology.ErrUnkownAuthority) {
				logger.WithTransactionID(transID).WithUUID(aggConcept.PrefUUID).Debugf("Unknown authority supplied in the request: %s", sourceConcept.Authority)
			} else {
				logger.WithError(err).WithTransactionID(transID).WithUUID(sourceConcept.UUID).Error("Validation of payload failed")
			}

			return requestError{err.Error()}
		}

		if sourceConcept.Type == "" {
			return requestError{formatError("sourceRepresentation.type", sourceConcept.UUID, transID)}
		}

		if _, ok := constraintMap[sourceConcept.Type]; !ok {
			return requestError{formatError("type", aggConcept.PrefUUID, transID)}
		}
	}
	return nil
}

func formatError(field string, uuid string, transID string) string {
	err := errors.New("invalid request, no " + field + " has been supplied")
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
					authority := newAggregatedConcept.GetCanonicalAuthority()
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
	for _, sr := range ac.SourceRepresentations {
		deletePreviousSourceLabelsAndPropertiesQuery := &neoism.CypherQuery{
			Statement: getDeleteStatement(),
			Parameters: map[string]interface{}{
				"id": sr.UUID,
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
			DELETE rel`, getLabelsToRemove()),
		Parameters: map[string]interface{}{
			"acUUID": acUUID,
		},
	}
	queryBatch = append(queryBatch, deletePreviousCanonicalLabelsAndPropertiesQuery)

	return queryBatch
}

//Curate all queries to populate concept nodes
func populateConceptQueries(queryBatch []*neoism.CypherQuery, aggregatedConcept ontology.NewAggregatedConcept) []*neoism.CypherQuery {
	queryBatch = append(queryBatch, createCanonicalNodeQueries(aggregatedConcept, aggregatedConcept.PrefUUID)...)

	for _, sourceConcept := range aggregatedConcept.SourceRepresentations {
		queryBatch = append(queryBatch, createNodeQueries(sourceConcept, sourceConcept.UUID)...)
		queryBatch = append(queryBatch, createEquivalentToQueries(sourceConcept, aggregatedConcept)...)

		for _, rel := range sourceConcept.Relationships {
			relCfg, ok := ontology.GetConfig().Relationships[rel.Label]
			if !ok {
				continue
			}

			relIDs := filterSlice([]string{rel.UUID})
			queryBatch = append(queryBatch, createRelQueries(sourceConcept.UUID, relIDs, rel.Label, relCfg.NeoCreate)...)

			if len(relCfg.Properties) > 0 {
				queryBatch = append(queryBatch, setRelPropsQueries(sourceConcept.UUID, rel)...)
			}
		}

		queryBatch = append(queryBatch, createRelQueries(sourceConcept.UUID, sourceConcept.RelatedUUIDs, "IS_RELATED_TO", false)...)
		queryBatch = append(queryBatch, createRelQueries(sourceConcept.UUID, sourceConcept.BroaderUUIDs, "HAS_BROADER", false)...)
		queryBatch = append(queryBatch, createRelQueries(sourceConcept.UUID, sourceConcept.SupersededByUUIDs, "SUPERSEDED_BY", false)...)
		queryBatch = append(queryBatch, createRelQueries(sourceConcept.UUID, sourceConcept.ImpliedByUUIDs, "IMPLIED_BY", false)...)
		queryBatch = append(queryBatch, createRelQueries(sourceConcept.UUID, sourceConcept.HasFocusUUIDs, "HAS_FOCUS", false)...)
	}

	return queryBatch
}

func createEquivalentToQueries(sourceConcept ontology.NewConcept, aggregatedConcept ontology.NewAggregatedConcept) []*neoism.CypherQuery {
	var queryBatch []*neoism.CypherQuery
	equivQuery := &neoism.CypherQuery{
		Statement: `MATCH (t:Thing {uuid:{uuid}}), (c:Thing {prefUUID:{prefUUID}})
						MERGE (t)-[:EQUIVALENT_TO]->(c)`,
		Parameters: map[string]interface{}{
			"uuid":     sourceConcept.UUID,
			"prefUUID": aggregatedConcept.PrefUUID,
		},
	}

	queryBatch = append(queryBatch, equivQuery)
	return queryBatch
}

func createCanonicalNodeQueries(canonical ontology.NewAggregatedConcept, prefUUID string) []*neoism.CypherQuery {
	var queryBatch []*neoism.CypherQuery
	var createConceptQuery *neoism.CypherQuery

	allProps := setCanonicalProps(canonical, prefUUID)
	createConceptQuery = &neoism.CypherQuery{
		Statement: fmt.Sprintf(`MERGE (n:Thing {prefUUID: {prefUUID}})
								set n={allprops}
								set n :%s`, getAllLabels(canonical.Type)),
		Parameters: map[string]interface{}{
			"prefUUID": prefUUID,
			"allprops": allProps,
		},
	}

	queryBatch = append(queryBatch, createConceptQuery)
	return queryBatch
}

func createNodeQueries(concept ontology.NewConcept, uuid string) []*neoism.CypherQuery {
	var queryBatch []*neoism.CypherQuery
	var createConceptQuery *neoism.CypherQuery

	allProps := setProps(concept, uuid)
	createConceptQuery = &neoism.CypherQuery{
		Statement: fmt.Sprintf(`MERGE (n:Thing {uuid: {uuid}})
											set n={allprops}
											set n :%s`, getAllLabels(concept.Type)),
		Parameters: map[string]interface{}{
			"uuid":     uuid,
			"allprops": allProps,
		},
	}

	queryBatch = append(queryBatch, createRelQueries(concept.UUID, concept.ParentUUIDs, "HAS_PARENT", true)...)

	relIDs := filterSlice([]string{concept.OrganisationUUID})
	queryBatch = append(queryBatch, createRelQueries(concept.UUID, relIDs, "HAS_ORGANISATION", true)...)

	relIDs = filterSlice([]string{concept.PersonUUID})
	queryBatch = append(queryBatch, createRelQueries(concept.UUID, relIDs, "HAS_MEMBER", true)...)

	relIDs = filterSlice([]string{concept.IssuedBy})
	queryBatch = append(queryBatch, createRelQueries(concept.UUID, relIDs, "ISSUED_BY", true)...)

	relIDs = filterSlice([]string{concept.ParentOrganisation})
	queryBatch = append(queryBatch, createRelQueries(concept.UUID, relIDs, "SUB_ORGANISATION_OF", true)...)

	relIDs = filterSlice([]string{concept.CountryOfRiskUUID})
	queryBatch = append(queryBatch, createRelQueries(concept.UUID, relIDs, "COUNTRY_OF_RISK", true)...)

	relIDs = filterSlice([]string{concept.CountryOfIncorporationUUID})
	queryBatch = append(queryBatch, createRelQueries(concept.UUID, relIDs, "COUNTRY_OF_INCORPORATION", true)...)

	relIDs = filterSlice([]string{concept.CountryOfOperationsUUID})
	queryBatch = append(queryBatch, createRelQueries(concept.UUID, relIDs, "COUNTRY_OF_OPERATIONS", true)...)

	for _, naics := range concept.NAICSIndustryClassifications {
		if naics.UUID != "" {
			writeNAICS := &neoism.CypherQuery{
				Statement: `MERGE (org:Thing {uuid: {uuid}})
								MERGE (naicsIC:Thing {uuid: {naicsUUID}})
								MERGE (org)-[:HAS_INDUSTRY_CLASSIFICATION{rank:{rank}}]->(naicsIC)`,
				Parameters: neoism.Props{
					"naicsUUID": naics.UUID,
					"rank":      naics.Rank,
					"uuid":      concept.UUID,
				},
			}
			queryBatch = append(queryBatch, writeNAICS)
		}
	}

	for _, membershipRole := range concept.MembershipRoles {
		params := neoism.Props{
			"inceptionDate":        nil,
			"inceptionDateEpoch":   nil,
			"terminationDate":      nil,
			"terminationDateEpoch": nil,
			"roleUUID":             membershipRole.RoleUUID,
			"nodeUUID":             concept.UUID,
		}
		if membershipRole.InceptionDate != "" {
			params["inceptionDate"] = membershipRole.InceptionDate
		}
		if membershipRole.InceptionDateEpoch > 0 {
			params["inceptionDateEpoch"] = membershipRole.InceptionDateEpoch
		}
		if membershipRole.TerminationDate != "" {
			params["terminationDate"] = membershipRole.TerminationDate
		}
		if membershipRole.TerminationDateEpoch > 0 {
			params["terminationDateEpoch"] = membershipRole.TerminationDateEpoch
		}
		writeParent := &neoism.CypherQuery{
			Statement: `MERGE (node:Thing{uuid: {nodeUUID}})
							MERGE (role:Thing{uuid: {roleUUID}})
								ON CREATE SET
									role.uuid = {roleUUID}
							MERGE (node)-[rel:HAS_ROLE]->(role)
								ON CREATE SET
									rel.inceptionDate = {inceptionDate},
									rel.inceptionDateEpoch = {inceptionDateEpoch},
									rel.terminationDate = {terminationDate},
									rel.terminationDateEpoch = {terminationDateEpoch}
							`,
			Parameters: params,
		}
		queryBatch = append(queryBatch, writeParent)
	}

	queryBatch = append(queryBatch, createConceptQuery)
	return queryBatch
}

// createRelQueries creates relationships Cypher queries for concepts
func createRelQueries(conceptID string, relationshipIDs []string, relationshipType string, shouldCreate bool) []*neoism.CypherQuery {
	const createMissing = `
		MERGE (thing:Thing {uuid: {uuid}})
		MERGE (other:Thing {uuid: {id}})
		MERGE (thing)-[:%s]->(other)
	`

	const matchExisting = `
		MATCH (concept:Concept {uuid: {uuid}})
		MERGE (other:Thing {uuid: {id}})
		MERGE (concept)-[:%s]->(other)	
	`

	cypherStatement := matchExisting
	if shouldCreate {
		cypherStatement = createMissing
	}

	var queryBatch []*neoism.CypherQuery
	for _, id := range relationshipIDs {
		addRelationshipQuery := &neoism.CypherQuery{
			Statement: fmt.Sprintf(cypherStatement, relationshipType),
			Parameters: map[string]interface{}{
				"uuid": conceptID,
				"id":   id,
			},
		}
		queryBatch = append(queryBatch, addRelationshipQuery)
	}

	return queryBatch
}

func setRelPropsQueries(conceptID string, rel ontology.Relationship) []*neoism.CypherQuery {
	var queryBatch []*neoism.CypherQuery
	setRelProps := &neoism.CypherQuery{
		Statement: fmt.Sprintf(`
			MATCH (t:Thing {uuid: {uuid}})
			MATCH (other:Thing {uuid: {otherUUID}})
			MATCH (t)-[rel:%s]->(other)
			set rel={relProps}`, rel.Label),
		Parameters: neoism.Props{
			"uuid":      conceptID,
			"otherUUID": rel.UUID,
			"relProps":  rel.Properties,
		},
	}

	queryBatch = append(queryBatch, setRelProps)
	return queryBatch
}

//Create canonical node for any concepts that were removed from a concordance and thus would become lone
func (s *ConceptService) writeCanonicalNodeForUnconcordedConcepts(canonical ontology.NewAggregatedConcept, prefUUID string) *neoism.CypherQuery {
	allProps := setCanonicalProps(canonical, prefUUID)
	logger.WithField("UUID", prefUUID).Debug("Creating prefUUID node for unconcorded concept")
	createCanonicalNodeQuery := &neoism.CypherQuery{
		Statement: fmt.Sprintf(`
					MATCH (t:Thing{uuid:{prefUUID}})
					MERGE (n:Thing {prefUUID: {prefUUID}})
					MERGE (n)<-[:EQUIVALENT_TO]-(t)
					set n={allprops}
					set n :%s`, getAllLabels(canonical.Type)),
		Parameters: map[string]interface{}{
			"prefUUID": prefUUID,
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

//return existing labels
func getLabelsToRemove() string {
	var labelsToRemove string
	for i, conceptType := range conceptLabels {
		labelsToRemove += conceptType
		if i+1 < len(conceptLabels) {
			labelsToRemove += ":"
		}
	}
	return labelsToRemove
}

//extract uuids of the source concepts
func getSourceData(sourceConcepts []ontology.NewConcept) map[string]string {
	conceptData := make(map[string]string)
	for _, concept := range sourceConcepts {
		conceptData[concept.UUID] = concept.Type
	}
	return conceptData
}

//This function dictates which properties will be actually
//written in neo for source nodes.
func setProps(source ontology.NewConcept, uuid string) map[string]interface{} {
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

	nodeProps["uuid"] = uuid
	nodeProps["authority"] = source.Authority
	nodeProps["authorityValue"] = source.AuthorityValue

	return nodeProps
}

//This function dictates which properties will be actually
//written in neo for canonical nodes.
func setCanonicalProps(canonical ontology.NewAggregatedConcept, prefUUID string) map[string]interface{} {
	nodeProps := map[string]interface{}{}

	for field, prop := range ontology.GetConfig().FieldToNeoProps {
		if val, ok := canonical.GetPropertyValue(field); ok {
			nodeProps[prop] = val
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

	if len(canonical.Aliases) > 0 {
		nodeProps["aliases"] = canonical.Aliases
	}
	if len(canonical.FormerNames) > 0 {
		nodeProps["formerNames"] = canonical.FormerNames
	}
	if len(canonical.TradeNames) > 0 {
		nodeProps["tradeNames"] = canonical.TradeNames
	}
	if canonical.YearFounded > 0 {
		nodeProps["yearFounded"] = canonical.YearFounded
	}
	if canonical.InceptionDate != "" {
		nodeProps["inceptionDate"] = canonical.InceptionDate
	}
	if canonical.TerminationDate != "" {
		nodeProps["terminationDate"] = canonical.TerminationDate
	}
	if canonical.InceptionDateEpoch > 0 {
		nodeProps["inceptionDateEpoch"] = canonical.InceptionDateEpoch
	}
	if canonical.TerminationDateEpoch > 0 {
		nodeProps["terminationDateEpoch"] = canonical.TerminationDateEpoch
	}
	if canonical.BirthYear > 0 {
		nodeProps["birthYear"] = canonical.BirthYear
	}

	return nodeProps
}

//DecodeJSON - decode json
func (s *ConceptService) DecodeJSON(dec *json.Decoder) (interface{}, string, error) {
	sub := ontology.AggregatedConcept{}
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

func processMembershipRoles(c *ontology.NewAggregatedConcept) {
	for _, s := range c.SourceRepresentations {
		s.MembershipRoles = cleanMembershipRoles(s.MembershipRoles)
		for i := range s.MembershipRoles {
			s.MembershipRoles[i].InceptionDateEpoch = getEpoch(s.MembershipRoles[i].InceptionDate)
			s.MembershipRoles[i].TerminationDateEpoch = getEpoch(s.MembershipRoles[i].TerminationDate)
		}
	}
}

func getEpoch(t string) int64 {
	if t == "" {
		return 0
	}

	tt, _ := time.Parse(iso8601DateOnly, t)
	return tt.Unix()
}

func cleanSourceProperties(c ontology.NewAggregatedConcept) ontology.NewAggregatedConcept {
	var cleanSources []ontology.NewConcept
	for _, source := range c.SourceRepresentations {
		cleanConcept := ontology.NewConcept{
			Relationships:     source.Relationships,
			UUID:              source.UUID,
			PrefLabel:         source.PrefLabel,
			Type:              source.Type,
			Authority:         source.Authority,
			AuthorityValue:    source.AuthorityValue,
			ParentUUIDs:       source.ParentUUIDs,
			OrganisationUUID:  source.OrganisationUUID,
			PersonUUID:        source.PersonUUID,
			RelatedUUIDs:      source.RelatedUUIDs,
			BroaderUUIDs:      source.BroaderUUIDs,
			SupersededByUUIDs: source.SupersededByUUIDs,
			ImpliedByUUIDs:    source.ImpliedByUUIDs,
			HasFocusUUIDs:     source.HasFocusUUIDs,
			MembershipRoles:   source.MembershipRoles,
			IssuedBy:          source.IssuedBy,
			FigiCode:          source.FigiCode,
			IsDeprecated:      source.IsDeprecated,
			// Organisations
			ParentOrganisation:           source.ParentOrganisation,
			CountryOfOperationsUUID:      source.CountryOfOperationsUUID,
			CountryOfIncorporationUUID:   source.CountryOfIncorporationUUID,
			CountryOfRiskUUID:            source.CountryOfRiskUUID,
			NAICSIndustryClassifications: source.NAICSIndustryClassifications,
		}
		cleanSources = append(cleanSources, cleanConcept)
	}
	c.SourceRepresentations = cleanSources
	return c
}

func stringInArr(searchFor string, values []string) bool {
	for _, val := range values {
		if searchFor == val {
			return true
		}
	}
	return false
}

func sourceToCanonical(source ontology.NewConcept) ontology.NewAggregatedConcept {
	return ontology.NewAggregatedConcept{
		Aliases:              source.Aliases,
		AggregatedHash:       source.Hash,
		InceptionDate:        source.InceptionDate,
		InceptionDateEpoch:   source.InceptionDateEpoch,
		IssuedBy:             source.IssuedBy,
		PrefLabel:            source.PrefLabel,
		TerminationDate:      source.TerminationDate,
		TerminationDateEpoch: source.TerminationDateEpoch,
		Type:                 source.Type,
		//TODO deprecated event?
		IsDeprecated: source.IsDeprecated,
		// Organisations
		TradeNames:  source.TradeNames,
		FormerNames: source.FormerNames,
		YearFounded: source.YearFounded,
		// Person
		BirthYear: source.BirthYear,
	}
}
