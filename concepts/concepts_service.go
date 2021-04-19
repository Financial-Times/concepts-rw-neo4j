package concepts

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/Financial-Times/concepts-rw-neo4j/ontology"
	"github.com/Financial-Times/concepts-rw-neo4j/ontology/transform"
	"github.com/Financial-Times/go-logger"
	"github.com/Financial-Times/neo-model-utils-go/mapper"
	"github.com/Financial-Times/neo-utils-go/neoutils"
	"github.com/jmcvetta/neoism"
	"github.com/mitchellh/hashstructure"
	"github.com/sirupsen/logrus"
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

var ConceptNotFoundErr = errors.New("concept not found")

//Read - read service
func (s *ConceptService) Read(uuid string, transID string) (interface{}, bool, error) {
	concept, err := s.read(uuid, transID)
	result := transform.TransformToOldAggregateConcept(concept)
	if err == nil {
		return result, true, nil
	}
	if errors.Is(err, ConceptNotFoundErr) {
		return result, false, nil
	}
	return result, false, err
}

func (s *ConceptService) read(uuid string, transID string) (ontology.NewAggregatedConcept, error) {
	logEntry := logger.WithTransactionID(transID).WithUUID(uuid)

	var results []neoAggregatedConcept
	query := getNeoConceptReadQuery(uuid, &results)

	err := s.conn.CypherBatch([]*neoism.CypherQuery{query})
	if err != nil {
		logEntry.WithError(err).Error("Error executing neo4j read query")
		return ontology.NewAggregatedConcept{}, err
	}

	if len(results) == 0 {
		logEntry.Info("Concept not found in db")
		return ontology.NewAggregatedConcept{}, ConceptNotFoundErr
	}

	aggregatedConcept, err := results[0].ToAggregateConcept()
	if err != nil {
		logEntry.WithError(err).Error("Returned concept had no recognized type")
		return ontology.NewAggregatedConcept{}, err
	}

	logEntry.Debugf("Returned concept is %v", aggregatedConcept)
	return cleanAndSortAggregateConcept(aggregatedConcept), nil
}

func (s *ConceptService) Write(thing interface{}, transID string) (interface{}, error) {
	concept, ok := thing.(ontology.AggregatedConcept)
	if !ok {
		return nil, errors.New("wrong thing")
	}
	newConcept := transform.TransformToNewAggregateConcept(concept)
	return s.write(transID, newConcept)
}

func (s *ConceptService) write(tid string, aggregatedConceptToWrite ontology.NewAggregatedConcept) (ConceptChanges, error) {
	// Read the aggregated concept - We need read the entire model first. This is because if we unconcord a TME concept
	// then we need to add prefUUID to the lone node if it has been removed from the concordance listed against a Smartlogic concept

	aggregatedConceptToWrite = cleanSourceProperties(aggregatedConceptToWrite)
	requestSourceData := getSourceData(aggregatedConceptToWrite.SourceRepresentations)

	logEntry := logger.WithTransactionID(tid).WithUUID(aggregatedConceptToWrite.PrefUUID)

	requestHash, err := hashstructure.Hash(aggregatedConceptToWrite, nil)
	if err != nil {
		logEntry.WithError(err).Error("Error hashing json from request")
		return ConceptChanges{}, err
	}

	hashAsString := strconv.FormatUint(requestHash, 10)

	if err = validateObject(aggregatedConceptToWrite); err != nil {
		logEntry.WithError(err).Error("filed to validate aggregate concept")
		return ConceptChanges{}, err
	}
	exists := true
	existingConcept, err := s.read(aggregatedConceptToWrite.PrefUUID, tid)
	if err != nil {
		if !errors.Is(err, ConceptNotFoundErr) {
			logEntry.WithError(err).Error("Read request for existing concordance resulted in error")
			return ConceptChanges{}, err
		}
		exists = false
	}

	updateRecord := ConceptChanges{}
	var queryBatch []*neoism.CypherQuery
	if exists {
		queryBatch, updateRecord, err = s.handleExistingConcept(tid, aggregatedConceptToWrite, existingConcept, requestHash, requestSourceData, hashAsString)
		if err != nil {
			if errors.Is(err, ConceptNotChangedErr) {
				return ConceptChanges{}, nil
			}
			return ConceptChanges{}, err
		}
	} else {
		queryBatch, updateRecord, err = s.handleNewConcept(tid, aggregatedConceptToWrite, requestSourceData, hashAsString)
		if err != nil {
			return ConceptChanges{}, err
		}
	}

	if logger.Logger().Level == logrus.DebugLevel {
		logEntry.Debug("Executing " + strconv.Itoa(len(queryBatch)) + " queries")
		for _, query := range queryBatch {
			logEntry.Debug(fmt.Sprintf("Query: %v", query))
		}
	}

	// check that the issuer is not already related to a different org
	if aggregatedConceptToWrite.IssuedBy != "" {
		issuerQuery, err := s.getIssuerChangeQueries(tid, aggregatedConceptToWrite)
		if err != nil {
			return ConceptChanges{}, err
		}
		queryBatch = append(queryBatch, issuerQuery...)
	}

	if err = s.conn.CypherBatch(queryBatch); err != nil {
		logEntry.WithError(err).Error("Error executing neo4j write queries. Concept NOT written.")
		return ConceptChanges{}, err
	}

	logEntry.Info("Concept written to db")
	return updateRecord, nil
}

func (s *ConceptService) getIssuerChangeQueries(tid string, aggregatedConceptToWrite ontology.NewAggregatedConcept) ([]*neoism.CypherQuery, error) {
	var fiRes []map[string]string
	logEntry := logger.WithTransactionID(tid).WithUUID(aggregatedConceptToWrite.PrefUUID)
	queryBatch := []*neoism.CypherQuery{}
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
		logEntry.Error("Could not get existing issuer.")
		return nil, err
	}

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
		logEntry.WithField("alert_tag", "ConceptLoadingLedToDifferentIssuer").Info(msg)

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
	return queryBatch, nil
}

var ConceptNotChangedErr = errors.New("concept not changed")

func (s *ConceptService) handleExistingConcept(tid string, aggregatedConceptToWrite ontology.NewAggregatedConcept, existingConcept ontology.NewAggregatedConcept, requestHash uint64, requestSourceData map[string]string, hashAsString string) ([]*neoism.CypherQuery, ConceptChanges, error) {
	logEntry := logger.WithTransactionID(tid).WithUUID(aggregatedConceptToWrite.PrefUUID)
	updateRecord := ConceptChanges{}
	if existingConcept.AggregatedHash == "" {
		existingConcept.AggregatedHash = "0"
	}
	currentHash, err := strconv.ParseUint(existingConcept.AggregatedHash, 10, 64)
	if err != nil {
		logEntry.WithError(err).Info("Error whilst parsing existing concept hash")
		return nil, ConceptChanges{}, ConceptNotChangedErr
	}
	logEntry.Debugf("Currently stored concept has hash of %d", currentHash)
	logEntry.Debugf("Aggregated concept has hash of %d", requestHash)
	if currentHash == requestHash {
		logEntry.Info("This concept has not changed since most recent update")
		return nil, ConceptChanges{}, ConceptNotChangedErr
	}
	logEntry.Info("This concept is different to record stored in db, updating...")

	existingSourceData := getSourceData(existingConcept.SourceRepresentations)

	//Concept has been updated since last write, so need to send notification of all affected ids
	for _, source := range aggregatedConceptToWrite.SourceRepresentations {
		updateRecord.UpdatedIds = append(updateRecord.UpdatedIds, source.UUID)
	}

	//This filter will leave us with ids that were members of existing concordance but are NOT members of current concordance
	//They will need a new prefUUID node written
	conceptsToUnconcord := filterIdsThatAreUniqueToFirstMap(existingSourceData, requestSourceData)

	//This filter will leave us with ids that are members of current concordance payload but were not previously concorded to this concordance
	conceptsToTransferConcordance := filterIdsThatAreUniqueToFirstMap(requestSourceData, existingSourceData)

	//Handle scenarios for transferring source id from an existing concordance to this concordance
	var deletUUIDs []string
	if len(conceptsToTransferConcordance) > 0 {
		uuidsToDelete, changeEvent, err := s.handleTransferConcordance(conceptsToTransferConcordance, hashAsString, aggregatedConceptToWrite, tid)
		if err != nil {
			return nil, ConceptChanges{}, err
		}
		deletUUIDs = uuidsToDelete
		updateRecord.ChangedRecords = append(updateRecord.ChangedRecords, changeEvent...)
	}

	queryBatch := s.clearDownExistingNodes(aggregatedConceptToWrite)

	for idToUnconcord := range conceptsToUnconcord {
		for _, concept := range existingConcept.SourceRepresentations {
			if idToUnconcord == concept.UUID {
				//aggConcept := buildAggregateConcept(concept)
				//set this to 0 as otherwise it is empty
				//TODO fix this up at some point to do it properly?
				concept.Hash = "0"
				unconcordQuery := s.writeCanonicalNodeForUnconcordedConcepts(concept)
				queryBatch = append(queryBatch, unconcordQuery)

				//We will need to send a notification of ids that have been removed from current concordance
				updateRecord.UpdatedIds = append(updateRecord.UpdatedIds, idToUnconcord)

				//Unconcordance event for new concept notifications
				updateRecord.ChangedRecords = append(updateRecord.ChangedRecords, Event{
					ConceptType:   conceptsToUnconcord[idToUnconcord],
					ConceptUUID:   idToUnconcord,
					AggregateHash: hashAsString,
					TransactionID: tid,
					EventDetails: ConcordanceEvent{
						Type:  RemovedEvent,
						OldID: aggregatedConceptToWrite.PrefUUID,
						NewID: idToUnconcord,
					},
				})
			}
		}
	}

	for _, id := range deletUUIDs {
		queryBatch = append(queryBatch, deleteLonePrefUUID(id))
	}
	aggregatedConceptToWrite.AggregatedHash = hashAsString
	queryBatch = populateConceptQueries(queryBatch, aggregatedConceptToWrite)

	updateRecord.ChangedRecords = append(updateRecord.ChangedRecords, Event{
		ConceptType:   aggregatedConceptToWrite.Type,
		ConceptUUID:   aggregatedConceptToWrite.PrefUUID,
		AggregateHash: hashAsString,
		TransactionID: tid,
		EventDetails: ConceptEvent{
			Type: UpdatedEvent,
		},
	})
	return queryBatch, updateRecord, nil
}

func (s *ConceptService) handleNewConcept(tid string, aggregatedConceptToWrite ontology.NewAggregatedConcept, requestSourceData map[string]string, hashAsString string) ([]*neoism.CypherQuery, ConceptChanges, error) {
	uuidsToDelete, changeEvent, err := s.handleTransferConcordance(requestSourceData, hashAsString, aggregatedConceptToWrite, tid)
	if err != nil {
		return nil, ConceptChanges{}, err
	}
	updateRecord := ConceptChanges{}
	//Concept is new, send notification of all source ids
	for _, source := range aggregatedConceptToWrite.SourceRepresentations {
		updateRecord.UpdatedIds = append(updateRecord.UpdatedIds, source.UUID)
	}

	updateRecord.ChangedRecords = append(updateRecord.ChangedRecords, changeEvent...)
	updateRecord.ChangedRecords = append(updateRecord.ChangedRecords, Event{
		ConceptType:   aggregatedConceptToWrite.Type,
		ConceptUUID:   aggregatedConceptToWrite.PrefUUID,
		AggregateHash: hashAsString,
		TransactionID: tid,
		EventDetails: ConceptEvent{
			Type: UpdatedEvent,
		},
	})

	aggregatedConceptToWrite.AggregatedHash = hashAsString
	queryBatch := s.clearDownExistingNodes(aggregatedConceptToWrite)
	for _, id := range uuidsToDelete {
		queryBatch = append(queryBatch, deleteLonePrefUUID(id))
	}
	queryBatch = populateConceptQueries(queryBatch, aggregatedConceptToWrite)

	return queryBatch, updateRecord, nil
}

func validateObject(aggConcept ontology.NewAggregatedConcept) error {
	prefLabel, ok := aggConcept.GetPropString(ontology.PrefLabelProp)
	if !ok || prefLabel == "" {
		return formatError("prefLabel", aggConcept.PrefUUID)
	}
	if _, ok := constraintMap[aggConcept.Type]; !ok {
		return formatError("type", aggConcept.PrefUUID)
	}
	if aggConcept.SourceRepresentations == nil {
		return formatError("sourceRepresentation", aggConcept.PrefUUID)
	}
	for _, concept := range aggConcept.SourceRepresentations {
		authority, ok := concept.GetPropString(ontology.AuthorityProp)
		if !ok || authority == "" {
			return formatError("sourceRepresentation.authority", concept.UUID)
		}
		if concept.Type == "" {
			return formatError("sourceRepresentation.type", concept.UUID)
		}
		authorityValue, ok := concept.GetPropString(ontology.AuthorityValueProp)
		if !ok || authorityValue == "" {
			return formatError("sourceRepresentation.authorityValue", concept.UUID)
		}
		if _, ok := constraintMap[concept.Type]; !ok {
			return formatError("type", aggConcept.PrefUUID)
		}
	}
	return nil
}

func formatError(field string, uuid string) error {
	return requestError{details: "Invalid request, no " + field + " has been supplied"}
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

func (s *ConceptService) getEquivalentNodes(uuid string) ([]equivalenceResult, error) {
	var result []equivalenceResult
	equivQuery := &neoism.CypherQuery{
		Statement: `
					MATCH (t:Thing {uuid:{id}})
					OPTIONAL MATCH (t)-[:EQUIVALENT_TO]->(c)
					OPTIONAL MATCH (c)<-[eq:EQUIVALENT_TO]-(x:Thing)
					RETURN t.uuid as sourceUuid, labels(t) as types, c.prefUUID as prefUuid, t.authority as authority, COUNT(DISTINCT eq) as count`,
		Parameters: map[string]interface{}{
			"id": uuid,
		},
		Result: &result,
	}
	err := s.conn.CypherBatch([]*neoism.CypherQuery{equivQuery})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (s *ConceptService) handleSingleSourceTransfer(updatedSourceID string, updatedSourceType string, aggregateHash string, aggregatePrefUUID string, aggregateAuthority string, transID string) ([]string, []Event, error) {
	logEntry := logger.WithTransactionID(transID).WithUUID(aggregatePrefUUID)

	result, err := s.getEquivalentNodes(updatedSourceID)
	if err != nil {
		logEntry.WithError(err).Error("Requests for source nodes canonical information resulted in error")
		return nil, nil, err
	}

	if len(result) > 1 {
		//this scenario should never happen
		err = fmt.Errorf("Multiple source concepts found with matching uuid: %s", updatedSourceID)
		logEntry.Error(err.Error())
		return nil, nil, err
	}

	//source node does not currently exist in neo4j, nothing to tidy up
	if len(result) == 0 {
		logEntry.Info("No existing concordance record found")
		if updatedSourceID == aggregatePrefUUID {
			return nil, nil, nil
		}
		//concept does not exist, need update event
		return nil, []Event{
			{
				ConceptType:   updatedSourceType,
				ConceptUUID:   updatedSourceID,
				AggregateHash: aggregateHash,
				TransactionID: transID,
				EventDetails: ConceptEvent{
					Type: UpdatedEvent,
				},
			},
			{
				ConceptType:   updatedSourceType,
				ConceptUUID:   updatedSourceID,
				AggregateHash: aggregateHash,
				TransactionID: transID,
				EventDetails: ConcordanceEvent{
					Type:  AddedEvent,
					OldID: updatedSourceID,
					NewID: aggregatePrefUUID,
				},
			},
		}, err
	}

	entityEquivalence := result[0]
	conceptType, err := mapper.MostSpecificType(entityEquivalence.Types)
	if err != nil {
		logEntry.WithError(err).Errorf("could not return most specific type from source node: %v", entityEquivalence.Types)
		return nil, nil, err
	}

	logEntry.WithField("UUID", updatedSourceID).Debug("Existing prefUUID is " + entityEquivalence.PrefUUID + " equivalence count is " + strconv.Itoa(entityEquivalence.Equivalence))
	if entityEquivalence.Equivalence == 0 {
		// Source is old as exists in Neo4j without a prefNode. It can be transferred without issue
		return nil, nil, nil
	}
	if entityEquivalence.Equivalence == 1 {
		// Source exists in neo4j but is not concorded. It can be transferred without issue but its prefNode should be deleted
		if updatedSourceID != entityEquivalence.PrefUUID {
			// Source is only source concorded to non-matching prefUUID; scenario should NEVER happen
			err := fmt.Errorf("This source id: %s the only concordance to a non-matching node with prefUuid: %s", updatedSourceID, entityEquivalence.PrefUUID)
			logEntry.WithField("alert_tag", "ConceptLoadingDodgyData").Error(err)
			return nil, nil, err
		}
		logEntry.Debugf("Pref uuid node for source %s will need to be deleted as its source will be removed", updatedSourceID)
		//concordance added
		return []string{
				entityEquivalence.PrefUUID,
			}, []Event{{
				ConceptType:   conceptType,
				ConceptUUID:   updatedSourceID,
				AggregateHash: aggregateHash,
				TransactionID: transID,
				EventDetails: ConcordanceEvent{
					Type:  AddedEvent,
					OldID: updatedSourceID,
					NewID: aggregatePrefUUID,
				},
			}}, nil
	}

	if updatedSourceID == entityEquivalence.PrefUUID {
		if updatedSourceID != aggregatePrefUUID {
			if entityEquivalence.Authority != aggregateAuthority && stringInArr(entityEquivalence.Authority, concordancesSources) {
				logEntry.Debugf("Canonical node for main source %s will need to be deleted and all concordances will be transfered to the new concordance", updatedSourceID)
				// just delete the lone prefUUID node because the other concordances to
				// this node should already be in the new sourceRepresentations (aggregate-concept-transformer responsability)
				return []string{
						entityEquivalence.PrefUUID,
					}, []Event{{
						ConceptType:   conceptType,
						ConceptUUID:   updatedSourceID,
						AggregateHash: aggregateHash,
						TransactionID: transID,
						EventDetails: ConcordanceEvent{
							Type:  AddedEvent,
							OldID: updatedSourceID,
							NewID: aggregatePrefUUID,
						},
					}}, nil
			}
			// Source is prefUUID for a different concordance
			err := fmt.Errorf("Cannot currently process this record as it will break an existing concordance with prefUuid: %s", updatedSourceID)
			logEntry.WithField("alert_tag", "ConceptLoadingInvalidConcordance").Error(err)
			return nil, nil, err
		}
	}
	// Source was concorded to different concordance. Data on existing concordance is now out of date
	logEntry.WithField("alert_tag", "ConceptLoadingStaleData").Infof("Need to re-ingest concordance record for prefUuid: %s as source: %s has been removed.", entityEquivalence.PrefUUID, updatedSourceID)
	return nil, []Event{
		{
			ConceptType:   conceptType,
			ConceptUUID:   updatedSourceID,
			AggregateHash: aggregateHash,
			TransactionID: transID,
			EventDetails: ConcordanceEvent{
				Type:  RemovedEvent,
				OldID: entityEquivalence.PrefUUID,
				NewID: updatedSourceID,
			},
		},
		{
			ConceptType:   conceptType,
			ConceptUUID:   updatedSourceID,
			AggregateHash: aggregateHash,
			TransactionID: transID,
			EventDetails: ConcordanceEvent{
				Type:  AddedEvent,
				OldID: updatedSourceID,
				NewID: aggregatePrefUUID,
			},
		},
	}, nil
}

//Handle new source nodes that have been added to current concordance
func (s *ConceptService) handleTransferConcordance(conceptData map[string]string, aggregateHash string, newAggregatedConcept ontology.NewAggregatedConcept, transID string) ([]string, []Event, error) {

	uuidsToDelete := []string{}
	changeEvents := []Event{}
	aggregateAuthority := getCanonicalAuthority(newAggregatedConcept)
	aggregatePrefUUID := newAggregatedConcept.PrefUUID
	for updatedSourceID, updatedSourceType := range conceptData {
		uuids, events, err := s.handleSingleSourceTransfer(updatedSourceID, updatedSourceType, aggregateHash, aggregatePrefUUID, aggregateAuthority, transID)
		if err != nil {
			return nil, nil, err
		}
		uuidsToDelete = append(uuidsToDelete, uuids...)
		changeEvents = append(changeEvents, events...)
	}

	return uuidsToDelete, changeEvents, nil
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
	for _, sr := range ac.SourceRepresentations {
		deletePreviousSourceLabelsAndPropertiesQuery := &neoism.CypherQuery{
			Statement: fmt.Sprintf(`MATCH (t:Thing {uuid:{id}})
			MATCH (t)-[r]->(other)
			WHERE TYPE(r) IN {relations}
			REMOVE t:%s
			SET t={uuid:{id}}
			DELETE  r`, getLabelsToRemove()),
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
	createConceptQuery := &neoism.CypherQuery{
		Statement: fmt.Sprintf(`MERGE (n:Thing {prefUUID: {prefUUID}})
											set n={allprops}
											set n :%s`, getAllLabels(concept.Type)),
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

//Create concept nodes
func createNodeQueries(concept ontology.NewSourceConcept, uuid string) []*neoism.CypherQuery {
	var queryBatch []*neoism.CypherQuery
	var createConceptQuery *neoism.CypherQuery

	allProps := setProps(concept, uuid, true)
	createConceptQuery = &neoism.CypherQuery{
		Statement: fmt.Sprintf(`MERGE (n:Thing {uuid: {uuid}})
											set n={allprops}
											set n :%s`, getAllLabels(concept.Type)),
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
	createCanonicalNodeQuery := &neoism.CypherQuery{
		Statement: fmt.Sprintf(`
					MATCH (t:Thing{uuid:{prefUUID}})
					MERGE (n:Thing {prefUUID: {prefUUID}})
					MERGE (n)<-[:EQUIVALENT_TO]-(t)
					set n={allprops}
					set n :%s`, getAllLabels(concept.Type)),
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

//return existing labels
func getLabelsToRemove() string {
	var labelsToRemove string
	for i, conceptType := range ontology.ConceptLabels {
		labelsToRemove += conceptType
		if i+1 < len(ontology.ConceptLabels) {
			labelsToRemove += ":"
		}
	}
	return labelsToRemove
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

func cleanAndSortAggregateConcept(c ontology.NewAggregatedConcept) ontology.NewAggregatedConcept {

	deniedRelationProperties := map[string][]string{
		ontology.HasMembershipRoleRelation: {
			ontology.InceptionDateEpochProp,
			ontology.TerminationDateEpochProp,
		},
	}

	relationsToSort := map[string]bool{
		ontology.BroaderRelation:           true,
		ontology.ParentRelation:            true,
		ontology.ImpliedByRelation:         true,
		ontology.HasFocusRelation:          true,
		ontology.IsRelatedRelation:         true,
		ontology.SupersededByRelation:      true,
		ontology.HasMembershipRoleRelation: true,
	}
	for j := range c.SourceRepresentations {
		source := &c.SourceRepresentations[j]
		source.LastModifiedEpoch = 0
		for i := range source.Relations {
			rel := &source.Relations[i]
			if propsToClean, has := deniedRelationProperties[rel.Label]; has {
				for k := range rel.Connections {
					con := &rel.Connections[k]
					for _, prop := range propsToClean {
						delete(con.Properties, prop)
					}
				}
			}

			if !relationsToSort[rel.Label] {
				continue
			}
			sort.SliceStable(rel.Connections, func(k, l int) bool {
				return rel.Connections[k].UUID < rel.Connections[l].UUID
			})
		}
	}
	sort.SliceStable(c.SourceRepresentations, func(k, l int) bool {
		return c.SourceRepresentations[k].UUID < c.SourceRepresentations[l].UUID
	})
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
