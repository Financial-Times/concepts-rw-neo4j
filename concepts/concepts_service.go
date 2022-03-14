package concepts

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	cmneo4j "github.com/Financial-Times/cm-neo4j-driver"
	"github.com/Financial-Times/go-logger/v2"
	"github.com/Financial-Times/neo-model-utils-go/mapper"
	"github.com/mitchellh/hashstructure"
	"github.com/sirupsen/logrus"

	"github.com/Financial-Times/concepts-rw-neo4j/ontology"
	"github.com/Financial-Times/concepts-rw-neo4j/ontology/neo4j"
	"github.com/Financial-Times/concepts-rw-neo4j/ontology/transform"
)

const (
	//Event types
	UpdatedEvent = "CONCEPT_UPDATED"
	AddedEvent   = "CONCORDANCE_ADDED"
	RemovedEvent = "CONCORDANCE_REMOVED"
)

var ErrUnexpectedReadResult = errors.New("unexpected read result count")

var concordancesSources = []string{"ManagedLocation", "Smartlogic"}

// ConceptService - CypherDriver - CypherDriver
type ConceptService struct {
	driver *cmneo4j.Driver
	log    *logger.UPPLogger
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
func NewConceptService(driver *cmneo4j.Driver, log *logger.UPPLogger) ConceptService {
	return ConceptService{driver: driver, log: log}
}

// Initialise tries to create indexes and constraints if they are not already
// created.
func (s *ConceptService) Initialise() error {
	err := s.driver.EnsureIndexes(map[string]string{
		"Concept": "leiCode",
	})

	if err != nil {
		s.log.WithError(err).Error("Could not run db index")
		return err
	}

	err = s.driver.EnsureIndexes(map[string]string{
		"Thing":   "authorityValue",
		"Concept": "authorityValue",
	})
	if err != nil {
		s.log.WithError(err).Error("Could not run db index")
		return err
	}

	err = s.driver.EnsureConstraints(map[string]string{
		"Thing":                       "prefUUID",
		"Concept":                     "prefUUID",
		"Location":                    "iso31661",
		"NAICSIndustryClassification": "industryIdentifier",
	})
	if err != nil {
		s.log.WithError(err).Error("Could not run db constraints")
		return err
	}

	err = s.driver.EnsureConstraints(constraintMap)
	if err != nil {
		s.log.WithError(err).Error("Could not run db constraints")
		return err
	}

	return nil
}

func (s *ConceptService) Read(uuid string, transID string) (interface{}, bool, error) {
	newAggregatedConcept, exists, err := s.read(uuid, transID)
	if err != nil {
		return transform.OldAggregatedConcept{}, exists, err
	}
	aggregatedConcept, err := transform.ToOldAggregateConcept(newAggregatedConcept)
	s.log.WithTransactionID(transID).WithUUID(uuid).Debugf("Returned concept is %v", aggregatedConcept)
	return aggregatedConcept, exists, err
}

func (s *ConceptService) read(uuid string, transID string) (ontology.NewAggregatedConcept, bool, error) {
	query, neoAggregateConcept := neo4j.GetReadQuery(uuid)
	err := s.driver.Read(query)
	if errors.Is(err, cmneo4j.ErrNoResultsFound) {
		s.log.WithTransactionID(transID).WithUUID(uuid).Info("Concept not found in db")
		return ontology.NewAggregatedConcept{}, false, nil
	}
	if errors.Is(err, cmneo4j.ErrMultipleResultsFound) {
		s.log.WithTransactionID(transID).WithUUID(uuid).Errorf("read concept returned multiple rows, where one is expected")
		return ontology.NewAggregatedConcept{}, false, ErrUnexpectedReadResult
	}
	if err != nil {
		s.log.WithError(err).WithTransactionID(transID).WithUUID(uuid).Error("Error executing neo4j read query")
		return ontology.NewAggregatedConcept{}, false, err
	}

	newAggregatedConcept, logMsg, err := neoAggregateConcept.ToOntologyNewAggregateConcept(ontology.GetConfig())
	if err != nil {
		s.log.WithError(err).WithTransactionID(transID).WithUUID(uuid).Error(logMsg)
		return ontology.NewAggregatedConcept{}, false, err
	}

	return newAggregatedConcept, true, nil
}

func (s *ConceptService) Write(thing interface{}, transID string) (interface{}, error) {
	// Read the aggregated concept - We need read the entire model first. This is because if we unconcord a TME concept
	// then we need to add prefUUID to the lone node if it has been removed from the concordance listed against a Smartlogic concept
	oldAggregatedConcept := thing.(transform.OldAggregatedConcept)
	aggregatedConceptToWrite, err := transform.ToNewAggregateConcept(oldAggregatedConcept)
	if err != nil {
		return ConceptChanges{}, err
	}

	aggregatedConceptToWrite = cleanSourceProperties(aggregatedConceptToWrite)
	requestSourceData := getSourceData(aggregatedConceptToWrite.SourceRepresentations)

	requestHash, err := hashstructure.Hash(aggregatedConceptToWrite, nil)
	if err != nil {
		s.log.WithError(err).WithTransactionID(transID).WithUUID(aggregatedConceptToWrite.PrefUUID).Error("Error hashing json from request")
		return ConceptChanges{}, err
	}

	hashAsString := strconv.FormatUint(requestHash, 10)

	if err = s.validateObject(aggregatedConceptToWrite, transID); err != nil {
		return ConceptChanges{}, err
	}

	existingAggregateConcept, exists, err := s.read(aggregatedConceptToWrite.PrefUUID, transID)
	if err != nil {
		s.log.WithError(err).WithTransactionID(transID).WithUUID(aggregatedConceptToWrite.PrefUUID).Error("Read request for existing concordance resulted in error")
		return ConceptChanges{}, err
	}

	var queryBatch []*cmneo4j.Query
	var prefUUIDsToBeDeleted []string
	var updatedUUIDList []string
	var orphanConcepts []ontology.NewConcept
	updateRecord := ConceptChanges{}
	if exists {
		if existingAggregateConcept.AggregatedHash == "" {
			existingAggregateConcept.AggregatedHash = "0"
		}
		currentHash, err := strconv.ParseUint(existingAggregateConcept.AggregatedHash, 10, 64)
		if err != nil {
			s.log.WithError(err).WithTransactionID(transID).WithUUID(aggregatedConceptToWrite.PrefUUID).Info("Error whilst parsing existing concept hash")
			return updateRecord, nil
		}
		s.log.WithTransactionID(transID).WithUUID(aggregatedConceptToWrite.PrefUUID).Debugf("Currently stored concept has hash of %d", currentHash)
		s.log.WithTransactionID(transID).WithUUID(aggregatedConceptToWrite.PrefUUID).Debugf("Aggregated concept has hash of %d", requestHash)
		if currentHash == requestHash {
			s.log.WithTransactionID(transID).WithUUID(aggregatedConceptToWrite.PrefUUID).Info("This concept has not changed since most recent update")
			return updateRecord, nil
		}
		s.log.WithTransactionID(transID).WithUUID(aggregatedConceptToWrite.PrefUUID).Info("This concept is different to record stored in db, updating...")

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
			prefUUIDsToBeDeleted, err = s.handleTransferConcordance(conceptsToTransferConcordance, &updateRecord, hashAsString, aggregatedConceptToWrite, transID)
			if err != nil {
				return updateRecord, err
			}

		}

		for idToUnconcord := range conceptsToUnconcord {
			for _, concept := range existingAggregateConcept.SourceRepresentations {
				if idToUnconcord == concept.UUID {
					//aggConcept := buildAggregateConcept(concept)
					//set this to 0 as otherwise it is empty
					//TODO fix this up at some point to do it properly?
					concept.Hash = "0"
					orphanConcepts = append(orphanConcepts, concept)

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
		prefUUIDsToBeDeleted, err = s.handleTransferConcordance(requestSourceData, &updateRecord, hashAsString, aggregatedConceptToWrite, transID)
		if err != nil {
			return updateRecord, err
		}

		//Concept is new, send notification of all source ids
		for _, source := range aggregatedConceptToWrite.SourceRepresentations {
			updatedUUIDList = append(updatedUUIDList, source.UUID)
		}
	}

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

	// for the new concept we remove all previous concept to concept relations and all properties for the source concepts.
	// as well as the source node to canonical node relationship.
	clearDownQuery := neo4j.ClearExistingConcept(aggregatedConceptToWrite)
	queryBatch = append(queryBatch, clearDownQuery...)

	// for source concepts that were unconcorded we recreate the canonical node
	for _, concept := range orphanConcepts {
		canonical := neo4j.SourceToCanonical(concept)
		unconcordQuery := neo4j.WriteCanonicalNodeForUnconcordedConcepts(canonical, concept.UUID)
		s.log.WithTransactionID(transID).WithUUID(concept.UUID).Warn("Creating prefUUID node for unconcorded concept")
		queryBatch = append(queryBatch, unconcordQuery)
	}

	// for source concepts that were concorded we remove the undesired canonical node
	for _, uuid := range prefUUIDsToBeDeleted {
		queryBatch = append(queryBatch, removeLoneCanonicalNode(uuid))
	}

	aggregatedConceptToWrite.AggregatedHash = hashAsString
	writeQueries := neo4j.WriteCanonicalConceptQueries(aggregatedConceptToWrite)
	queryBatch = append(queryBatch, writeQueries...)

	// check that the issuer is not already related to a different org
	if aggregatedConceptToWrite.IssuedBy != "" {
		var fiRes []map[string]string
		issuerQuery := &cmneo4j.Query{
			Cypher: `
					MATCH (issuer:Thing {uuid: $issuerUUID})<-[:ISSUED_BY]-(fi)
					RETURN fi.uuid AS fiUUID
				`,
			Params: map[string]interface{}{
				"issuerUUID": aggregatedConceptToWrite.IssuedBy,
			},
			Result: &fiRes,
		}

		err := s.driver.Read(issuerQuery)
		if err != nil && !errors.Is(err, cmneo4j.ErrNoResultsFound) {
			s.log.WithError(err).
				WithTransactionID(transID).
				WithUUID(aggregatedConceptToWrite.PrefUUID).
				Error("Could not get existing issuer.")
			return updateRecord, err
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
			s.log.WithTransactionID(transID).
				WithUUID(aggregatedConceptToWrite.PrefUUID).
				WithField("alert_tag", "ConceptLoadingLedToDifferentIssuer").Info(msg)

			deleteIssuerRelations := &cmneo4j.Query{
				Cypher: `
					MATCH (issuer:Thing {uuid: $issuerUUID})
					MATCH (fi:Thing {uuid: $fiUUID})
					MATCH (issuer)<-[issuerRel:ISSUED_BY]-(fi)
					DELETE issuerRel
				`,
				Params: map[string]interface{}{
					"issuerUUID": aggregatedConceptToWrite.IssuedBy,
					"fiUUID":     fiUUID,
				},
			}
			queryBatch = append(queryBatch, deleteIssuerRelations)
		}
	}

	s.log.WithTransactionID(transID).WithUUID(aggregatedConceptToWrite.PrefUUID).Debug("Executing " + strconv.Itoa(len(queryBatch)) + " queries")
	if s.log.IsLevelEnabled(logrus.DebugLevel) {
		for _, query := range queryBatch {
			s.log.WithTransactionID(transID).WithUUID(aggregatedConceptToWrite.PrefUUID).Debug(fmt.Sprintf("Query: %v", query))
		}
	}

	if err = s.driver.Write(queryBatch...); err != nil {
		s.log.WithError(err).WithTransactionID(transID).WithUUID(aggregatedConceptToWrite.PrefUUID).Error("Error executing neo4j write queries. Concept NOT written.")
		return updateRecord, err
	}

	s.log.WithTransactionID(transID).WithUUID(aggregatedConceptToWrite.PrefUUID).Info("Concept written to db")
	return updateRecord, nil
}

func (s *ConceptService) validateObject(aggConcept ontology.NewAggregatedConcept, transID string) error {
	if aggConcept.PrefLabel == "" {
		return requestError{s.formatError("prefLabel", aggConcept.PrefUUID, transID)}
	}

	if _, ok := constraintMap[aggConcept.Type]; !ok {
		return requestError{s.formatError("type", aggConcept.PrefUUID, transID)}
	}

	if aggConcept.SourceRepresentations == nil {
		return requestError{s.formatError("sourceRepresentation", aggConcept.PrefUUID, transID)}
	}

	if err := ontology.GetConfig().ValidateProperties(aggConcept.Properties); err != nil {
		return requestError{err.Error()}
	}

	for _, sourceConcept := range aggConcept.SourceRepresentations {
		if err := sourceConcept.Validate(); err != nil {
			if errors.Is(err, ontology.ErrUnknownAuthority) {
				s.log.WithTransactionID(transID).WithUUID(aggConcept.PrefUUID).Debugf("Unknown authority supplied in the request: %s", sourceConcept.Authority)
			} else {
				s.log.WithError(err).WithTransactionID(transID).WithUUID(sourceConcept.UUID).Error("Validation of payload failed")
			}

			return requestError{err.Error()}
		}

		if sourceConcept.Type == "" {
			return requestError{s.formatError("sourceRepresentation.type", sourceConcept.UUID, transID)}
		}

		if _, ok := constraintMap[sourceConcept.Type]; !ok {
			return requestError{s.formatError("type", aggConcept.PrefUUID, transID)}
		}
	}

	return nil
}

func (s *ConceptService) formatError(field, uuid, transID string) string {
	err := errors.New("invalid request, no " + field + " has been supplied")
	s.log.WithError(err).WithTransactionID(transID).WithUUID(uuid).Error("Validation of payload failed")
	return err.Error()
}

// Handle new source nodes that have been added to current concordance
// nolint:gocognit
func (s *ConceptService) handleTransferConcordance(conceptData map[string]string, updateRecord *ConceptChanges, aggregateHash string, newAggregatedConcept ontology.NewAggregatedConcept, transID string) ([]string, error) {
	var canonicalUUIDsToRemove []string
	for updatedSourceID := range conceptData {
		equivQuery, result := readCanonicalStats(updatedSourceID)

		err := s.driver.Read(equivQuery)
		if err != nil && !errors.Is(err, cmneo4j.ErrNoResultsFound) {
			s.log.WithError(err).WithTransactionID(transID).WithUUID(newAggregatedConcept.PrefUUID).Error("Requests for source nodes canonical information resulted in error")
			return nil, err
		}

		//source node does not currently exist in neo4j, nothing to tidy up
		if len(*result) == 0 {
			s.log.WithTransactionID(transID).WithUUID(newAggregatedConcept.PrefUUID).Info("No existing concordance record found")
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
		} else if len(*result) > 1 {
			//this scenario should never happen
			err = fmt.Errorf("Multiple source concepts found with matching uuid: %s", updatedSourceID)
			s.log.WithTransactionID(transID).WithUUID(newAggregatedConcept.PrefUUID).Error(err.Error())
			return nil, err
		}

		entityEquivalence := (*result)[0]
		conceptType, err := mapper.MostSpecificType(entityEquivalence.Types)
		if err != nil {
			s.log.WithError(err).WithTransactionID(transID).WithUUID(newAggregatedConcept.PrefUUID).Errorf("could not return most specific type from source node: %v", entityEquivalence.Types)
			return nil, err
		}

		s.log.WithField("UUID", updatedSourceID).Debug("Existing prefUUID is " + entityEquivalence.PrefUUID + " equivalence count is " + strconv.Itoa(entityEquivalence.Equivalence))
		if entityEquivalence.Equivalence == 0 {
			// Source is old as exists in Neo4j without a prefNode. It can be transferred without issue
			continue
		} else if entityEquivalence.Equivalence == 1 {
			// Source exists in neo4j but is not concorded. It can be transferred without issue but its prefNode should be deleted
			if updatedSourceID == entityEquivalence.PrefUUID {
				s.log.WithTransactionID(transID).WithUUID(newAggregatedConcept.PrefUUID).Debugf("Pref uuid node for source %s will need to be deleted as its source will be removed", updatedSourceID)
				canonicalUUIDsToRemove = append(canonicalUUIDsToRemove, entityEquivalence.PrefUUID)
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
				s.log.WithTransactionID(transID).WithUUID(newAggregatedConcept.PrefUUID).WithField("alert_tag", "ConceptLoadingDodgyData").Error(err)
				return nil, err
			}
		} else {
			if updatedSourceID == entityEquivalence.PrefUUID {
				if updatedSourceID != newAggregatedConcept.PrefUUID {
					authority := newAggregatedConcept.GetCanonicalAuthority()
					if entityEquivalence.Authority != authority && stringInArr(entityEquivalence.Authority, concordancesSources) {
						s.log.WithTransactionID(transID).WithUUID(newAggregatedConcept.PrefUUID).Debugf("Canonical node for main source %s will need to be deleted and all concordances will be transferred to the new concordance", updatedSourceID)
						// just delete the lone prefUUID node because the other concordances to
						// this node should already be in the new sourceRepresentations (aggregate-concept-transformer responsability)
						canonicalUUIDsToRemove = append(canonicalUUIDsToRemove, entityEquivalence.PrefUUID)
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
					s.log.WithTransactionID(transID).WithUUID(newAggregatedConcept.PrefUUID).WithField("alert_tag", "ConceptLoadingInvalidConcordance").Error(err)
					return nil, err
				}
			} else {
				// Source was concorded to different concordance. Data on existing concordance is now out of date
				s.log.WithTransactionID(transID).WithUUID(newAggregatedConcept.PrefUUID).WithField("alert_tag", "ConceptLoadingStaleData").Infof("Need to re-ingest concordance record for prefUuid: %s as source: %s has been removed.", entityEquivalence.PrefUUID, updatedSourceID)

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
	return canonicalUUIDsToRemove, nil
}

type equivalenceResult struct {
	SourceUUID  string   `json:"sourceUuid"`
	PrefUUID    string   `json:"prefUuid"`
	Types       []string `json:"types"`
	Equivalence int      `json:"count"`
	Authority   string   `json:"authority"`
}

// readCanonicalStats will generate a Neo4j query that will read equivalenceResult that contains a count of the concorded source concepts
// This information is used for determining how to proseed when concording concepts
func readCanonicalStats(uuid string) (*cmneo4j.Query, *[]equivalenceResult) {
	var result []equivalenceResult
	query := &cmneo4j.Query{
		Cypher: `
					MATCH (t:Thing {uuid:$id})
					OPTIONAL MATCH (t)-[:EQUIVALENT_TO]->(c)
					OPTIONAL MATCH (c)<-[eq:EQUIVALENT_TO]-(x:Thing)
					RETURN t.uuid as sourceUuid, labels(t) as types, c.prefUUID as prefUuid, t.authority as authority, COUNT(DISTINCT eq) as count`,
		Params: map[string]interface{}{
			"id": uuid,
		},
		Result: &result,
	}
	return query, &result
}

// removeLoneCanonicalNode will detach and remove the canonical node for specified prefUUID
// Used when concepts are concorded, and we need to clean up the unnecessary canonical nodes.
func removeLoneCanonicalNode(prefUUID string) *cmneo4j.Query {
	equivQuery := &cmneo4j.Query{
		Cypher: `MATCH (t:Thing {prefUUID:$id}) DETACH DELETE t`,
		Params: map[string]interface{}{
			"id": prefUUID,
		},
	}
	return equivQuery
}

//extract uuids of the source concepts
func getSourceData(sourceConcepts []ontology.NewConcept) map[string]string {
	conceptData := make(map[string]string)
	for _, concept := range sourceConcepts {
		conceptData[concept.UUID] = concept.Type
	}
	return conceptData
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

//DecodeJSON - decode json
func (s *ConceptService) DecodeJSON(dec *json.Decoder) (interface{}, string, error) {
	sub := transform.OldAggregatedConcept{}
	err := dec.Decode(&sub)
	return sub, sub.PrefUUID, err
}

//Check - checker
func (s *ConceptService) Check() error {
	return s.driver.VerifyWriteConnectivity()
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

// cleanSourceProperties removes all properties from source concepts that are not stored in source nodes
// TODO: investigate why are we doing this.
func cleanSourceProperties(c ontology.NewAggregatedConcept) ontology.NewAggregatedConcept {
	var cleanSources []ontology.NewConcept
	for _, source := range c.SourceRepresentations {
		cleanConcept := ontology.NewConcept{
			Relationships:  source.Relationships,
			UUID:           source.UUID,
			PrefLabel:      source.PrefLabel,
			Type:           source.Type,
			Authority:      source.Authority,
			AuthorityValue: source.AuthorityValue,
			IssuedBy:       source.IssuedBy,
			FigiCode:       source.FigiCode,
			IsDeprecated:   source.IsDeprecated,
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
