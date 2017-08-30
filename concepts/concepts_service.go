package concepts

import (
	"encoding/json"

	"github.com/Financial-Times/neo-model-utils-go/mapper"

	"fmt"
	"time"

	"errors"
	"strconv"

	"github.com/Financial-Times/neo-utils-go/neoutils"
	log "github.com/Sirupsen/logrus"
	"github.com/jmcvetta/neoism"
)

//Service - CypherDriver - CypherDriver
type Service struct {
	conn neoutils.NeoConnection
	hc   *neoutils.AsyncHealthcheck
}

//NewConceptService instantiate driver
func NewConceptService(cypherRunner neoutils.NeoConnection, healthcheck *neoutils.AsyncHealthcheck) Service {
	return Service{cypherRunner, healthcheck}
}

//Initialise - Would this be better as an extension in Neo4j? i.e. that any Thing has this constraint added on creation
func (s Service) Initialise() error {

	s.hc.Initialise(s.conn)

	err := s.conn.EnsureIndexes(map[string]string{
		"Identifier": "value",
		"Thing":      "prefUUID",
	})
	if err != nil {
		log.WithError(err).Error("Could not run db index")
		return err
	}

	err = s.conn.EnsureIndexes(map[string]string{
		"Thing":   "authorityValue",
		"Concept": "authorityValue",
	})
	if err != nil {
		log.WithError(err).Error("Could not run db index")
		return err
	}
	return s.conn.EnsureConstraints(constraintMap)
}

type neoAggregatedConcept struct {
	PrefUUID              string       `json:"prefUUID,omitempty"`
	PrefLabel             string       `json:"prefLabel"`
	Types                 []string     `json:"types"`
	Aliases               []string     `json:"aliases,omitempty"`
	Strapline             string       `json:"strapline,omitempty"`
	DescriptionXML        string       `json:"descriptionXML,omitempty"`
	ImageURL              string       `json:"imageUrl,omitempty"`
	SourceRepresentations []neoConcept `json:"sourceRepresentations"`
	Authority             string       `json:"authority,omitempty"`
	AuthorityValue        string       `json:"authorityValue,omitempty"`
	LastModifiedEpoch     int          `json:"lastModifiedEpoch,omitempty"`
}

type neoConcept struct {
	UUID              string   `json:"uuid,omitempty"`
	PrefUUID          string   `json:"prefUUID,omitempty"`
	Types             []string `json:"types,omitempty"`
	PrefLabel         string   `json:"prefLabel,omitempty"`
	Authority         string   `json:"authority,omitempty"`
	AuthorityValue    string   `json:"authorityValue,omitempty"`
	LastModifiedEpoch int      `json:"lastModifiedEpoch,omitempty"`
	Aliases           []string `json:"aliases,omitempty"`
	ParentUUIDs       []string `json:"parentUUIDs,omitempty"`
	Strapline         string   `json:"strapline,omitempty"`
	ImageURL          string   `json:"imageUrl,omitempty"`
	DescriptionXML    string   `json:"descriptionXML,omitempty"`
}

type equivalenceResult struct {
	SourceUuid  string `json:"sourceUuid"`
	PrefUuid    string `json:"prefUuid"`
	Equivalence int    `json:"count"`
}

//Read - read service
func (s Service) Read(uuid string, transId string) (interface{}, bool, error) {
	results := []neoAggregatedConcept{}

	query := &neoism.CypherQuery{
		Statement: `	MATCH (canonical:Thing {prefUUID:{uuid}})<-[:EQUIVALENT_TO]-(node:Thing)
				OPTIONAL MATCH (node)-[HAS_PARENT]->(parent:Thing)
				WITH canonical.prefUUID as prefUUID, canonical.prefLabel as prefLabel, labels(canonical) as types, canonical.aliases as aliases,
				canonical.descriptionXML as descriptionXML, canonical.strapline as strapline, canonical.imageUrl as imageUrl,
				{uuid:node.uuid, prefLabel:node.prefLabel, authority:node.authority, authorityValue: node.authorityValue,
				types: labels(node), lastModifiedEpoch: node.lastModifiedEpoch, aliases: node.aliases, descriptionXML: node.descriptionXML,
				imageUrl: node.imageUrl, strapline: node.strapline, parentUUIDs:collect(parent.uuid)} as sources
				RETURN prefUUID, prefLabel, types, aliases, descriptionXML, strapline, imageUrl, collect(sources) as sourceRepresentations `,
		Parameters: map[string]interface{}{
			"uuid": uuid,
		},
		Result: &results,
	}

	err := s.conn.CypherBatch([]*neoism.CypherQuery{query})
	if err != nil {
		log.WithError(err).WithFields(log.Fields{"UUID": uuid, "transaction_id": transId}).Error("Error executing neo4j read query")
		return AggregatedConcept{}, false, err
	}

	if len(results) == 0 {
		log.WithFields(log.Fields{"UUID": uuid, "transaction_id": transId}).Info("Concept not found in db")
		return AggregatedConcept{}, false, nil
	}
	typeName, err := mapper.MostSpecificType(results[0].Types)
	if err != nil {
		log.WithError(err).WithFields(log.Fields{"UUID": uuid, "transaction_id": transId}).Error("Returned concept had no recognized type")
		return AggregatedConcept{}, false, err
	}

	var sourceConcepts []Concept
	aggregatedConcept := AggregatedConcept{
		PrefUUID:       results[0].PrefUUID,
		PrefLabel:      results[0].PrefLabel,
		Type:           typeName,
		ImageURL:       results[0].ImageURL,
		DescriptionXML: results[0].DescriptionXML,
		Strapline:      results[0].Strapline,
		Aliases:        results[0].Aliases,
	}

	for _, srcConcept := range results[0].SourceRepresentations {
		var concept Concept
		conceptType, err := mapper.MostSpecificType(srcConcept.Types)
		if err != nil {
			log.WithError(err).WithFields(log.Fields{"UUID": srcConcept.UUID, "transaction_id": transId}).Error("Returned source concept had no recognized type")
			return AggregatedConcept{}, false, err
		}
		if len(srcConcept.Aliases) > 0 {
			concept.Aliases = srcConcept.Aliases
		}

		uuids := []string{}

		if len(srcConcept.ParentUUIDs) > 0 {
			//TODO do this differently but I get a "" back from the cypher!
			for _, uuid := range srcConcept.ParentUUIDs {
				if uuid != "" {
					uuids = append(uuids, uuid)
				}
			}
			if len(uuids) > 0 {
				concept.ParentUUIDs = uuids
			}
		}
		concept.UUID = srcConcept.UUID
		concept.PrefLabel = srcConcept.PrefLabel
		concept.Authority = srcConcept.Authority
		concept.AuthorityValue = srcConcept.AuthorityValue
		concept.Type = conceptType
		concept.LastModifiedEpoch = srcConcept.LastModifiedEpoch
		concept.ImageURL = srcConcept.ImageURL
		concept.Strapline = srcConcept.Strapline
		concept.DescriptionXML = srcConcept.DescriptionXML

		sourceConcepts = append(sourceConcepts, concept)
	}

	aggregatedConcept.SourceRepresentations = sourceConcepts

	log.WithFields(log.Fields{"UUID": uuid, "transaction_id": transId}).Debugf("Returned concept is %v", aggregatedConcept)

	return aggregatedConcept, true, nil
}

func (s Service) Write(thing interface{}, transId string) error {
	// Read the aggregated concept - We need read the entire model first. This is because if we unconcord a TME concept
	// then we need to add prefUUID to the lode node if it has been removed from the concordance listed against a smart logic concept
	aggregatedConceptToWrite := thing.(AggregatedConcept)

	existingConcept, exists, err := s.Read(aggregatedConceptToWrite.PrefUUID, transId)
	if err != nil {
		log.WithError(err).WithFields(log.Fields{"UUID": aggregatedConceptToWrite.PrefUUID, "transaction_id": transId}).Error("Read request for existing concordance resulted in error")
		return err
	}

	err = validateObject(aggregatedConceptToWrite, transId)
	if err != nil {
		return err
	}

	var updatedSourceIds []string
	for _, updatedSource := range aggregatedConceptToWrite.SourceRepresentations {
		if updatedSource.UUID != aggregatedConceptToWrite.PrefUUID {
			updatedSourceIds = append(updatedSourceIds, updatedSource.UUID)
		}
	}

	existingAggregateConcept := existingConcept.(AggregatedConcept)
	var existingSourceIds []string
	for _, existingSource := range existingAggregateConcept.SourceRepresentations {
		if existingSource.UUID != existingAggregateConcept.PrefUUID {
			existingSourceIds = append(existingSourceIds, existingSource.UUID)
		}
	}

	var listToUnconcord []string
	if exists {
		//This filter will leave us with ids that were members of existing concordance but are NOT members of current concordance
		//They will need a new prefUUID node written
		listToUnconcord = filterIdsThatAreUniqueToFirstList(existingSourceIds, updatedSourceIds)
	}

	//This filter will leave us with ids that are members of current concordance payload but were not previously concorded to this concordance
	listToTransferConcordance := filterIdsThatAreUniqueToFirstList(updatedSourceIds, existingSourceIds)

	var prefUUIDsToBeDeletedQueryBatch []*neoism.CypherQuery
	//Handle scenarios for transferring source id from an existing concordance to this concordance
	if len(listToTransferConcordance) > 0 {
		prefUUIDsToBeDeletedQueryBatch, err = s.handleTransferConcordance(listToTransferConcordance, aggregatedConceptToWrite.PrefUUID, transId)
		if err != nil {
			return err
		}
	}

	var queryBatch []*neoism.CypherQuery

	clearDownQuery := s.clearDownExistingNodes(aggregatedConceptToWrite)

	for _, query := range clearDownQuery {
		queryBatch = append(queryBatch, query)
	}

	// Create a concept from the canonical information - WITH NO UUID
	concept := Concept{
		PrefLabel:      aggregatedConceptToWrite.PrefLabel,
		Aliases:        aggregatedConceptToWrite.Aliases,
		Strapline:      aggregatedConceptToWrite.Strapline,
		DescriptionXML: aggregatedConceptToWrite.DescriptionXML,
		ImageURL:       aggregatedConceptToWrite.ImageURL,
		Type:           aggregatedConceptToWrite.Type,
	}

	// Create the canonical node
	queryBatch = append(queryBatch, createNodeQueries(concept, aggregatedConceptToWrite.PrefUUID, "")...)

	// Repopulate
	for _, concept := range aggregatedConceptToWrite.SourceRepresentations {
		queryBatch = append(queryBatch, createNodeQueries(concept, "", concept.UUID)...)

		equivQuery := &neoism.CypherQuery{
			Statement: `MATCH (t:Thing {uuid:{uuid}}), (c:Thing {prefUUID:{prefUUID}})
			MERGE (t)-[:EQUIVALENT_TO]->(c)`,
			Parameters: map[string]interface{}{
				"uuid":     concept.UUID,
				"prefUUID": aggregatedConceptToWrite.PrefUUID,
			},
		}
		queryBatch = append(queryBatch, equivQuery)

	}

	if len(listToUnconcord) > 0 {
		for _, idToUnconcord := range listToUnconcord {
			for _, concept := range existingAggregateConcept.SourceRepresentations {
				if idToUnconcord == concept.UUID {
					unconcordQuery := s.writeConcordedNodeForUnconcordedConcepts(concept)
					queryBatch = append(queryBatch, unconcordQuery)
				}
			}
		}
	}

	if len(prefUUIDsToBeDeletedQueryBatch) > 0 {
		for _, query := range prefUUIDsToBeDeletedQueryBatch {
			queryBatch = append(queryBatch, query)
		}
	}

	log.WithFields(log.Fields{"UUID": aggregatedConceptToWrite.PrefUUID, "transaction_id": transId}).Debug("Executing " + strconv.Itoa(len(queryBatch)) + " queries")
	for _, query := range queryBatch {
		log.WithFields(log.Fields{"UUID": aggregatedConceptToWrite.PrefUUID, "transaction_id": transId}).Debug(fmt.Sprintf("Query: %s", query))
	}

	// TODO: Handle Constraint error properly but having difficulties with *neoutils.ConstraintViolationError
	err = s.conn.CypherBatch(queryBatch)
	if err != nil {
		return err
	} else {
		log.WithFields(log.Fields{"UUID": aggregatedConceptToWrite.PrefUUID, "transaction_id": transId}).Info("Concept written to db")
		return nil
	}
	return nil
}

func validateObject(aggConcept AggregatedConcept, transId string) error {
	if aggConcept.PrefLabel == "" {
		return requestError{formatError("prefLabel", aggConcept.PrefUUID, transId)}
	}
	if _, ok := constraintMap[aggConcept.Type]; !ok {
		return requestError{formatError("type", aggConcept.PrefUUID, transId)}
	}
	if aggConcept.SourceRepresentations == nil {
		return requestError{formatError("sourceRepresentation", aggConcept.PrefUUID, transId)}
	}
	for _, concept := range aggConcept.SourceRepresentations {
		// Is Authority recognised?
		if _, ok := authorityToIdentifierLabelMap[concept.Authority]; !ok {
			log.WithField("UUID", aggConcept.PrefUUID).Debug("Unknown authority, therefore unable to add the relevant Identifier node: %s", concept.Authority)
		}
		if concept.PrefLabel == "" {
			return requestError{formatError("sourceRepresentation.prefLabel", concept.UUID, transId)}
		}
		if concept.Type == "" {
			return requestError{formatError("sourceRepresentation.type", concept.UUID, transId)}
		}
		if concept.AuthorityValue == "" {
			return requestError{formatError("sourceRepresentation.authorityValue", concept.UUID, transId)}
		}
		if _, ok := constraintMap[concept.Type]; !ok {
			return requestError{formatError("type", aggConcept.PrefUUID, transId)}
		}
	}
	return nil
}

func formatError(field string, uuid string, transId string) string {
	err := errors.New("Invalid request, no " + field + " has been supplied")
	log.WithError(err).WithFields(log.Fields{"UUID": uuid, "transaction_id": transId}).Error("Validation of payload failed")
	return err.Error()
}

func filterIdsThatAreUniqueToFirstList(firstListIds []string, secondListIds []string) []string {
	//Loop through both lists to find id which is present in first list but not in the second
	var idIsUniqueToFirstList = true
	needToBeHandled := []string{}
	for _, firstId := range firstListIds {
		for _, secondId := range secondListIds {
			if firstId == secondId {
				//Id is present in both lists
				idIsUniqueToFirstList = false
			}
		}
		if idIsUniqueToFirstList == true {
			needToBeHandled = append(needToBeHandled, firstId)
		}
		idIsUniqueToFirstList = true
	}
	return needToBeHandled
}

func (s Service) handleTransferConcordance(updatedSourceIds []string, prefUUID string, transId string) ([]*neoism.CypherQuery, error) {
	result := []equivalenceResult{}

	deleteLonePrefUuidQueries := []*neoism.CypherQuery{}

	for _, updatedSourceId := range updatedSourceIds {
		equivQuery := &neoism.CypherQuery{
			Statement: `MATCH (t:Thing {uuid:{uuid}}) OPTIONAL MATCH (t)-[:EQUIVALENT_TO]->(c) OPTIONAL MATCH (c)<-[eq:EQUIVALENT_TO]-(x:Thing) RETURN t.uuid as sourceUuid, c.prefUUID as prefUuid, COUNT(DISTINCT eq) as count`,
			Parameters: map[string]interface{}{
				"uuid": updatedSourceId,
			},
			Result: &result,
		}

		err := s.conn.CypherBatch([]*neoism.CypherQuery{equivQuery})
		if err != nil {
			log.WithError(err).WithFields(log.Fields{"UUID": prefUUID, "transaction_id": transId}).Error("Requests for source nodes canonical information resulted in error")
			return deleteLonePrefUuidQueries, err
		}

		if len(result) == 0 {
			log.WithFields(log.Fields{"UUID": prefUUID, "transaction_id": transId}).Debug("No existing concordance found")
			break
		} else if len(result) > 1 {
			err = errors.New("Multiple concepts found with matching uuid!")
			log.WithError(err).WithField("UUID", updatedSourceId)
			return deleteLonePrefUuidQueries, err
		}

		log.WithField("UUID", result[0].SourceUuid).Debug("Existing prefUUID is " + result[0].PrefUuid + " equivalence count is " + strconv.Itoa(result[0].Equivalence))
		// Source has no existing concordance and will be handled by clearDownExistingNodes function
		if result[0].Equivalence == 0 {
			break
		} else if result[0].Equivalence == 1 {
			// Source has existing concordance to itself, after transfer old pref uuid node will need to be cleaned up
			if result[0].SourceUuid == result[0].PrefUuid {
				log.WithField("UUID", result[0].SourceUuid).Debug("Pref uuid node will need to be deleted")
				deleteLonePrefUuidQueries = append(deleteLonePrefUuidQueries, deleteLonePrefUuid(result[0].PrefUuid))
				break
			} else {
				// Source is only source concorded to non-matching prefUUID; scenario should NEVER happen
				err := errors.New("This source id: " + result[0].SourceUuid + " the only concordance to a non-matching node with prefUuid: " + result[0].PrefUuid)
				log.WithFields(log.Fields{"UUID": prefUUID, "transaction_id": transId, "alert_tag": "ConceptLoadingDodgyData"}).Error(err)
				return deleteLonePrefUuidQueries, err
			}
		} else {
			if result[0].SourceUuid == result[0].PrefUuid {
				if result[0].SourceUuid != prefUUID {
					//TODO ???
					// Source is prefUUID for a different concordance
					err := errors.New("Cannot currently process this record as it will break an existing concordance with prefUuid: " + result[0].SourceUuid)
					log.WithFields(log.Fields{"UUID": prefUUID, "transaction_id": transId, "alert_tag": "ConceptLoadingInvalidConcordance"}).Error(err)
					return deleteLonePrefUuidQueries, err
				} else {
					// Source is prefUUID for a current concordance
					break
				}
			} else {
				// Source was concorded to different concordance. Data on existing concordance is now out of data
				log.WithFields(log.Fields{"UUID": prefUUID, "transaction_id": transId, "alert_tag": "ConceptLoadingStaleData"}).Info("Need to re-ingest concordance record for prefUuid: " + result[0].PrefUuid + " as source: " + result[0].SourceUuid + " has been removed.")
				break
			}
		}
	}
	return deleteLonePrefUuidQueries, nil
}

func deleteLonePrefUuid(prefUUID string) *neoism.CypherQuery {
	log.WithField("UUID", prefUUID).Debug("Deleting orphaned prefUUID node")
	equivQuery := &neoism.CypherQuery{
		Statement: `MATCH (t:Thing {prefUUID:{id}}) DELETE t`,
		Parameters: map[string]interface{}{
			"id": prefUUID,
		},
	}
	return equivQuery
}

func (s Service) clearDownExistingNodes(ac AggregatedConcept) []*neoism.CypherQuery {
	acUUID := ac.PrefUUID
	sourceUuids := getSourceIds(ac.SourceRepresentations)

	queryBatch := []*neoism.CypherQuery{}

	for _, id := range sourceUuids {
		deletePreviousIdentifiersLabelsAndPropertiesQuery := &neoism.CypherQuery{
			Statement: fmt.Sprintf(`MATCH (t:Thing {uuid:{id}})
			OPTIONAL MATCH (t)<-[rel:IDENTIFIES]-(i)
			OPTIONAL MATCH (t)-[eq:EQUIVALENT_TO]->(a:Thing)
			OPTIONAL MATCH (t)-[x:HAS_PARENT]->(p)
			REMOVE t:%s
			SET t={uuid:{id}}
			DELETE x, rel, i, eq`, getLabelsToRemove()),
			Parameters: map[string]interface{}{
				"id": id,
			},
		}
		queryBatch = append(queryBatch, deletePreviousIdentifiersLabelsAndPropertiesQuery)
	}

	//cleanUP all the previous IDENTIFIERS referring to that uuid
	deletePreviousIdentifiersLabelsAndPropertiesQuery := &neoism.CypherQuery{
		Statement: fmt.Sprintf(`MATCH (t:Thing {prefUUID:{acUUID}})
			OPTIONAL MATCH (t)<-[rel:EQUIVALENT_TO]-(s)
			REMOVE t:%s
			SET t={prefUUID:{acUUID}}
			DELETE rel`, getLabelsToRemove()),
		Parameters: map[string]interface{}{
			"acUUID": acUUID,
		},
	}
	queryBatch = append(queryBatch, deletePreviousIdentifiersLabelsAndPropertiesQuery)

	return queryBatch
}

func createNodeQueries(concept Concept, prefUUID string, uuid string) []*neoism.CypherQuery {
	queryBatch := []*neoism.CypherQuery{}
	var createConceptQuery *neoism.CypherQuery

	// Leaf or Lone Node
	if uuid != "" {
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
	} else {
		// Canonical node that doesn't have UUID
		allProps := setProps(concept, prefUUID, false)
		createConceptQuery = &neoism.CypherQuery{
			Statement: fmt.Sprintf(`MERGE (n:Thing {prefUUID: {prefUUID}})
								set n={allprops}
								set n :%s`, getAllLabels(concept.Type)),
			Parameters: map[string]interface{}{
				"prefUUID": prefUUID,
				"allprops": allProps,
			},
		}
	}

	if len(concept.ParentUUIDs) > 0 {
		for _, parentUUID := range concept.ParentUUIDs {
			writeParent := &neoism.CypherQuery{
				Statement: `
                                MERGE (o:Thing {uuid: {uuid}})
		  	   	MERGE (parentupp:Identifier:UPPIdentifier{value:{paUuid}})
                            	MERGE (parentupp)-[:IDENTIFIES]->(p:Thing) ON CREATE SET p.uuid = {paUuid}
		            	MERGE (o)-[:HAS_PARENT]->(p)	`,
				Parameters: neoism.Props{
					"paUuid": parentUUID,
					"uuid":   concept.UUID,
				},
			}
			queryBatch = append(queryBatch, writeParent)
		}

	}

	queryBatch = append(queryBatch, createConceptQuery)

	// If no UUID then it is the canonical node and will not have identifier nodes
	if uuid != "" {
		queryBatch = append(queryBatch, addIdentifierNodes(uuid, concept.Authority, concept.AuthorityValue)...)
	}

	return queryBatch

}

func (s Service) writeConcordedNodeForUnconcordedConcepts(concept Concept) *neoism.CypherQuery {
	allProps := setProps(concept, concept.UUID, false)
	log.WithField("UUID", concept.UUID).Debug("Creating prefUUID node for unconcorded concept")
	createCanonicalNodeQuery := &neoism.CypherQuery{
		Statement: fmt.Sprintf(`MATCH (t:Thing{uuid:{prefUUID}}) MERGE (n:Thing {prefUUID: {prefUUID}})<-[:EQUIVALENT_TO]-(t)
								set n={allprops}
								set n :%s`, getAllLabels(concept.Type)),
		Parameters: map[string]interface{}{
			"prefUUID": concept.UUID,
			"allprops": allProps,
		},
	}
	return createCanonicalNodeQuery
}

func getAllLabels(conceptType string) string {
	labels := conceptType
	parentType := mapper.ParentType(conceptType)
	for parentType != "" {
		labels += ":" + parentType
		parentType = mapper.ParentType(parentType)
	}
	return labels
}

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

func getSourceIds(sourceConcepts []Concept) []string {
	var idList []string
	for _, concept := range sourceConcepts {
		idList = append(idList, concept.UUID)
	}
	return idList
}

func setProps(concept Concept, id string, isSource bool) map[string]interface{} {
	nodeProps := map[string]interface{}{}

	nodeProps["prefLabel"] = concept.PrefLabel
	nodeProps["lastModifiedEpoch"] = time.Now().Unix()

	if len(concept.Aliases) > 0 {
		nodeProps["aliases"] = concept.Aliases
	}

	if concept.DescriptionXML != "" {
		nodeProps["descriptionXML"] = concept.DescriptionXML
	}
	if concept.ImageURL != "" {
		nodeProps["imageUrl"] = concept.ImageURL
	}
	if concept.Strapline != "" {
		nodeProps["strapline"] = concept.Strapline
	}

	if isSource {
		nodeProps["uuid"] = id
		nodeProps["authority"] = concept.Authority
		nodeProps["authorityValue"] = concept.AuthorityValue

	} else {
		nodeProps["prefUUID"] = id
	}

	return nodeProps
}

func addIdentifierNodes(UUID string, authority string, authorityValue string) []*neoism.CypherQuery {
	var queryBatch []*neoism.CypherQuery
	//Add Alternative Identifier
	for k, v := range authorityToIdentifierLabelMap {
		if k == authority {
			alternativeIdentifierQuery := createNewIdentifierQuery(UUID, v, authorityValue)
			queryBatch = append(queryBatch, alternativeIdentifierQuery)
		}
	}

	if authority != "UPP" {
		//TODO Remove this when things no longer matches on UppIdentifier
		// Add UPPIdentififer
		uppIdentifierQuery := createNewIdentifierQuery(UUID,
			authorityToIdentifierLabelMap["UPP"], UUID)

		queryBatch = append(queryBatch, uppIdentifierQuery)
	}
	return queryBatch
}

func createNewIdentifierQuery(uuid string, identifierLabel string, identifierValue string) *neoism.CypherQuery {
	statementTemplate := fmt.Sprintf(`MERGE (t:Thing {uuid:{uuid}})
					CREATE (i:Identifier {value:{value}})
					MERGE (t)<-[:IDENTIFIES]-(i)
					set i : %s `, identifierLabel)
	query := &neoism.CypherQuery{
		Statement: statementTemplate,
		Parameters: map[string]interface{}{
			"uuid":  uuid,
			"value": identifierValue,
		},
	}
	return query
}

//Delete - Delete method
func (s Service) Delete(uuid string, transId string) (bool, error) {
	log.WithFields(log.Fields{"UUID": uuid, "transaction_id": transId}).Info("Delete endpoint is currently non-functional")
	return false, nil
}

//DecodeJSON - decode json
func (s Service) DecodeJSON(dec *json.Decoder) (interface{}, string, error) {
	sub := AggregatedConcept{}
	err := dec.Decode(&sub)
	return sub, sub.PrefUUID, err
}

//Check - checker
func (s Service) Check() error {
	err, _ := s.hc.Check()
	return err
}

//Count - Count of concepts
// TODO: This needs to change of course to be taxonomy specific but will involve
// a breaking change to the base app so vendoring needs to be applied - Do we care about this count?
func (s Service) Count() (int, error) {

	results := []struct {
		Count int `json:"c"`
	}{}

	query := &neoism.CypherQuery{
		Statement: `MATCH (n:Concept) return count(n) as c`,
		Result:    &results,
	}

	err := s.conn.CypherBatch([]*neoism.CypherQuery{query})

	if err != nil {
		return 0, err
	}

	return results[0].Count, nil
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
