package concepts

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/Financial-Times/go-logger"
	"github.com/Financial-Times/neo-model-utils-go/mapper"
	"github.com/Financial-Times/neo-utils-go/neoutils"
	"github.com/jmcvetta/neoism"
	"github.com/mitchellh/hashstructure"
)

//Service - CypherDriver - CypherDriver
type ConceptService struct {
	conn neoutils.NeoConnection
}

// Service defines the functions any read-write application needs to implement
type ConceptServicer interface {
	Write(thing interface{}, transID string) (updatedIds interface{}, err error)
	Read(uuid string, transID string) (thing interface{}, found bool, err error)
	DecodeJSON(*json.Decoder) (thing interface{}, identity string, err error)
	Check() error
	Initialise() error
}

//NewConceptService instantiate driver
func NewConceptService(cypherRunner neoutils.NeoConnection) ConceptService {
	return ConceptService{cypherRunner}
}

//Initialise - Would this be better as an extension in Neo4j? i.e. that any Thing has this constraint added on creation
func (s ConceptService) Initialise() error {
	err := s.conn.EnsureIndexes(map[string]string{
		"Identifier": "value",
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
		"Thing":   "prefUUID",
		"Concept": "prefUUID",
	})
	if err != nil {
		logger.WithError(err).Error("Could not run db constraints")
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
	EmailAddress          string       `json:"emailAddress,omitempty"`
	FacebookPage          string       `json:"facebookPage,omitempty"`
	TwitterHandle         string       `json:"twitterHandle,omitempty"`
	ScopeNote             string       `json:"scopeNote,omitempty"`
	ShortLabel            string       `json:"shortLabel,omitempty"`
	OrganisationUUID      string       `json:"organisationUUID,omitempty"`
	PersonUUID            string       `json:"personUUID,omitempty"`
	MembershipRoles       []string     `json:"membershipRoles,omitempty"`
	AggregateHash         string       `json:"aggregateHash,omitempty"`
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
	EmailAddress      string   `json:"emailAddress,omitempty"`
	FacebookPage      string   `json:"facebookPage,omitempty"`
	TwitterHandle     string   `json:"twitterHandle,omitempty"`
	ScopeNote         string   `json:"scopeNote,omitempty"`
	ShortLabel        string   `json:"shortLabel,omitempty"`
	RelatedUUIDs      []string `json:"relatedUUIDs,omitempty"`
	BroaderUUIDs      []string `json:"broaderUUIDs,omitempty"`
	OrganisationUUID  string   `json:"organisationUUID,omitempty"`
	PersonUUID        string   `json:"personUUID,omitempty"`
	MembershipRoles   []string `json:"membershipRoles,omitempty"`
}

type equivalenceResult struct {
	SourceUUID  string `json:"sourceUuid"`
	PrefUUID    string `json:"prefUuid"`
	Equivalence int    `json:"count"`
}

//Read - read service
func (s ConceptService) Read(uuid string, transID string) (interface{}, bool, error) {
	results := []neoAggregatedConcept{}

	query := &neoism.CypherQuery{
		Statement: `
				MATCH (canonical:Thing {prefUUID:{uuid}})<-[:EQUIVALENT_TO]-(node:Thing)
				OPTIONAL MATCH (node)-[:HAS_ORGANISATION]->(org:Thing)
				WITH canonical, node, org.uuid as organisationUUID
				OPTIONAL MATCH (node)-[:HAS_MEMBER]->(person:Thing)
				WITH canonical, node, organisationUUID, person.uuid as personUUID
				OPTIONAL MATCH (node)-[:IS_RELATED_TO]->(related:Thing)
				WITH canonical, node, collect(related.uuid) as relUUIDS, organisationUUID, personUUID
				OPTIONAL MATCH (node)-[:HAS_BROADER]->(broader:Thing)
				WITH canonical, node, relUUIDS, organisationUUID, personUUID, collect(broader.uuid) as broaderUUIDs
				OPTIONAL MATCH (node)-[:HAS_ROLE]->(role:Thing)
				WITH canonical, node, relUUIDS, organisationUUID, personUUID, broaderUUIDs, collect(role.uuid) as membershipRoles
				OPTIONAL MATCH (node)-[:HAS_PARENT]->(parent:Thing)
				WITH canonical.prefUUID as prefUUID, canonical.prefLabel as prefLabel, labels(canonical) as types, canonical.aliases as aliases,
				canonical.descriptionXML as descriptionXML, canonical.strapline as strapline, canonical.imageUrl as imageUrl,
				canonical.emailAddress as emailAddress, canonical.facebookPage as facebookPage, canonical.twitterHandle as twitterHandle,
				canonical.scopeNote as scopeNote, canonical.shortLabel as shortLabel, canonical.aggregateHash as aggregateHash, organisationUUID, personUUID, membershipRoles,
				{uuid:node.uuid, prefLabel:node.prefLabel, authority:node.authority, authorityValue: node.authorityValue,
				types: labels(node), lastModifiedEpoch: node.lastModifiedEpoch, emailAddress: node.emailAddress,
				facebookPage: node.facebookPage,twitterHandle: node.twitterHandle, scopeNote: node.scopeNote, shortLabel: node.shortLabel,
				aliases: node.aliases,descriptionXML: node.descriptionXML, imageUrl: node.imageUrl, strapline: node.strapline, parentUUIDs:collect(parent.uuid),
				relatedUUIDs:relUUIDS, broaderUUIDs:broaderUUIDs, organisationUUID: organisationUUID, personUUID: personUUID, membershipRoles: membershipRoles} as sources
				RETURN prefUUID, prefLabel, types, aliases, descriptionXML, strapline, imageUrl, emailAddress,
				facebookPage, twitterHandle, scopeNote, shortLabel, organisationUUID, personUUID, membershipRoles, aggregateHash, collect(sources) as sourceRepresentations `,
		Parameters: map[string]interface{}{
			"uuid": uuid,
		},
		Result: &results,
	}

	err := s.conn.CypherBatch([]*neoism.CypherQuery{query})
	if err != nil {
		logger.WithError(err).WithTransactionID(transID).WithUUID(uuid).Error("Error executing neo4j read query")
		return AggregatedConcept{}, false, err
	}

	if len(results) == 0 {
		logger.WithTransactionID(transID).WithUUID(uuid).Info("Concept not found in db")
		return AggregatedConcept{}, false, nil
	}
	typeName, err := mapper.MostSpecificType(results[0].Types)
	if err != nil {
		logger.WithError(err).WithTransactionID(transID).WithUUID(uuid).Error("Returned concept had no recognized type")
		return AggregatedConcept{}, false, err
	}

	var sourceConcepts []Concept
	aggregatedConcept := AggregatedConcept{
		PrefUUID:         results[0].PrefUUID,
		PrefLabel:        results[0].PrefLabel,
		Type:             typeName,
		ImageURL:         results[0].ImageURL,
		DescriptionXML:   results[0].DescriptionXML,
		Strapline:        results[0].Strapline,
		Aliases:          results[0].Aliases,
		EmailAddress:     results[0].EmailAddress,
		FacebookPage:     results[0].FacebookPage,
		TwitterHandle:    results[0].TwitterHandle,
		ScopeNote:        results[0].ScopeNote,
		ShortLabel:       results[0].ShortLabel,
		PersonUUID:       results[0].PersonUUID,
		OrganisationUUID: results[0].OrganisationUUID,
		AggregatedHash:   results[0].AggregateHash,
	}

	if len(results[0].MembershipRoles) > 0 {
		var uuids = []string{}
		//TODO do this differently but I get a "" back from the cypher!
		for _, uuid := range results[0].MembershipRoles {
			if uuid != "" {
				uuids = append(uuids, uuid)
			}
		}
		if len(uuids) > 0 {
			aggregatedConcept.MembershipRoles = uuids
		}
	}

	for _, srcConcept := range results[0].SourceRepresentations {
		var concept Concept
		conceptType, err := mapper.MostSpecificType(srcConcept.Types)
		if err != nil {
			logger.WithError(err).WithTransactionID(transID).WithUUID(uuid).Error("Returned source concept had no recognized type")
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

		if len(srcConcept.RelatedUUIDs) > 0 {
			uuids = []string{}
			//TODO do this differently but I get a "" back from the cypher!
			for _, uuid := range srcConcept.RelatedUUIDs {
				if uuid != "" {
					uuids = append(uuids, uuid)
				}
			}
			if len(uuids) > 0 {
				concept.RelatedUUIDs = uuids
			}
		}

		if len(srcConcept.BroaderUUIDs) > 0 {
			uuids = []string{}
			//TODO do this differently but I get a "" back from the cypher!
			for _, uuid := range srcConcept.BroaderUUIDs {
				if uuid != "" {
					uuids = append(uuids, uuid)
				}
			}
			if len(uuids) > 0 {
				concept.BroaderUUIDs = uuids
			}
		}

		if len(srcConcept.MembershipRoles) > 0 {
			uuids = []string{}
			//TODO do this differently but I get a "" back from the cypher!
			for _, uuid := range srcConcept.MembershipRoles {
				if uuid != "" {
					uuids = append(uuids, uuid)
				}
			}
			if len(uuids) > 0 {
				concept.MembershipRoles = uuids
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
		concept.FacebookPage = srcConcept.FacebookPage
		concept.EmailAddress = srcConcept.EmailAddress
		concept.TwitterHandle = srcConcept.TwitterHandle
		concept.ShortLabel = srcConcept.ShortLabel
		concept.ScopeNote = srcConcept.ScopeNote
		concept.PersonUUID = srcConcept.PersonUUID
		concept.OrganisationUUID = srcConcept.OrganisationUUID
		sourceConcepts = append(sourceConcepts, concept)
	}

	aggregatedConcept.SourceRepresentations = sourceConcepts

	logger.WithTransactionID(transID).WithUUID(uuid).Debugf("Returned concept is %v", aggregatedConcept)

	return aggregatedConcept, true, nil
}

func (s ConceptService) Write(thing interface{}, transID string) (interface{}, error) {
	// Read the aggregated concept - We need read the entire model first. This is because if we unconcord a TME concept
	// then we need to add prefUUID to the lone node if it has been removed from the concordance listed against a Smartlogic concept
	uuidsToUpdate := UpdatedConcepts{}
	var updatedUUIDList []string
	aggregatedConceptToWrite := thing.(AggregatedConcept)

	requestHash, err := hashstructure.Hash(thing, nil)
	if err != nil {
		logger.WithError(err).WithTransactionID(transID).WithUUID(aggregatedConceptToWrite.PrefUUID).Error("Error hashing json from request")
		return uuidsToUpdate, err
	}

	if err = validateObject(aggregatedConceptToWrite, transID); err != nil {
		return uuidsToUpdate, err
	}

	existingConcept, exists, err := s.Read(aggregatedConceptToWrite.PrefUUID, transID)
	if err != nil {
		logger.WithError(err).WithTransactionID(transID).WithUUID(aggregatedConceptToWrite.PrefUUID).Error("Read request for existing concordance resulted in error")
		return uuidsToUpdate, err
	}

	var queryBatch []*neoism.CypherQuery
	var prefUUIDsToBeDeletedQueryBatch []*neoism.CypherQuery
	if exists {
		existingAggregateConcept := existingConcept.(AggregatedConcept)
		if existingAggregateConcept.AggregatedHash == "" {
			existingAggregateConcept.AggregatedHash = "0"
		}
		currentHash, err := strconv.ParseUint(existingAggregateConcept.AggregatedHash, 10, 64)
		if err != nil {
			logger.WithError(err).WithTransactionID(transID).WithUUID(aggregatedConceptToWrite.PrefUUID).Info("Error whilst parsing existing concept hash")
			return uuidsToUpdate, nil
		}
		logger.WithTransactionID(transID).WithUUID(aggregatedConceptToWrite.PrefUUID).Debugf("Currently stored concept has hash of %d", currentHash)
		logger.WithTransactionID(transID).WithUUID(aggregatedConceptToWrite.PrefUUID).Debugf("Aggregated concept has hash of %d", requestHash)
		if currentHash == requestHash {
			logger.WithTransactionID(transID).WithUUID(aggregatedConceptToWrite.PrefUUID).Info("This concept has not changed since most recent update")
			return uuidsToUpdate, nil
		} else {
			logger.WithTransactionID(transID).WithUUID(aggregatedConceptToWrite.PrefUUID).Info("This concept is different to record stored in db, updating...")
		}

		requestSourceUuids := getSourceIds(aggregatedConceptToWrite.SourceRepresentations)
		existingSourceUuids := getSourceIds(existingAggregateConcept.SourceRepresentations)

		//Concept has been updated since last write, so need to send notification of all affected ids
		for _, source := range aggregatedConceptToWrite.SourceRepresentations {
			updatedUUIDList = append(updatedUUIDList, source.UUID)
		}

		//This filter will leave us with ids that were members of existing concordance but are NOT members of current concordance
		//They will need a new prefUUID node written
		listToUnconcord := filterIdsThatAreUniqueToFirstList(existingSourceUuids, requestSourceUuids)

		//This filter will leave us with ids that are members of current concordance payload but were not previously concorded to this concordance
		listToTransferConcordance := filterIdsThatAreUniqueToFirstList(requestSourceUuids, existingSourceUuids)

		//Handle scenarios for transferring source id from an existing concordance to this concordance
		if len(listToTransferConcordance) > 0 {
			prefUUIDsToBeDeletedQueryBatch, err = s.handleTransferConcordance(listToTransferConcordance, aggregatedConceptToWrite.PrefUUID, transID)
			if err != nil {
				return uuidsToUpdate, err
			}
		}

		clearDownQuery := s.clearDownExistingNodes(aggregatedConceptToWrite)
		for _, query := range clearDownQuery {
			queryBatch = append(queryBatch, query)
		}

		for _, idToUnconcord := range listToUnconcord {
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
				}
			}
		}
	} else {
		prefUUIDsToBeDeletedQueryBatch, err = s.handleTransferConcordance(getSourceIds(aggregatedConceptToWrite.SourceRepresentations), aggregatedConceptToWrite.PrefUUID, transID)
		if err != nil {
			return uuidsToUpdate, err
		}
		//Concept is new, send notification of all source ids
		for _, source := range aggregatedConceptToWrite.SourceRepresentations {
			updatedUUIDList = append(updatedUUIDList, source.UUID)
		}
	}

	hashAsString := strconv.FormatUint(requestHash, 10)
	aggregatedConceptToWrite.AggregatedHash = hashAsString
	queryBatch = populateConceptQueries(queryBatch, aggregatedConceptToWrite)
	for _, query := range prefUUIDsToBeDeletedQueryBatch {
		queryBatch = append(queryBatch, query)
	}

	uuidsToUpdate.UpdatedIds = updatedUUIDList

	logger.WithTransactionID(transID).WithUUID(aggregatedConceptToWrite.PrefUUID).Debug("Executing " + strconv.Itoa(len(queryBatch)) + " queries")
	for _, query := range queryBatch {
		logger.WithTransactionID(transID).WithUUID(aggregatedConceptToWrite.PrefUUID).Debug(fmt.Sprintf("Query: %v", query))
	}

	if err = s.conn.CypherBatch(queryBatch); err != nil {
		logger.WithError(err).WithTransactionID(transID).WithUUID(aggregatedConceptToWrite.PrefUUID).Error("Error executing neo4j write queries. Concept NOT written.")
		return uuidsToUpdate, err
	}
	logger.WithTransactionID(transID).WithUUID(aggregatedConceptToWrite.PrefUUID).Info("Concept written to db")
	return uuidsToUpdate, nil
}

func validateObject(aggConcept AggregatedConcept, transID string) error {
	if aggConcept.PrefLabel == "" {
		return requestError{formatError("prefLabel", aggConcept.PrefUUID, transID)}
	}
	if _, ok := constraintMap[aggConcept.Type]; !ok {
		return requestError{formatError("type", aggConcept.PrefUUID, transID)}
	}
	if aggConcept.SourceRepresentations == nil {
		return requestError{formatError("sourceRepresentation", aggConcept.PrefUUID, transID)}
	}
	for _, concept := range aggConcept.SourceRepresentations {
		// Is Authority recognised?
		if _, ok := authorityToIdentifierLabelMap[concept.Authority]; !ok {
			logger.WithTransactionID(transID).WithUUID(aggConcept.PrefUUID).Debugf("Unknown authority, therefore unable to add the relevant Identifier node: %s", concept.Authority)
		}
		if concept.PrefLabel == "" {
			return requestError{formatError("sourceRepresentation.prefLabel", concept.UUID, transID)}
		}
		if concept.Type == "" {
			return requestError{formatError("sourceRepresentation.type", concept.UUID, transID)}
		}
		if concept.AuthorityValue == "" {
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

//filter out ids that are unique to the first list
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

//Handle new source nodes that have been added to current concordance
func (s ConceptService) handleTransferConcordance(updatedSourceIds []string, prefUUID string, transID string) ([]*neoism.CypherQuery, error) {
	result := []equivalenceResult{}
	deleteLonePrefUuidQueries := []*neoism.CypherQuery{}

	for _, updatedSourceId := range updatedSourceIds {
		equivQuery := &neoism.CypherQuery{
			Statement: `
					MATCH (t:Thing {uuid:{id}})
					OPTIONAL MATCH (t)-[:EQUIVALENT_TO]->(c)
					OPTIONAL MATCH (c)<-[eq:EQUIVALENT_TO]-(x:Thing)
					RETURN t.uuid as sourceUuid, c.prefUUID as prefUuid, COUNT(DISTINCT eq) as count`,
			Parameters: map[string]interface{}{
				"id": updatedSourceId,
			},
			Result: &result,
		}
		err := s.conn.CypherBatch([]*neoism.CypherQuery{equivQuery})
		if err != nil {
			logger.WithError(err).WithTransactionID(transID).WithUUID(prefUUID).Error("Requests for source nodes canonical information resulted in error")
			return deleteLonePrefUuidQueries, err
		}

		if len(result) == 0 {
			logger.WithTransactionID(transID).WithUUID(prefUUID).Info("No existing concordance record found")
			continue
		} else if len(result) > 1 {
			err = fmt.Errorf("Multiple source concepts found with matching uuid: %s", updatedSourceId)
			logger.WithTransactionID(transID).WithUUID(prefUUID).Error(err.Error())
			return deleteLonePrefUuidQueries, err
		}

		logger.WithField("UUID", result[0].SourceUUID).Debug("Existing prefUUID is " + result[0].PrefUUID + " equivalence count is " + strconv.Itoa(result[0].Equivalence))
		// Source has no existing concordance and will be handled by clearDownExistingNodes function
		if result[0].Equivalence == 0 {
			break
		} else if result[0].Equivalence == 1 {
			// Source has existing concordance to itself, after transfer old pref uuid node will need to be cleaned up
			if result[0].SourceUUID == result[0].PrefUUID {
				logger.WithTransactionID(transID).WithUUID(prefUUID).Debugf("Pref uuid node for source %s will need to be deleted as its source will be removed", result[0].SourceUUID)
				deleteLonePrefUuidQueries = append(deleteLonePrefUuidQueries, deleteLonePrefUuid(result[0].PrefUUID))
				break
			} else {
				// Source is only source concorded to non-matching prefUUID; scenario should NEVER happen
				err := fmt.Errorf("This source id: %s the only concordance to a non-matching node with prefUuid: %s", result[0].SourceUUID, result[0].PrefUUID)
				logger.WithTransactionID(transID).WithUUID(prefUUID).WithField("alert_tag", "ConceptLoadingDodgyData").Error(err)
				return deleteLonePrefUuidQueries, err
			}
		} else {
			if result[0].SourceUUID == result[0].PrefUUID {
				if result[0].SourceUUID != prefUUID {
					// Source is prefUUID for a different concordance
					err := fmt.Errorf("Cannot currently process this record as it will break an existing concordance with prefUuid: %s", result[0].SourceUUID)
					logger.WithTransactionID(transID).WithUUID(prefUUID).WithField("alert_tag", "ConceptLoadingInvalidConcordance").Error(err)
					return deleteLonePrefUuidQueries, err
				}
			} else {
				// Source was concorded to different concordance. Data on existing concordance is now out of data
				logger.WithTransactionID(transID).WithUUID(prefUUID).WithField("alert_tag", "ConceptLoadingStaleData").Infof("Need to re-ingest concordance record for prefUuid: % as source: %s has been removed.", result[0].PrefUUID, result[0].SourceUUID)
				break
			}
		}
	}
	return deleteLonePrefUuidQueries, nil
}

//Clean up canonical nodes of a concept that has become a source of current concept
func deleteLonePrefUuid(prefUUID string) *neoism.CypherQuery {
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
func (s ConceptService) clearDownExistingNodes(ac AggregatedConcept) []*neoism.CypherQuery {
	acUUID := ac.PrefUUID
	sourceUuids := getSourceIds(ac.SourceRepresentations)

	queryBatch := []*neoism.CypherQuery{}

	for _, id := range sourceUuids {
		deletePreviousIdentifiersLabelsAndPropertiesQuery := &neoism.CypherQuery{
			Statement: fmt.Sprintf(`MATCH (t:Thing {uuid:{id}})
			OPTIONAL MATCH (t)<-[rel:IDENTIFIES]-(i)
			OPTIONAL MATCH (t)-[eq:EQUIVALENT_TO]->(a:Thing)
			OPTIONAL MATCH (t)-[x:HAS_PARENT]->(p)
			OPTIONAL MATCH (t)-[relatedTo:IS_RELATED_TO]->(relNode)
			OPTIONAL MATCH (t)-[broader:HAS_BROADER]->(brNode)
			OPTIONAL MATCH (t)-[ho:HAS_ORGANISATION]->(org)
			OPTIONAL MATCH (t)-[hm:HAS_MEMBER]->(memb)
			OPTIONAL MATCH (t)-[hr:HAS_ROLE]->(mr)
			REMOVE t:%s
			SET t={uuid:{id}}
			DELETE x, rel, i, eq, relatedTo, broader, ho, hm, hr`, getLabelsToRemove()),
			Parameters: map[string]interface{}{
				"id": id,
			},
		}
		queryBatch = append(queryBatch, deletePreviousIdentifiersLabelsAndPropertiesQuery)
	}

	//cleanUP all the previous Equivalent to relationships
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

//Curate all queries to populate concept nodes
func populateConceptQueries(queryBatch []*neoism.CypherQuery, aggregatedConcept AggregatedConcept) []*neoism.CypherQuery {
	// Create a sourceConcept from the canonical information - WITH NO UUID
	concept := Concept{
		PrefLabel:      aggregatedConcept.PrefLabel,
		Aliases:        aggregatedConcept.Aliases,
		Strapline:      aggregatedConcept.Strapline,
		DescriptionXML: aggregatedConcept.DescriptionXML,
		ImageURL:       aggregatedConcept.ImageURL,
		Type:           aggregatedConcept.Type,
		EmailAddress:   aggregatedConcept.EmailAddress,
		FacebookPage:   aggregatedConcept.FacebookPage,
		TwitterHandle:  aggregatedConcept.TwitterHandle,
		ScopeNote:      aggregatedConcept.ScopeNote,
		ShortLabel:     aggregatedConcept.ShortLabel,
		Hash:           aggregatedConcept.AggregatedHash,
	}

	queryBatch = append(queryBatch, createNodeQueries(concept, aggregatedConcept.PrefUUID, "")...)

	// Repopulate
	for _, sourceConcept := range aggregatedConcept.SourceRepresentations {
		queryBatch = append(queryBatch, createNodeQueries(sourceConcept, "", sourceConcept.UUID)...)

		equivQuery := &neoism.CypherQuery{
			Statement: `MATCH (t:Thing {uuid:{uuid}}), (c:Thing {prefUUID:{prefUUID}})
						MERGE (t)-[:EQUIVALENT_TO]->(c)`,
			Parameters: map[string]interface{}{
				"uuid":     sourceConcept.UUID,
				"prefUUID": aggregatedConcept.PrefUUID,
			},
		}
		queryBatch = append(queryBatch, equivQuery)

		if len(sourceConcept.RelatedUUIDs) > 0 {
			queryBatch = addRelationship(sourceConcept.UUID, sourceConcept.RelatedUUIDs, "IS_RELATED_TO", queryBatch)
		}

		if len(sourceConcept.BroaderUUIDs) > 0 {
			queryBatch = addRelationship(sourceConcept.UUID, sourceConcept.BroaderUUIDs, "HAS_BROADER", queryBatch)
		}
	}
	return queryBatch
}

//Create concept nodes
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

	for _, parentUUID := range concept.ParentUUIDs {
		writeParent := &neoism.CypherQuery{
			Statement: `MERGE (o:Thing {uuid: {uuid}})
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

	if concept.OrganisationUUID != "" {
		writeOrganisation := &neoism.CypherQuery{
			Statement: `MERGE (membership:Thing {uuid: {uuid}})
		  	   				MERGE (orgupp:Identifier:UPPIdentifier{value:{orgUuid}})
                            MERGE (orgupp)-[:IDENTIFIES]->(org:Thing) ON CREATE SET org.uuid = {orgUuid}
		            		MERGE (membership)-[:HAS_ORGANISATION]->(org)`,
			Parameters: neoism.Props{
				"orgUuid": concept.OrganisationUUID,
				"uuid":    concept.UUID,
			},
		}
		queryBatch = append(queryBatch, writeOrganisation)
	}

	if concept.PersonUUID != "" {
		writePerson := &neoism.CypherQuery{
			Statement: `MERGE (membership:Thing {uuid: {uuid}})
		  	   				MERGE (personupp:Identifier:UPPIdentifier{value:{personUuid}})
                            MERGE (personupp)-[:IDENTIFIES]->(person:Thing) ON CREATE SET person.uuid = {personUuid}
		            		MERGE (membership)-[:HAS_MEMBER]->(person)`,
			Parameters: neoism.Props{
				"personUuid": concept.PersonUUID,
				"uuid":       concept.UUID,
			},
		}
		queryBatch = append(queryBatch, writePerson)
	}

	if len(concept.MembershipRoles) > 0 {
		for _, membershipRoleUUID := range concept.MembershipRoles {
			writeParent := &neoism.CypherQuery{
				Statement: `MERGE (membership:Thing {uuid: {uuid}})
		  	   				MERGE (roleupp:Identifier:UPPIdentifier{value:{mmbUuid}})
                            MERGE (roleupp)-[:IDENTIFIES]->(role:Thing) ON CREATE SET role.uuid = {mmbUuid}
		            		MERGE (membership)-[:HAS_ROLE]->(role)	`,
				Parameters: neoism.Props{
					"mmbUuid": membershipRoleUUID,
					"uuid":    concept.UUID,
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

//Add relationships to concepts
func addRelationship(conceptID string, relationshipIDs []string, relationshipType string, queryBatch []*neoism.CypherQuery) []*neoism.CypherQuery {
	for _, id := range relationshipIDs {
		addRelationshipQuery := &neoism.CypherQuery{
			Statement: fmt.Sprintf(`
						MATCH (o:Concept {uuid: {uuid}})
						MERGE (p:Thing {uuid: {id}})
		            	MERGE (o)-[:%s]->(p)
						MERGE (x:Identifier:UPPIdentifier{value:{id}})
                        MERGE (x)-[:IDENTIFIES]->(p)`, relationshipType),
			Parameters: map[string]interface{}{
				"uuid":         conceptID,
				"id":           id,
				"relationship": relationshipType,
			},
		}
		queryBatch = append(queryBatch, addRelationshipQuery)
	}
	return queryBatch
}

//Create canonical node for any concepts that were removed from a concordance and thus would become lone
func (s ConceptService) writeCanonicalNodeForUnconcordedConcepts(concept Concept) *neoism.CypherQuery {
	allProps := setProps(concept, concept.UUID, false)
	logger.WithField("UUID", concept.UUID).Debug("Creating prefUUID node for unconcorded concept")
	createCanonicalNodeQuery := &neoism.CypherQuery{
		Statement: fmt.Sprintf(`	MATCH (t:Thing{uuid:{prefUUID}})
										MERGE (n:Thing {prefUUID: {prefUUID}})<-[:EQUIVALENT_TO]-(t)
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
	for i, conceptType := range conceptLabels {
		labelsToRemove += conceptType
		if i+1 < len(conceptLabels) {
			labelsToRemove += ":"
		}
	}
	return labelsToRemove
}

//extract uuids of the source concepts
func getSourceIds(sourceConcepts []Concept) []string {
	var idList []string
	for _, concept := range sourceConcepts {
		idList = append(idList, concept.UUID)
	}
	return idList
}

//set properties on concept node
func setProps(concept Concept, id string, isSource bool) map[string]interface{} {
	nodeProps := map[string]interface{}{}

	nodeProps["prefLabel"] = concept.PrefLabel
	nodeProps["lastModifiedEpoch"] = time.Now().Unix()

	if len(concept.Aliases) > 0 {
		nodeProps["aliases"] = concept.Aliases
	}
	if concept.EmailAddress != "" {
		nodeProps["emailAddress"] = concept.EmailAddress
	}
	if concept.FacebookPage != "" {
		nodeProps["facebookPage"] = concept.FacebookPage
	}
	if concept.TwitterHandle != "" {
		nodeProps["twitterHandle"] = concept.TwitterHandle
	}
	if concept.ScopeNote != "" {
		nodeProps["scopeNote"] = concept.ScopeNote
	}
	if concept.ShortLabel != "" {
		nodeProps["shortLabel"] = concept.ShortLabel
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
		nodeProps["aggregateHash"] = concept.Hash
	}

	return nodeProps
}

//Add identifiers to node
func addIdentifierNodes(UUID string, authority string, authorityValue string) []*neoism.CypherQuery {
	var queryBatch []*neoism.CypherQuery
	//Add Alternative Identifier

	if label, ok := authorityToIdentifierLabelMap[authority]; ok {
		alternativeIdentifierQuery := createNewIdentifierQuery(UUID, label, authorityValue)
		queryBatch = append(queryBatch, alternativeIdentifierQuery)

		uppIdentifierQuery := createNewIdentifierQuery(UUID, authorityToIdentifierLabelMap["UPP"], UUID)
		queryBatch = append(queryBatch, uppIdentifierQuery)
	}

	return queryBatch
}

//Create identifier
func createNewIdentifierQuery(uuid string, identifierLabel string, identifierValue string) *neoism.CypherQuery {
	statementTemplate := fmt.Sprintf(`MERGE (t:Thing {uuid:{uuid}})
					MERGE (i:Identifier:%s {value:{value}})
					MERGE (t)<-[:IDENTIFIES]-(i)`, identifierLabel)
	query := &neoism.CypherQuery{
		Statement: statementTemplate,
		Parameters: map[string]interface{}{
			"uuid":  uuid,
			"value": identifierValue,
		},
	}
	return query
}

//DecodeJSON - decode json
func (s ConceptService) DecodeJSON(dec *json.Decoder) (interface{}, string, error) {
	sub := AggregatedConcept{}
	err := dec.Decode(&sub)
	return sub, sub.PrefUUID, err
}

//Check - checker
func (s ConceptService) Check() error {
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
