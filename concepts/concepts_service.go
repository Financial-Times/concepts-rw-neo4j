package concepts

import (
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"time"

	"github.com/Financial-Times/go-logger"
	"github.com/Financial-Times/neo-model-utils-go/mapper"
	"github.com/Financial-Times/neo-utils-go/neoutils"
	"github.com/bradfitz/slice"
	"github.com/jmcvetta/neoism"
)

const (
	//UpdatedEvent
	UpdatedEvent = "CONCEPT_UPDATED"
	AddedEvent   = "CONCORDDANCE_ADDED"
	RemovedEvent = "CONCORDANCE_REMOVED"
)

// ConceptService - CypherDriver - CypherDriver
type ConceptService struct {
	conn neoutils.NeoConnection
}

// ConceptServicer defines the functions any read-write application needs to implement
type ConceptServicer interface {
	Write(thing interface{}, transID string) (updatedIds interface{}, err error)
	Read(uuid string, transID string) (thing interface{}, found bool, err error)
}

// Initialise - Would this be better as an extension in Neo4j? i.e. that any Thing has this constraint added on creation
func Initialise(conn neoutils.NeoConnection) error {
	err := conn.EnsureIndexes(map[string]string{
		"Identifier": "value",
		"Concept":    "leiCode",
	})
	if err != nil {
		logger.WithError(err).Error("Could not run db index")
		return err
	}

	err = conn.EnsureIndexes(map[string]string{
		"Thing":   "authorityValue",
		"Concept": "authorityValue",
	})
	if err != nil {
		logger.WithError(err).Error("Could not run DB constraints")
		return err
	}

	err = conn.EnsureConstraints(map[string]string{
		"Thing":   "prefUUID",
		"Concept": "prefUUID",
	})
	if err != nil {
		logger.WithError(err).Error("Could not run db constraints")
		return err
	}
	return conn.EnsureConstraints(constraintMap)
}

type NeoAggregatedConcept struct {
	Aliases               []string               `json:"aliases,omitempty"`
	Authority             string                 `json:"authority,omitempty"`
	AuthorityValue        string                 `json:"authorityValue,omitempty"`
	BroaderUUIDs          []string               `json:"broaderUUIDs,omitempty"`
	DescriptionXML        string                 `json:"descriptionXML,omitempty"`
	EmailAddress          string                 `json:"emailAddress,omitempty"`
	FacebookPage          string                 `json:"facebookPage,omitempty"`
	FigiCode              string                 `json:"figiCode,omitempty"`
	ImageURL              string                 `json:"imageUrl,omitempty"`
	InceptionDate         string                 `json:"inceptionDate,omitempty"`
	InceptionDateEpoch    int64                  `json:"inceptionDateEpoch,omitempty"`
	IssuedBy              string                 `json:"issuedBy,omitempty"`
	LastModifiedEpoch     int                    `json:"lastModifiedEpoch,omitempty"`
	MembershipRoles       []MembershipRole       `json:"membershipRoles,omitempty"`
	OrganisationUUID      string                 `json:"organisationUUID,omitempty"`
	ParentUUIDs           []string               `json:"parentUUIDs,omitempty"`
	PersonUUID            string                 `json:"personUUID,omitempty"`
	PrefLabel             string                 `json:"prefLabel"`
	PrefUUID              string                 `json:"prefUUID,omitempty"`
	RelatedUUIDs          []string               `json:"relatedUUIDs,omitempty"`
	SupersededUUIDs       []string               `json:"supersededByUUIDs,omitempty"`
	ScopeNote             string                 `json:"scopeNote,omitempty"`
	ShortLabel            string                 `json:"shortLabel,omitempty"`
	SourceRepresentations []NeoAggregatedConcept `json:"sourceRepresentations"`
	Strapline             string                 `json:"strapline,omitempty"`
	TerminationDate       string                 `json:"terminationDate,omitempty"`
	TerminationDateEpoch  int64                  `json:"terminationDateEpoch,omitempty"`
	TwitterHandle         string                 `json:"twitterHandle,omitempty"`
	Types                 []string               `json:"types"`
	UUID                  string                 `json:"uuid,omitempty"`
	IsDeprecated          bool                   `json:"isDeprecated,omitempty"`
	// Organisations
	ProperName             string   `json:"properName,omitempty"`
	ShortName              string   `json:"shortName,omitempty"`
	LegalName              string   `json:"legalName,omitempty"`
	TradeNames             []string `json:"tradeNames,omitempty"`
	FormerNames            []string `json:"formerNames,omitempty"`
	LocalNames             []string `json:"localNames,omitempty"`
	CountryCode            string   `json:"countryCode,omitempty"`
	CountryOfIncorporation string   `json:"countryOfIncorporation,omitempty"`
	PostalCode             string   `json:"postalCode,omitempty"`
	YearFounded            int      `json:"yearFounded,omitempty"`
	LeiCode                string   `json:"leiCode,omitempty"`
	ParentOrganisation     string   `json:"parentOrganisation,omitempty"`
	// Person
	Salutation string `json:"salutation,omitempty"`
	BirthYear  int    `json:"birthYear,omitempty"`
}

type equivalenceResult struct {
	SourceUUID  string   `json:"sourceUuid"`
	PrefUUID    string   `json:"prefUuid"`
	Types       []string `json:"types"`
	Equivalence int      `json:"count"`
}

func ValidateBasicConcept(aggConcept AggregatedConcept, transID string) error {
	if aggConcept.PrefLabel == "" {
		return requestError{formatError("no prefLabel", aggConcept.PrefUUID, transID)}
	}
	if aggConcept.Type == "" {
		return requestError{formatError("no type", aggConcept.PrefUUID, transID)}
	}
	if _, ok := constraintMap[aggConcept.Type]; !ok {
		return requestError{formatError("invalid type", aggConcept.PrefUUID, transID)}
	}
	if aggConcept.SourceRepresentations == nil {
		return requestError{formatError("no sourceRepresentation", aggConcept.PrefUUID, transID)}
	}
	for _, concept := range aggConcept.SourceRepresentations {
		if concept.Authority == "" {
			return requestError{formatError("no sourceRepresentation.authority", concept.UUID, transID)}
		}
		// Is Authority recognised?
		if _, ok := authorityToIdentifierLabelMap[concept.Authority]; !ok {
			logger.WithTransactionID(transID).WithUUID(aggConcept.PrefUUID).Debugf("Unknown authority, therefore unable to add the relevant Identifier node: %s", concept.Authority)
		}
		if concept.Type == "" {
			return requestError{formatError("no sourceRepresentation type", concept.UUID, transID)}
		}
		if _, ok := constraintMap[concept.Type]; !ok {
			return requestError{formatError("invalid sourceRepresentation type", aggConcept.PrefUUID, transID)}
		}
		if concept.PrefLabel == "" {
			return requestError{formatError("no sourceRepresentation prefLabel", concept.UUID, transID)}
		}
		if concept.AuthorityValue == "" {
			return requestError{formatError("no sourceRepresentation.authorityValue", concept.UUID, transID)}
		}
	}
	return nil
}

func formatError(field string, uuid string, transID string) string {
	err := errors.New("invalid request, " + field + " has been supplied")
	logger.WithError(err).WithTransactionID(transID).WithUUID(uuid).Error("Validation of payload failed")
	return err.Error()
}

func FilterIdsThatAreUniqueToFirstMap(firstMapConcepts map[string]string, secondMapConcepts map[string]string) map[string]string {
	//Loop through both lists to find id which is present in first list but not in the second
	filteredMap := make(map[string]string)

	for conceptID := range firstMapConcepts {
		if _, ok := secondMapConcepts[conceptID]; !ok {
			filteredMap[conceptID] = firstMapConcepts[conceptID]
		}
	}
	return filteredMap
}

//HandleTransferConcordance handles scenarios where a source node is added to an existing concordance
//it will validate the source can be moved from its current state, clean up any leftover nodes and create events of each change
func HandleTransferConcordance(conceptData map[string]string, conn neoutils.NeoConnection, updateRecord *ConceptChanges, aggregateHash string, prefUUID string, transID string) ([]*neoism.CypherQuery, error) {
	var result []equivalenceResult
	var deleteLonePrefUUIDQueries []*neoism.CypherQuery

	for updatedSourceID := range conceptData {
		equivQuery := &neoism.CypherQuery{
			Statement: `
					MATCH (t:Thing {uuid:{id}})
					OPTIONAL MATCH (t)-[:EQUIVALENT_TO]->(c)
					OPTIONAL MATCH (c)<-[eq:EQUIVALENT_TO]-(x:Thing)
					RETURN t.uuid as sourceUuid, labels(t) as types, c.prefUUID as prefUuid, COUNT(DISTINCT eq) as count`,
			Parameters: map[string]interface{}{
				"id": updatedSourceID,
			},
			Result: &result,
		}
		err := conn.CypherBatch([]*neoism.CypherQuery{equivQuery})
		if err != nil {
			logger.WithError(err).WithTransactionID(transID).WithUUID(prefUUID).Error("requests for source nodes canonical information resulted in error")
			return deleteLonePrefUUIDQueries, err
		}

		//source node does not currently exist in neo4j, nothing to tidy up
		if len(result) == 0 {
			logger.WithTransactionID(transID).WithUUID(prefUUID).Info("no existing concordance record found")
			if updatedSourceID != prefUUID {
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
						NewID: prefUUID,
					},
				})
			}
			continue
			//this scenario should never happen
		} else if len(result) > 1 {
			err := fmt.Errorf("multiple source concepts found with matching uuid: %s", updatedSourceID)
			logger.WithTransactionID(transID).WithUUID(prefUUID).Error(err.Error())
			return deleteLonePrefUUIDQueries, err
		}

		conceptType, err := mapper.MostSpecificType(result[0].Types)
		if err != nil {
			logger.WithError(err).WithTransactionID(transID).WithUUID(prefUUID).Errorf("could not return most specific type from source node: %v", result[0].Types)
			return deleteLonePrefUUIDQueries, err
		}

		logger.WithField("Uuid", result[0].SourceUUID).Debug("existing prefUUID is " + result[0].PrefUUID + " equivalence count is " + strconv.Itoa(result[0].Equivalence))
		// Source is old as exists in Neo4j without a prefNode. It can be transferred without issue
		if result[0].Equivalence == 0 {
			continue
		} else if result[0].Equivalence == 1 {
			// Source exists in neo4j but is not concorded. It can be transferred without issue but its prefNode should be deleted
			if result[0].SourceUUID == result[0].PrefUUID {
				logger.WithTransactionID(transID).WithUUID(prefUUID).Debugf("pref uuid node for source %s will need to be deleted as its source will be removed", result[0].SourceUUID)
				deleteLonePrefUUIDQueries = append(deleteLonePrefUUIDQueries, deleteLonePrefUUID(result[0].PrefUUID))
				//concordance added
				updateRecord.ChangedRecords = append(updateRecord.ChangedRecords, Event{
					ConceptType:   conceptType,
					ConceptUUID:   result[0].SourceUUID,
					AggregateHash: aggregateHash,
					TransactionID: transID,
					EventDetails: ConcordanceEvent{
						Type:  AddedEvent,
						OldID: result[0].SourceUUID,
						NewID: prefUUID,
					},
				})
				continue
			} else {
				// Source is only source concorded to non-matching prefUUID; scenario should NEVER happen
				err := fmt.Errorf("this source id: %s the only concordance to a non-matching node with prefUuid: %s", result[0].SourceUUID, result[0].PrefUUID)
				logger.WithTransactionID(transID).WithUUID(prefUUID).WithField("alert_tag", "ConceptLoadingDodgyData").Error(err)
				return deleteLonePrefUUIDQueries, err
			}
		} else {
			if result[0].SourceUUID == result[0].PrefUUID {
				if result[0].SourceUUID != prefUUID {
					// Source is prefNode for a different concordance.
					err := fmt.Errorf("cannot currently process this record as it will break an existing concordance with prefUuid: %s", result[0].SourceUUID)
					logger.WithTransactionID(transID).WithUUID(prefUUID).WithField("alert_tag", "ConceptLoadingInvalidConcordance").Error(err)
					return deleteLonePrefUUIDQueries, err
				}
			} else {
				// Source was concorded to different concordance. Data on existing concordance is now out of date
				logger.WithTransactionID(transID).WithUUID(prefUUID).WithField("alert_tag", "ConceptLoadingStaleData").Infof("need to re-ingest concordance record for prefUuid: %s as source: %s has been removed.", result[0].PrefUUID, result[0].SourceUUID)

				updateRecord.ChangedRecords = append(updateRecord.ChangedRecords, Event{
					ConceptType:   conceptType,
					ConceptUUID:   updatedSourceID,
					AggregateHash: aggregateHash,
					TransactionID: transID,
					EventDetails: ConcordanceEvent{
						Type:  RemovedEvent,
						OldID: result[0].PrefUUID,
						NewID: result[0].SourceUUID,
					},
				})

				updateRecord.ChangedRecords = append(updateRecord.ChangedRecords, Event{
					ConceptType:   conceptType,
					ConceptUUID:   updatedSourceID,
					AggregateHash: aggregateHash,
					TransactionID: transID,
					EventDetails: ConcordanceEvent{
						Type:  AddedEvent,
						OldID: result[0].SourceUUID,
						NewID: prefUUID,
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
	logger.WithField("Uuid", prefUUID).Debug("deleting orphaned prefUUID node")
	equivQuery := &neoism.CypherQuery{
		Statement: `MATCH (t:Thing {prefUUID:{id}}) DETACH DELETE t`,
		Parameters: map[string]interface{}{
			"id": prefUUID,
		},
	}
	return equivQuery
}

//CreateNodeQueries will create the queriesto write the concept to Neo4j
func CreateNodeQueries(concept Concept, prefUUID string, uuid string) []*neoism.CypherQuery {
	var queryBatch []*neoism.CypherQuery
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
		// Canonical node that doesn't have Uuid
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
						MERGE (parentupp:Identifier:UPPIdentifier {value: {parentUUID}})
						MERGE (parent:Thing {uuid: {parentUUID}})
						MERGE (parentupp)-[:IDENTIFIES]->(parent)
						MERGE (o)-[:HAS_PARENT]->(parent)	`,
			Parameters: neoism.Props{
				"parentUUID": parentUUID,
				"uuid":       concept.UUID,
			},
		}
		queryBatch = append(queryBatch, writeParent)
	}

	if concept.OrganisationUUID != "" {
		writeOrganisation := &neoism.CypherQuery{
			Statement: `MERGE (membership:Thing {uuid: {uuid}})
						MERGE (orgupp:Identifier:UPPIdentifier {value: {orgUUID}})
						MERGE (org:Thing {uuid: {orgUUID}})
						MERGE (orgupp)-[:IDENTIFIES]->(org)
						MERGE (membership)-[:HAS_ORGANISATION]->(org)`,
			Parameters: neoism.Props{
				"orgUUID": concept.OrganisationUUID,
				"uuid":    concept.UUID,
			},
		}
		queryBatch = append(queryBatch, writeOrganisation)
	}

	if concept.PersonUUID != "" {
		writePerson := &neoism.CypherQuery{
			Statement: `MERGE (membership:Thing {uuid: {uuid}})
						MERGE (personupp:Identifier:UPPIdentifier {value: {personUUID}})
						MERGE (person:Thing {uuid: {personUUID}})
						MERGE (personupp)-[:IDENTIFIES]->(person)
						MERGE (membership)-[:HAS_MEMBER]->(person)`,
			Parameters: neoism.Props{
				"personUUID": concept.PersonUUID,
				"uuid":       concept.UUID,
			},
		}
		queryBatch = append(queryBatch, writePerson)
	}

	if uuid != "" && concept.IssuedBy != "" {
		writeFinIns := &neoism.CypherQuery{
			Statement: `MERGE (fi:Thing {uuid: {fiUUID}})
						MERGE (org:Thing {uuid: {orgUUID}})
						MERGE (fi)-[:ISSUED_BY]->(org)
						MERGE (fiupp:Identifier:FIGIIdentifier {value: {fiCode}})
						MERGE (fiupp)-[:IDENTIFIES]->(fi)
						`,
			Parameters: neoism.Props{
				"fiUUID":  concept.UUID,
				"fiCode":  concept.FigiCode,
				"orgUUID": concept.IssuedBy,
			},
		}
		queryBatch = append(queryBatch, writeFinIns)
	}

	if uuid != "" && concept.ParentOrganisation != "" {
		writeParentOrganisation := &neoism.CypherQuery{
			Statement: `MERGE (org:Thing {uuid: {uuid}})
							MERGE (orgUPP:Identifier:UPPIdentifier {value: {orgUUID}})
							MERGE (parentOrg:Thing {uuid: {orgUUID}})
							MERGE (orgUPP)-[:IDENTIFIES]->(parentOrg)
							MERGE (org)-[:SUB_ORGANISATION_OF]->(parentOrg)`,
			Parameters: neoism.Props{
				"orgUUID": concept.ParentOrganisation,
				"uuid":    concept.UUID,
			},
		}
		queryBatch = append(queryBatch, writeParentOrganisation)
	}

	if uuid != "" && len(concept.MembershipRoles) > 0 {
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
	}

	queryBatch = append(queryBatch, createConceptQuery)

	// If no Uuid then it is the canonical node and will not have identifier nodes
	if uuid != "" && concept.Type != "Membership" {
		queryBatch = append(queryBatch, addIdentifierNodes(uuid, concept.Authority, concept.AuthorityValue)...)
	}

	return queryBatch

}

//AddRelationship is responsible for creating the queries concerning adding owned relationships source concepts
func AddRelationship(conceptID string, relationshipIDs []string, relationshipType string, queryBatch []*neoism.CypherQuery) []*neoism.CypherQuery {
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

//WriteCanonicalNodeForUnconcordedConcepts will create a canonical node for any concepts that were removed from an existing concordance and thus would become lone
func WriteCanonicalNodeForUnconcordedConcepts(concept Concept) *neoism.CypherQuery {
	allProps := setProps(concept, concept.UUID, false)
	logger.WithField("Uuid", concept.UUID).Debug("Creating prefUUID node for unconcorded concept")
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

//GetLabelsToRemove is used to clear a concept of its type, down to a Thing, before repopulating the node
func GetLabelsToRemove() string {
	var labelsToRemove string
	for i, conceptType := range conceptLabels {
		labelsToRemove += conceptType
		if i+1 < len(conceptLabels) {
			labelsToRemove += ":"
		}
	}
	return labelsToRemove
}

//GetUUIDAndTypeFromSources returns Uuid & type of each source node
func GetUUIDAndTypeFromSources(sourceConcepts []Concept) map[string]string {
	conceptData := make(map[string]string)
	for _, concept := range sourceConcepts {
		conceptData[concept.UUID] = concept.Type
	}
	return conceptData
}

//set properties on concept node
func setProps(concept Concept, id string, isSource bool) map[string]interface{} {
	nodeProps := map[string]interface{}{}

	if concept.PrefLabel != "" {
		nodeProps["prefLabel"] = concept.PrefLabel
	}
	nodeProps["lastModifiedEpoch"] = time.Now().Unix()
	if concept.FigiCode != "" {
		nodeProps["figiCode"] = concept.FigiCode
	}

	if concept.IsDeprecated {
		nodeProps["isDeprecated"] = true
	}

	if isSource {
		nodeProps["uuid"] = id
		nodeProps["authority"] = concept.Authority
		nodeProps["authorityValue"] = concept.AuthorityValue

		return nodeProps
	}

	nodeProps["prefUUID"] = id

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
	if concept.FigiCode != "" {
		nodeProps["figiCode"] = concept.FigiCode
	}
	if concept.ProperName != "" {
		nodeProps["properName"] = concept.ProperName
	}
	if concept.ShortName != "" {
		nodeProps["shortName"] = concept.ShortName
	}
	if len(concept.FormerNames) > 0 {
		nodeProps["formerNames"] = concept.FormerNames
	}
	if len(concept.TradeNames) > 0 {
		nodeProps["tradeNames"] = concept.TradeNames
	}
	if len(concept.LocalNames) > 0 {
		nodeProps["localNames"] = concept.LocalNames
	}
	if concept.CountryCode != "" {
		nodeProps["countryCode"] = concept.CountryCode
	}
	if concept.CountryOfIncorporation != "" {
		nodeProps["countryOfIncorporation"] = concept.CountryOfIncorporation
	}
	if concept.PostalCode != "" {
		nodeProps["postalCode"] = concept.PostalCode
	}
	if concept.YearFounded > 0 {
		nodeProps["yearFounded"] = concept.YearFounded
	}
	if concept.LeiCode != "" {
		nodeProps["leiCode"] = concept.LeiCode
	}
	if concept.InceptionDate != "" {
		nodeProps["inceptionDate"] = concept.InceptionDate
	}
	if concept.TerminationDate != "" {
		nodeProps["terminationDate"] = concept.TerminationDate
	}
	if concept.InceptionDateEpoch > 0 {
		nodeProps["inceptionDateEpoch"] = concept.InceptionDateEpoch
	}
	if concept.TerminationDateEpoch > 0 {
		nodeProps["terminationDateEpoch"] = concept.TerminationDateEpoch
	}

	if concept.Salutation != "" {
		nodeProps["salutation"] = concept.Salutation
	}
	if concept.BirthYear > 0 {
		nodeProps["birthYear"] = concept.BirthYear
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

func FilterSlice(a []string) []string {
	r := []string{}
	for _, str := range a {
		if str != "" {
			r = append(r, str)
		}
	}

	if len(r) == 0 {
		return nil
	}
	sort.Strings(a)
	return a
}

func CleanConcept(c AggregatedConcept) AggregatedConcept {
	for j := range c.SourceRepresentations {
		c.SourceRepresentations[j].LastModifiedEpoch = 0
		for i := range c.SourceRepresentations[j].MembershipRoles {
			c.SourceRepresentations[j].MembershipRoles[i].InceptionDateEpoch = 0
			c.SourceRepresentations[j].MembershipRoles[i].TerminationDateEpoch = 0
		}
		slice.Sort(c.SourceRepresentations[j].MembershipRoles[:], func(k, l int) bool {
			return c.SourceRepresentations[j].MembershipRoles[k].RoleUUID < c.SourceRepresentations[j].MembershipRoles[l].RoleUUID
		})
	}
	for i := range c.MembershipRoles {
		c.MembershipRoles[i].InceptionDateEpoch = 0
		c.MembershipRoles[i].TerminationDateEpoch = 0
	}
	slice.Sort(c.SourceRepresentations[:], func(k, l int) bool {
		return c.SourceRepresentations[k].UUID < c.SourceRepresentations[l].UUID
	})
	return c
}

//CleanSourceProperties removes any extra fields from leaf nodes
func CleanSourceProperties(c AggregatedConcept) AggregatedConcept {
	var cleanSources []Concept
	for _, source := range c.SourceRepresentations {
		cleanConcept := Concept{
			UUID:              source.UUID,
			PrefLabel:         source.PrefLabel,
			Type:              source.Type,
			Authority:         source.Authority,
			AuthorityValue:    source.AuthorityValue,
			RelatedUUIDs:      source.RelatedUUIDs,
			SupersededByUUIDs: source.SupersededByUUIDs,
			BroaderUUIDs:      source.BroaderUUIDs,
			IsDeprecated:      source.IsDeprecated,
			//Brands
			ParentUUIDs: source.ParentUUIDs,
			//Organisations
			ParentOrganisation: source.ParentOrganisation,
			//Memberships
			PersonUUID:       source.PersonUUID,
			OrganisationUUID: source.OrganisationUUID,
			MembershipRoles:  source.MembershipRoles,
			//Financial Instruments
			IssuedBy: source.IssuedBy,
			FigiCode: source.FigiCode,
		}
		cleanSources = append(cleanSources, cleanConcept)
	}
	c.SourceRepresentations = cleanSources
	return c
}

//ChangedRecordsAreEqual compares expected and actual events from test cases
func ChangedRecordsAreEqual(expectedChanges []Event, actualChanges []Event) bool {
	var eventExists bool
	for _, actualEvent := range actualChanges {
		eventExists = false
		for _, expectedEvent := range expectedChanges {
			if reflect.DeepEqual(expectedEvent, actualEvent) {
				eventExists = true
				break
			}
		}
		if eventExists != true {
			return false
		}
	}
	return true
}
