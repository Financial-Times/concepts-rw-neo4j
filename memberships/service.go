package memberships

import (
	"errors"
	"fmt"
	"github.com/Financial-Times/concepts-rw-neo4j/concepts"
	"github.com/Financial-Times/go-logger"
	"github.com/Financial-Times/neo-model-utils-go/mapper"
	"github.com/Financial-Times/neo-utils-go/neoutils"
	"github.com/jmcvetta/neoism"
	"github.com/mitchellh/hashstructure"
	"strconv"
)

type MembershipService struct {
	conn neoutils.NeoConnection
}

func NewMembershipService(db neoutils.NeoConnection) *MembershipService {
	return &MembershipService{db}
}

func (ms *MembershipService) Write(thing interface{}, transID string) (interface{}, error) {
	// Read the aggregated concept - We need read the entire model first. This is because if we unconcord a TME concept
	// then we need to add prefUUID to the lone node if it has been removed from the concordance listed against a Smartlogic concept
	var updateRecord concepts.ConceptChanges
	var updatedUUIDList []string
	var queryBatch []*neoism.CypherQuery

	aggregatedConceptToWrite := thing.(concepts.AggregatedConcept)
	aggregatedConceptToWrite = concepts.CleanSourceProperties(aggregatedConceptToWrite)
	payloadHash, err := hashstructure.Hash(aggregatedConceptToWrite, nil)
	if err != nil {
		logger.WithError(err).WithTransactionID(transID).WithUUID(aggregatedConceptToWrite.PrefUUID).Error("read request for existing concordance resulted in error")
		return updateRecord, err
	}

	hashAsString := strconv.FormatUint(payloadHash, 10)

	if err = concepts.ValidateBasicConcept(aggregatedConceptToWrite, transID); err != nil {
		return updateRecord, err
	}
	if err = validateMembership(aggregatedConceptToWrite, transID); err != nil {
		return updateRecord, err
	}

	//Concept has been updated since last write, so need to send notification of all affected ids
	for _, source := range aggregatedConceptToWrite.SourceRepresentations {
		updatedUUIDList = append(updatedUUIDList, source.UUID)
	}

	clearDownQuery := clearDownExistingNodes(aggregatedConceptToWrite)
	for _, query := range clearDownQuery {
		queryBatch = append(queryBatch, query)
	}

	queryBatch = populateConceptQueries(queryBatch, aggregatedConceptToWrite)

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

	if err = ms.conn.CypherBatch(queryBatch); err != nil {
		logger.WithError(err).WithTransactionID(transID).WithUUID(aggregatedConceptToWrite.PrefUUID).Error("Error executing neo4j write queries. Concept NOT written.")
		return updateRecord, err
	}

	logger.WithTransactionID(transID).WithUUID(aggregatedConceptToWrite.PrefUUID).Info("Concept written to db")
	return updateRecord, nil
}

func clearDownExistingNodes(ac concepts.AggregatedConcept) []*neoism.CypherQuery {
	acUUID := ac.PrefUUID

	var queryBatch []*neoism.CypherQuery

	for _, sr := range ac.SourceRepresentations {
		deletePreviousSourceIdentifiersLabelsAndPropertiesQuery := &neoism.CypherQuery{
			Statement: fmt.Sprintf(`MATCH (t:Thing {uuid:{id}})
			OPTIONAL MATCH (t)-[hr:HAS_ROLE]->()
			OPTIONAL MATCH (t)-[hm:HAS_MEMBER]->()
			OPTIONAL MATCH (t)-[ho:HAS_ORGANISATION]->()
			OPTIONAL MATCH (t)-[sup:SUPERSEDED_BY]->()
			OPTIONAL MATCH (t)-[eq:EQUIVALENT_TO]->()
			REMOVE t:%s
			SET t={uuid:{id}}
			DELETE eq, hr, hm, ho, sup`, concepts.GetLabelsToRemove()),
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
			"acUUID": acUUID,
		},
	}
	queryBatch = append(queryBatch, deletePreviousCanonicalIdentifiersLabelsAndPropertiesQuery)

	return queryBatch
}

func populateConceptQueries(queryBatch []*neoism.CypherQuery, aggregatedConcept concepts.AggregatedConcept) []*neoism.CypherQuery {
	// Create a sourceConcept from the canonical information - WITH NO UUID
	concept := concepts.Concept{
		PrefLabel:            aggregatedConcept.PrefLabel,
		Type:                 aggregatedConcept.Type,
		Aliases:              aggregatedConcept.Aliases,
		InceptionDate:        aggregatedConcept.InceptionDate,
		InceptionDateEpoch:   aggregatedConcept.InceptionDateEpoch,
		TerminationDate:      aggregatedConcept.TerminationDate,
		TerminationDateEpoch: aggregatedConcept.TerminationDateEpoch,
		ScopeNote:            aggregatedConcept.ScopeNote,
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

		if len(sourceConcept.SupersededByUUIDs) > 0 {
			queryBatch = concepts.AddRelationship(sourceConcept.UUID, sourceConcept.SupersededByUUIDs, "SUPERSEDED_BY", queryBatch)
		}
	}
	return queryBatch
}

func validateMembership(aggregatedConceptToWrite concepts.AggregatedConcept, transID string) error {
	for _, concept := range aggregatedConceptToWrite.SourceRepresentations {
		// Is Authority recognised?
		if concept.PersonUUID == "" || aggregatedConceptToWrite.PersonUUID == "" {
			err := errors.New("invalid request, no PersonUUID has been supplied")
			logger.WithError(err).WithTransactionID(transID).WithUUID(aggregatedConceptToWrite.PrefUUID).Error(err)
			return err
		}
		if concept.OrganisationUUID == "" || aggregatedConceptToWrite.OrganisationUUID == "" {
			err := errors.New("invalid request, no OrganisationUUID has been supplied")
			logger.WithError(err).WithTransactionID(transID).WithUUID(aggregatedConceptToWrite.PrefUUID).Error(err)
			return err
		}
		if len(concept.MembershipRoles) < 1 || len(aggregatedConceptToWrite.MembershipRoles) < 1 {
			err := errors.New("invalid request, no MembershipRoles have been supplied")
			logger.WithError(err).WithTransactionID(transID).WithUUID(aggregatedConceptToWrite.PrefUUID).Error(err)
			return err
		}
	}
	return nil
}

func (ms *MembershipService) Read(uuid string, transID string) (interface{}, bool, error) {
	var results []concepts.NeoAggregatedConcept

	query := &neoism.CypherQuery{
		Statement: `MATCH (canonical:Thing {prefUUID:{uuid}})<-[:EQUIVALENT_TO]-(source:Thing)
            OPTIONAL MATCH (source)-[:HAS_MEMBER]->(person:Thing)
            OPTIONAL MATCH (source)-[:HAS_ORGANISATION]->(org:Thing)
            OPTIONAL MATCH (source)-[roleRel:HAS_ROLE]->(role:Thing)
			OPTIONAL MATCH (source)-[:SUPERSEDED_BY]->(supersededBy:Thing)
            WITH
                canonical,
                org,
                person,
                role,
                roleRel,
				supersededBy,
                source
                ORDER BY
                    source.uuid,     
                    role.uuid
            WITH
                canonical,
                org,
                person,
                {
                    authority: source.authority,
                    authorityValue: source.authorityValue,
                    lastModifiedEpoch: source.lastModifiedEpoch,
                    membershipRoles: collect({
                        membershipRoleUUID: role.uuid,
                        inceptionDate: roleRel.inceptionDate,
                        terminationDate: roleRel.terminationDate,
                        inceptionDateEpoch: roleRel.inceptionDateEpoch,
                        terminationDateEpoch: roleRel.terminationDateEpoch
                    }),
                    organisationUUID: org.uuid,
                    personUUID: person.uuid,
                    prefLabel: source.prefLabel,
                    types: labels(source),
                    uuid: source.uuid,
					supersededByUUIDs: collect(DISTINCT supersededBy.uuid),
                    isDeprecated: source.isDeprecated
                } as sources,
                collect({
                    inceptionDate: roleRel.inceptionDate,
                    inceptionDateEpoch: roleRel.inceptionDateEpoch,
                    membershipRoleUUID: role.uuid,
                    terminationDate: roleRel.terminationDate,
                    terminationDateEpoch: roleRel.terminationDateEpoch
                }) as membershipRoles
            RETURN
                canonical.aliases as aliases,
                canonical.inceptionDate as inceptionDate,
                canonical.inceptionDateEpoch as inceptionDateEpoch,
                canonical.prefLabel as prefLabel,
                canonical.prefUUID as prefUUID,
                canonical.terminationDate as terminationDate,
                canonical.terminationDateEpoch as terminationDateEpoch,
				canonical.scopeNote as scopeNote,
                collect(sources) as sourceRepresentations,
                labels(canonical) as types,
                membershipRoles,
                org.uuid as organisationUUID,
                person.uuid as personUUID,
                canonical.isDeprecated as isDeprecated`,
		Parameters: map[string]interface{}{
			"uuid": uuid,
		},
		Result: &results,
	}

	err := ms.conn.CypherBatch([]*neoism.CypherQuery{query})
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
		Aliases:              results[0].Aliases,
		PrefLabel:            results[0].PrefLabel,
		PrefUUID:             results[0].PrefUUID,
		Type:                 typeName,
		OrganisationUUID:     results[0].OrganisationUUID,
		PersonUUID:           results[0].PersonUUID,
		ScopeNote:            results[0].ScopeNote,
		InceptionDate:        results[0].InceptionDate,
		InceptionDateEpoch:   results[0].InceptionDateEpoch,
		TerminationDate:      results[0].TerminationDate,
		TerminationDateEpoch: results[0].TerminationDateEpoch,
		MembershipRoles:      results[0].MembershipRoles,
		IsDeprecated:         results[0].IsDeprecated,
	}

	var sourceConcepts []concepts.Concept
	for _, srcConcept := range results[0].SourceRepresentations {
		sourceType, err := mapper.MostSpecificType(srcConcept.Types)
		if err != nil {
			logger.WithError(err).WithTransactionID(transID).WithUUID(uuid).Error("returned source concept had no recognized type")
			return concepts.AggregatedConcept{}, false, err
		}

		concept := concepts.Concept{
			Authority:         srcConcept.Authority,
			AuthorityValue:    srcConcept.AuthorityValue,
			PrefLabel:         srcConcept.PrefLabel,
			Type:              sourceType,
			UUID:              srcConcept.UUID,
			OrganisationUUID:  srcConcept.OrganisationUUID,
			PersonUUID:        srcConcept.PersonUUID,
			MembershipRoles:   srcConcept.MembershipRoles,
			LastModifiedEpoch: srcConcept.LastModifiedEpoch,
			SupersededByUUIDs: concepts.FilterSlice(srcConcept.SupersededUUIDs),
			IsDeprecated:      srcConcept.IsDeprecated,
		}
		sourceConcepts = append(sourceConcepts, concept)
	}

	aggregatedConcept.SourceRepresentations = sourceConcepts
	logger.WithTransactionID(transID).WithUUID(uuid).Debugf("returned concept is %v", aggregatedConcept)
	return concepts.CleanConcept(aggregatedConcept), true, nil
}
