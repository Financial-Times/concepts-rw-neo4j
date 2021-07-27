package concepts

import (
	"fmt"
	"sort"
	"strings"

	"github.com/Financial-Times/concepts-rw-neo4j/ontology"
)

func getReadStatement() string {
	statementTemplate := `
		MATCH (canonical:Thing {prefUUID:{uuid}})<-[:EQUIVALENT_TO]-(source:Thing)
		OPTIONAL MATCH (source)-[:ISSUED_BY]->(issuer:Thing)
		%s
		WITH
			canonical,
			issuer,
			source,
			%s
			ORDER BY
				source.uuid,
				hasRoleNode.uuid
		WITH
			canonical,
			issuer,
			hasMemberNode,
			hasOrganisationNode,
			{
				uuid: source.uuid,
				prefLabel: source.prefLabel,
				types: labels(source),
				authority: source.authority,
				authorityValue: source.authorityValue,
				figiCode: source.figiCode,
				lastModifiedEpoch: source.lastModifiedEpoch,
				isDeprecated: source.isDeprecated,
				industryIdentifier: source.industryIdentifier,
				issuedBy: issuer.uuid,
				%s
			} as sources,
			collect({
				inceptionDate: hasRoleRel.inceptionDate,
				inceptionDateEpoch: hasRoleRel.inceptionDateEpoch,
				membershipRoleUUID: hasRoleNode.uuid,
				terminationDate: hasRoleRel.terminationDate,
				terminationDateEpoch: hasRoleRel.terminationDateEpoch
			}) as membershipRoles
		RETURN
			canonical.aggregateHash as aggregateHash,
			canonical.aliases as aliases,
			canonical.descriptionXML as descriptionXML,
			canonical.emailAddress as emailAddress,
			canonical.facebookPage as facebookPage,
			canonical.figiCode as figiCode,
			canonical.imageUrl as imageUrl,
			canonical.inceptionDate as inceptionDate,
			canonical.inceptionDateEpoch as inceptionDateEpoch,
			canonical.prefLabel as prefLabel,
			canonical.prefUUID as prefUUID,
			canonical.scopeNote as scopeNote,
			canonical.shortLabel as shortLabel,
			canonical.strapline as strapline,
			canonical.terminationDate as terminationDate,
			canonical.terminationDateEpoch as terminationDateEpoch,
			canonical.twitterHandle as twitterHandle,
			canonical.properName as properName,
			canonical.shortName as shortName,
			canonical.tradeNames as tradeNames,
			canonical.formerNames as formerNames,
			canonical.countryCode as countryCode,
			canonical.countryOfIncorporation as countryOfIncorporation,
			canonical.countryOfOperations as countryOfOperations,
			canonical.countryOfRisk as countryOfRisk,
			canonical.postalCode as postalCode,
			canonical.yearFounded as yearFounded,
			canonical.leiCode as leiCode,
			canonical.isDeprecated as isDeprecated,
			canonical.salutation as salutation,
			canonical.birthYear as birthYear,
			canonical.iso31661 as iso31661,
			canonical.industryIdentifier as industryIdentifier,
			issuer.uuid as issuedBy,
			hasOrganisationNode.uuid as organisationUUID,
			hasMemberNode.uuid as personUUID,
			membershipRoles,
			labels(canonical) as types,
			collect(sources) as sourceRepresentations`

	return fmt.Sprintf(statementTemplate,
		strings.Join(getOptionalMatchesForRead(), "\n"),
		strings.Join(getWithMatchedForRead(), ",\n"),
		strings.Join(getSourceRelsForRead(), ",\n"))
}

func getDeleteStatement() string {
	statementTemplate := `
		MATCH (t:Thing {uuid:{id}})
		OPTIONAL MATCH (t)-[eq:EQUIVALENT_TO]->(a:Thing)
		OPTIONAL MATCH (t)-[issuerRel:ISSUED_BY]->(issuer)
		%s
		REMOVE t:%s
		SET t={uuid:{id}}
		DELETE eq, issuerRel, %s`

	return fmt.Sprintf(statementTemplate,
		strings.Join(getOptionalMatchesForDelete(), "\n"),
		getLabelsToRemove(),
		strings.Join(getRelNamesForDelete(), ", "))
}

func getOptionalMatchesForRead() []string {
	var relOptionalMatches []string
	for relLabel, relCfg := range ontology.GetConfig().Relationships {
		relOptionalMatches = append(relOptionalMatches, getOptionalMatchForRead(relLabel, relCfg))
	}

	for relLabel, relCfg := range relationships {
		relOptionalMatches = append(relOptionalMatches, getOptionalMatchForRead(relLabel, relCfg))
	}

	sort.Strings(relOptionalMatches)
	return relOptionalMatches
}

func getWithMatchedForRead() []string {
	var withMatched []string
	for relLabel, relCfg := range ontology.GetConfig().Relationships {
		withMatched = append(withMatched, getMatchedForRead(relLabel, relCfg)...)
	}

	for relLabel, relCfg := range relationships {
		withMatched = append(withMatched, getMatchedForRead(relLabel, relCfg)...)
	}

	sort.Strings(withMatched)
	return withMatched
}

func getSourceRelsForRead() []string {
	var sourceRels []string
	for relLabel, relCfg := range ontology.GetConfig().Relationships {
		sourceRels = append(sourceRels, getSourceRelForRead(relLabel, relCfg))
	}

	for relLabel, relCfg := range relationships {
		sourceRels = append(sourceRels, getSourceRelForRead(relLabel, relCfg))
	}

	sort.Strings(sourceRels)
	return sourceRels
}

func getOptionalMatchesForDelete() []string {
	var relOptionalMatches []string
	for relLabel := range ontology.GetConfig().Relationships {
		relOptionalMatches = append(relOptionalMatches, getOptionalMatchForDelete(relLabel))
	}

	for relLabel := range relationships {
		relOptionalMatches = append(relOptionalMatches, getOptionalMatchForDelete(relLabel))
	}

	sort.Strings(relOptionalMatches)
	return relOptionalMatches
}

func getRelNamesForDelete() []string {
	var relNames []string
	for relLabel := range ontology.GetConfig().Relationships {
		r := toCamelCase(relLabel)
		relName := r + "Rel"
		relNames = append(relNames, relName)
	}

	for relLabel := range relationships {
		r := toCamelCase(relLabel)
		relName := r + "Rel"
		relNames = append(relNames, relName)
	}

	sort.Strings(relNames)
	return relNames
}

func getOptionalMatchForRead(relLabel string, relCfg ontology.RelationshipConfig) string {
	r := toCamelCase(relLabel)
	nodeName := r + "Node"

	var relName string
	if len(relCfg.Properties) > 0 {
		relName = r + "Rel"
	}

	toNodeLabel := "Thing"
	if relCfg.ToNodeWithLabel != "" {
		toNodeLabel = relCfg.ToNodeWithLabel
	}

	return fmt.Sprintf("OPTIONAL MATCH (source)-[%s:%s]->(%s:%s)", relName, relLabel, nodeName, toNodeLabel)
}

func getMatchedForRead(relLabel string, relCfg ontology.RelationshipConfig) []string {
	r := toCamelCase(relLabel)
	nodeName := r + "Node"

	var relName string
	if len(relCfg.Properties) > 0 {
		relName = r + "Rel"
	}

	matched := []string{nodeName}
	if len(relCfg.Properties) > 0 {
		matched = append(matched, relName)
	}

	return matched
}

func getSourceRelForRead(relLabel string, relCfg ontology.RelationshipConfig) string {
	r := toCamelCase(relLabel)
	nodeName := r + "Node"

	var relName string
	if len(relCfg.Properties) > 0 {
		relName = r + "Rel"
	}

	if relCfg.OneToOne {
		return fmt.Sprintf("%s: %s.uuid", relCfg.ConceptField, nodeName)
	} else if len(relCfg.Properties) == 0 {
		return fmt.Sprintf("%s: collect(DISTINCT %s.uuid)", relCfg.ConceptField, nodeName)
	} else {
		var relProps []string
		for _, relProp := range relCfg.Properties {
			relProps = append(relProps, fmt.Sprintf("%s: %s.%s", relProp, relName, relProp))
		}

		uuidField := "UUID"
		if relLabel == "HAS_ROLE" {
			uuidField = "membershipRoleUUID"
		}

		return fmt.Sprintf("%s: collect(DISTINCT {%s: %s.uuid, %s})",
			relCfg.ConceptField,
			uuidField,
			nodeName,
			strings.Join(relProps, ", "))
	}
}

func getOptionalMatchForDelete(relLabel string) string {
	r := toCamelCase(relLabel)
	relName := r + "Rel"
	nodeName := r + "Node"
	return fmt.Sprintf("OPTIONAL MATCH (t)-[%s:%s]->(%s)", relName, relLabel, nodeName)
}

func toCamelCase(relLabel string) string {
	labelLower := strings.ToLower(relLabel)
	var vals []string
	for _, val := range strings.Split(labelLower, "_") {
		vals = append(vals, strings.Title(val))
	}

	vals[0] = strings.ToLower(vals[0])
	return strings.Join(vals, "")
}
