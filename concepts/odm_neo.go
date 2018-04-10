package concepts

import (
	"fmt"

	"github.com/Financial-Times/neo-utils-go/neoutils"
	"github.com/jmcvetta/neoism"
)

type NeoGraphManager struct {
	conn neoutils.NeoConnection
}

func (gm *NeoGraphManager) Write(*Node) error {
	return nil
}

func (gm *NeoGraphManager) getNodeQueries(node *Node) ([]*neoism.CypherQuery, error) {
	options := NewDefaultOptions()
	options.Parse(node.Options)

	queries := []*neoism.CypherQuery{}

	statement := fmt.Sprintf(`
		MATCH (node: {%s:{primaryKey}})
		OPTIONAL MATCH (t)-[:EQUIVALENT_TO]->(c)
		OPTIONAL MATCH (c)<-[eq:EQUIVALENT_TO]-(x:Thing)
		RETURN t.uuid as sourceUuid, c.prefUUID as prefUuid, COUNT(DISTINCT eq) as count
	`,
		options.PrimaryKeyName,
	)

	queries = append(queries, &neoism.CypherQuery{
		Statement:  statement,
		Parameters: map[string]interface{}{},
	})
	return queries, nil
}
