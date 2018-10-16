package concepts

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/Financial-Times/go-logger"
	"github.com/Financial-Times/neo-utils-go/neoutils"
	"github.com/jmcvetta/neoism"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"os"
	"testing"
	"text/template"
)

func RunWriteFailServiceTest(
	t *testing.T,
	testName string,
	conceptsDriver ConceptServicer,
	transID string,
	concept string,
	conceptUUID string,
	expectedError string,
	sourceType func(concept string, uuid string) (ret interface{}, err error)) {

	write, err := sourceType(concept, conceptUUID)
	assert.NoError(t, err)
	_, err = conceptsDriver.Write(write, transID)
	assert.Equal(t,
		expectedError,
		err.Error(),
		fmt.Sprintf("test %s failed: actual error received differs from expected", testName))
}

func parseInvalidPayload(
	file string,
	templateName string,
	concept string,
	uuid string) (ret interface{}, err error) {
	a := struct {
		Type string
		Uuid string
	}{
		Type: concept,
		Uuid: uuid,
	}

	b, err := ioutil.ReadFile(file)
	if err != nil {
		return ret, err
	}
	tmp := string(b)
	t, err := template.New(templateName).Parse(tmp)
	if err != nil {
		logger.Errorf("Could not parse template:\n%s", tmp)
		logger.Error(err)
		return ret, err
	}

	buf := new(bytes.Buffer)
	err = t.Execute(buf, a)
	if err != nil {
		logger.Errorf("Template was:\n%s", tmp)
		logger.Error(err)
		return ret, err
	}

	dec := json.NewDecoder(buf)
	ret, _, err = DecodeJSON(dec)
	if err != nil {
		logger.Errorf("Could not decode:\n%s", string(buf.Bytes()))
	}

	return ret, err

}

func NewInvalidSourceType(concept string, uuid string) (ret interface{}, err error) {
	return parseInvalidPayload(
		"../concepts/fixtures/invalidSourceType.json",
		"invalid-source-type",
		concept,
		uuid)
}

func NewInvalidType(concept string, uuid string) (ret interface{}, err error) {
	return parseInvalidPayload(
		"../concepts/fixtures/invalidType.json",
		"invalid-type",
		concept,
		uuid)
}

func NewMissingPrefLabel(concept string, uuid string) (ret interface{}, err error) {
	logger.Info("Calling NewMissingPrefLabel")
	return parseInvalidPayload(
		"../concepts/fixtures/missingPrefLabel.json",
		"missing-pref-label",
		concept,
		uuid)
}

func NewMissingSourceAuthValue(concept string, uuid string) (ret interface{}, err error) {
	return parseInvalidPayload(
		"../concepts/fixtures/missingSourceAuthValue.json",
		"missing-source-auth-value",
		concept,
		uuid)
}

func NewMissingSourceAuth(concept string, uuid string) (ret interface{}, err error) {
	return parseInvalidPayload(
		"../concepts/fixtures/missingSourceAuth.json",
		"missing-source-auth",
		concept,
		uuid)
}

func NewMissingSources(concept string, uuid string) (ret interface{}, err error) {
	return parseInvalidPayload(
		"../concepts/fixtures/missingSources.json",
		"missing-sources",
		concept,
		uuid)
}

func NewMissingSourceType(concept string, uuid string) (ret interface{}, err error) {
	return parseInvalidPayload(
		"../concepts/fixtures/missingSourceType.json",
		"missing-source-type",
		concept,
		uuid)
}

func NewMissingType(concept string, uuid string) (ret interface{}, err error) {
	return parseInvalidPayload(
		"../concepts/fixtures/missingType.json",
		"missing-type",
		concept,
		uuid)
}

func NewMissingSourcePrefLabel(concept string, uuid string) (ret interface{}, err error) {
	return parseInvalidPayload(
		"../concepts/fixtures/missingSourcePrefLabel.json",
		"missing-source-pref-label",
		concept,
		uuid)
}

//CleanTestDB cleans the DB of each concept, all outgoing relationships, any identifiers or equivalent nodes
//Should only be used for testing purposes
//They are stored here as you cannot not export from test files otherwise they would be in concepts_service_test.go file
func CleanTestDB(t *testing.T, db neoutils.NeoConnection, uuids ...string) {
	for _, uuid := range uuids {
		cleanSourceNodes(t, db, uuid)
		deleteSourceNodes(t, db, uuid)
		deleteConcordedNodes(t, db, uuid)
	}
}

func cleanSourceNodes(t *testing.T, db neoutils.NeoConnection, uuid string) {
	var queries []*neoism.CypherQuery
	queries = append(queries, &neoism.CypherQuery{
		Statement: fmt.Sprintf(`
			MATCH (a:Thing {uuid: "%s"})
			OPTIONAL MATCH (a)-[rel:IDENTIFIES]-(i)
			OPTIONAL MATCH (a)-[hp:HAS_PARENT]->(p)
			OPTIONAL MATCH (a)-[eq:EQUIVALENT]->(e)
			DELETE rel, hp, i, eq`, uuid)})
	err := db.CypherBatch(queries)
	assert.NoError(t, err, "error executing clean up source node cypher")
}

func deleteSourceNodes(t *testing.T, db neoutils.NeoConnection, uuid string) {
	var queries []*neoism.CypherQuery
	queries = append(queries, &neoism.CypherQuery{
		Statement: fmt.Sprintf(`
			MATCH (a:Thing {uuid: "%s"})
			OPTIONAL MATCH (a)-[rel:IDENTIFIES]-(i)
			DETACH DELETE rel, i, a`, uuid)})
	err := db.CypherBatch(queries)
	assert.NoError(t, err, "error executing delete source node cypher")
}

func deleteConcordedNodes(t *testing.T, db neoutils.NeoConnection, uuid string) {
	var queries []*neoism.CypherQuery
	queries = append(queries, &neoism.CypherQuery{
		Statement: fmt.Sprintf(`
			MATCH (a:Thing {prefUUID: "%s"})
			DELETE a`, uuid)})
	err := db.CypherBatch(queries)
	assert.NoError(t, err, "Error executing delete concorded node cypher")
}

//ReadFileAndDecode will read the file and decode the contents. Used for the test fixtures
func ReadFileAndDecode(t *testing.T, pathToFile string) (interface{}, string, error) {
	f, err := os.Open(pathToFile)
	assert.NoError(t, err)
	defer f.Close()
	dec := json.NewDecoder(f)
	return DecodeJSON(dec)
}
