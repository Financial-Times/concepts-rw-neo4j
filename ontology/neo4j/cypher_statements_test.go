package neo4j

import (
	"flag"
	"io/ioutil"
	"os"
	"testing"

	cmneo4j "github.com/Financial-Times/cm-neo4j-driver"
	"github.com/google/go-cmp/cmp"
)

var update = flag.Bool("update", false, "update the golden files for tests")

func TestGetReadQuery(t *testing.T) {
	goldenFileName := "testdata/read/cypher-statement.golden"
	queries, _ := GetReadQuery("uuid")
	if diff := compareQueriesWithGoldenFile(t, goldenFileName, []*cmneo4j.Query{queries}); diff != "" {
		t.Errorf("Got unexpected Cypher statement:\n%s", diff)
	}
}

func TestClearExistingConcept(t *testing.T) {
	goldenFileName := "testdata/clear/queries.golden"
	concept := getAggregatedConcept(t, "clear/concept.json")
	queries := ClearExistingConcept(concept)
	if diff := compareQueriesWithGoldenFile(t, goldenFileName, queries); diff != "" {
		t.Errorf("Got unexpected Cypher statement:\n%s", diff)
	}
}

func TestGetLabelsToRemove(t *testing.T) {
	expected := "Concept:Classification:Section:Subject:SpecialReport:Topic:Location:Genre:Brand:Person:Organisation:MembershipRole:Membership:BoardRole:FinancialInstrument:Company:PublicCompany:IndustryClassification:NAICSIndustryClassification"
	got := getLabelsToRemove()
	if expected != got {
		t.Fatalf("expected '%s', but got '%s'", expected, got)
	}
}

// compareQueriesWithGoldenFile reads query data from a golden file and compares it as string with the actual queries
// It returns the differences it found as a string in `cmp` format.
// Otherwise, it returns an empty string.
func compareQueriesWithGoldenFile(t *testing.T, filename string, queries []*cmneo4j.Query) string {
	t.Helper()
	statement := cypherBatchToString(queries)
	expectedStatement := getFromGoldenFile(t, filename, statement, *update)
	if cmp.Equal(expectedStatement, statement) {
		return ""
	}
	return cmp.Diff(expectedStatement, statement)
}

func getFromGoldenFile(t *testing.T, fileName string, actual string, update bool) string {
	t.Helper()

	if update {
		file, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0755)
		if err != nil {
			t.Fatalf("failed to open golden file %s: %v", fileName, err)
		}
		defer file.Close()

		_, err = file.WriteString(actual)
		if err != nil {
			t.Fatalf("failed writing to golden file %s: %v", fileName, err)
		}

		return actual
	}

	file, err := os.OpenFile(fileName, os.O_RDONLY, 0755)
	if err != nil {
		t.Fatalf("failed to open golden file %s: %v", fileName, err)
	}
	defer file.Close()

	content, err := ioutil.ReadAll(file)
	if err != nil {
		t.Fatalf("failed opening golden file %s: %v", fileName, err)
	}

	return string(content)
}
