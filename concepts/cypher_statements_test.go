package concepts

import (
	"flag"
	"io/ioutil"
	"os"
	"testing"

	"github.com/google/go-cmp/cmp"
)

var update = flag.Bool("update", false, "update the golden files for tests")

func TestGenerateCypherStatements(t *testing.T) {
	tests := []struct {
		name           string
		statementFunc  func() string
		goldenFileName string
	}{
		{
			name:           "read statement",
			statementFunc:  getReadStatement,
			goldenFileName: "testdata/cypher-read-statement.golden",
		},
		{
			name:           "delete statement",
			statementFunc:  getDeleteStatement,
			goldenFileName: "testdata/cypher-delete-statement.golden",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			statement := test.statementFunc()
			expectedStatement := getFromGoldenFile(t, test.goldenFileName, statement, *update)
			if !cmp.Equal(expectedStatement, statement) {
				t.Errorf("Got unexpected Cypher statement:\n%s", cmp.Diff(expectedStatement, statement))
			}
		})
	}
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
