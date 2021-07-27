package concepts

import (
	"testing"

	"github.com/Financial-Times/concepts-rw-neo4j/ontology"
	"github.com/google/go-cmp/cmp"
)

func TestFilterSlice(t *testing.T) {
	tests := []struct {
		name     string
		input    []string
		expected []string
	}{
		{
			name:     "nil should return nil",
			input:    nil,
			expected: nil,
		},
		{
			name:     "empty slice should return nil",
			input:    []string{},
			expected: nil,
		},
		{
			name:     "one element empty string slice should return nil",
			input:    []string{""},
			expected: nil,
		},
		{
			name:     "one element non-empty string slice should return itself",
			input:    []string{"non-empty-string"},
			expected: []string{"non-empty-string"},
		},
		{
			name:     "multiple empty strings slice should return nil",
			input:    []string{"", "", "", "", ""},
			expected: nil,
		},
		{
			name:     "multiple non-empty strings slice should return itself",
			input:    []string{"multiple", "non-empty", "strings", "slice"},
			expected: []string{"multiple", "non-empty", "strings", "slice"},
		},
		{
			name:     "multiple strings slice should return slice with non-empty strings",
			input:    []string{"multiple", "", "strings", "", "slice"},
			expected: []string{"multiple", "strings", "slice"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := filterSlice(test.input)
			if !cmp.Equal(test.expected, got) {
				t.Error(cmp.Diff(test.expected, got))
			}
		})
	}
}

func TestFilterRelationships(t *testing.T) {
	tests := []struct {
		name     string
		input    []ontology.Relationship
		expected []ontology.Relationship
	}{
		{
			name:     "nil should return nil",
			input:    nil,
			expected: nil,
		},
		{
			name:     "empty slice should return nil",
			input:    []ontology.Relationship{},
			expected: nil,
		},
		{
			name:     "single relationship with empty UUID should return nil",
			input:    []ontology.Relationship{{UUID: ""}},
			expected: nil,
		},
		{
			name:     "single relationship with non-empty UUID should return itself",
			input:    []ontology.Relationship{{UUID: "non-empty-uuid"}},
			expected: []ontology.Relationship{{UUID: "non-empty-uuid"}},
		},
		{
			name:     "multiple relationships with empty UUIDs should return nil",
			input:    []ontology.Relationship{{UUID: ""}, {UUID: ""}, {UUID: ""}},
			expected: nil,
		},
		{
			name:     "multiple relationships with non-empty UUIDs should return itself",
			input:    []ontology.Relationship{{UUID: "uuid1"}, {UUID: "uuid2"}, {UUID: "uuid3"}},
			expected: []ontology.Relationship{{UUID: "uuid1"}, {UUID: "uuid2"}, {UUID: "uuid3"}},
		},
		{
			name:     "multiple relationships slice should return relationships with non-empty UUIDs",
			input:    []ontology.Relationship{{UUID: "uuid1"}, {UUID: ""}, {UUID: "uuid2"}, {UUID: ""}, {UUID: "uuid3"}},
			expected: []ontology.Relationship{{UUID: "uuid1"}, {UUID: "uuid2"}, {UUID: "uuid3"}},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := filterRelationships(test.input)
			if !cmp.Equal(test.expected, got) {
				t.Error(cmp.Diff(test.expected, got))
			}
		})
	}
}
