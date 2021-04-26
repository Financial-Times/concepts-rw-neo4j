package ontology

import (
	"errors"
	"fmt"
	"sort"
)

const (
	TypeThing                       = "Thing"
	TypeConcept                     = "Concept"
	TypeClassification              = "Classification"
	TypeSection                     = "Section"
	TypeSubject                     = "Subject"
	TypeSpecialReport               = "SpecialReport"
	TypeLocation                    = "Location"
	TypeTopic                       = "Topic"
	TypeGenre                       = "Genre"
	TypeBrand                       = "Brand"
	TypeAlphavilleSeries            = "AlphavilleSeries"
	TypeCompany                     = "Company"
	TypePublicCompany               = "PublicCompany"
	TypePrivateCompany              = "PrivateCompany"
	TypePerson                      = "Person"
	TypeOrganisation                = "Organisation"
	TypeMembershipRole              = "MembershipRole"
	TypeBoardRole                   = "BoardRole"
	TypeMembership                  = "Membership"
	TypeFinancialInstrument         = "FinancialInstrument"
	TypeIndustryClassification      = "IndustryClassification"
	TypeNAICSIndustryClassification = "NAICSIndustryClassification"
)

type ConceptType struct {
	Name        string
	Parent      string
	Indexes     []string
	Constraints []string
}

var conceptTypes = map[string]ConceptType{
	TypeThing:                       {Name: TypeThing, Constraints: []string{UUIDProp, PrefUUIDProp}, Indexes: []string{AuthorityValueProp}},
	TypeConcept:                     {Name: TypeConcept, Parent: TypeThing, Constraints: []string{UUIDProp, PrefUUIDProp}, Indexes: []string{LeiCodeProp, AuthorityValueProp}},
	TypeClassification:              {Name: TypeClassification, Parent: TypeConcept, Constraints: []string{UUIDProp}},
	TypeSection:                     {Name: TypeSection, Parent: TypeClassification, Constraints: []string{UUIDProp}},
	TypeSubject:                     {Name: TypeSubject, Parent: TypeClassification, Constraints: []string{UUIDProp}},
	TypeSpecialReport:               {Name: TypeSpecialReport, Parent: TypeClassification, Constraints: []string{UUIDProp}},
	TypeLocation:                    {Name: TypeLocation, Parent: TypeConcept, Constraints: []string{UUIDProp, ISO31661Prop}},
	TypeTopic:                       {Name: TypeTopic, Parent: TypeConcept, Constraints: []string{UUIDProp}},
	TypeGenre:                       {Name: TypeGenre, Parent: TypeClassification, Constraints: []string{UUIDProp}},
	TypeBrand:                       {Name: TypeBrand, Parent: TypeClassification, Constraints: []string{UUIDProp}},
	TypeAlphavilleSeries:            {Name: TypeAlphavilleSeries, Parent: TypeClassification, Constraints: []string{UUIDProp}},
	TypeCompany:                     {Name: TypeCompany, Parent: TypeOrganisation},
	TypePrivateCompany:              {Name: TypePrivateCompany, Parent: TypeCompany},
	TypePublicCompany:               {Name: TypePublicCompany, Parent: TypeCompany, Constraints: []string{UUIDProp}},
	TypePerson:                      {Name: TypePerson, Parent: TypeConcept, Constraints: []string{UUIDProp}},
	TypeOrganisation:                {Name: TypeOrganisation, Parent: TypeConcept, Constraints: []string{UUIDProp}},
	TypeMembershipRole:              {Name: TypeMembershipRole, Parent: TypeConcept, Constraints: []string{UUIDProp}},
	TypeBoardRole:                   {Name: TypeBoardRole, Parent: TypeMembershipRole, Constraints: []string{UUIDProp}},
	TypeMembership:                  {Name: TypeMembership, Parent: TypeConcept, Constraints: []string{UUIDProp}},
	TypeFinancialInstrument:         {Name: TypeFinancialInstrument, Parent: TypeConcept, Constraints: []string{UUIDProp}},
	TypeIndustryClassification:      {Name: TypeIndustryClassification, Parent: TypeConcept, Constraints: []string{UUIDProp}},
	TypeNAICSIndustryClassification: {Name: TypeNAICSIndustryClassification, Parent: TypeIndustryClassification, Constraints: []string{UUIDProp, IndustryIdentifierProp}},
}

func GetConceptTypeConstraints() map[string][]string {
	result := map[string][]string{}
	for label, t := range conceptTypes {
		result[label] = t.Constraints
	}
	return result
}
func GetConceptTypeIndexes() map[string][]string {
	result := map[string][]string{}
	for label, t := range conceptTypes {
		result[label] = t.Indexes
	}
	return result
}

//return all concept labels
func GetConceptTypeLabels(label string) []string {
	labels := []string{label}
	for t, ok := conceptTypes[label]; ok && t.Parent != ""; t, ok = conceptTypes[t.Parent] {
		labels = append(labels, t.Parent)
	}
	return labels
}

//return existing labels
func GetRemovableConceptTypeLabels() []string {
	var labels []string
	for label := range conceptTypes {
		if label == TypeThing {
			continue
		}
		labels = append(labels, label)
	}
	return labels
}

func HasType(label string) bool {
	_, ok := conceptTypes[label]
	return ok
}

// ParentType returns the immediate parent type for a given Type
func ParentType(t ConceptType) ConceptType {
	return conceptTypes[t.Parent]
}

func isDescendent(descendent, ancestor ConceptType) bool {
	for t := descendent; t.Name != ""; t = ParentType(t) {
		if t.Name == ancestor.Name {
			return true
		}
	}
	return false
}

// MostSpecific returns the most specific from a list of types in an hierarchy
// behaviour is undefined if any of the types are siblings.
func MostSpecificType(typeLabels []string) (string, error) {
	if len(typeLabels) == 0 {
		return "", errors.New("no types supplied")
	}
	var types []ConceptType
	for _, l := range typeLabels {
		t, ok := conceptTypes[l]
		if !ok {
			return "", fmt.Errorf("unknow concept type '%s'", l)
		}
		types = append(types, t)
	}
	sorted, err := SortTypes(types)
	if err != nil {
		return "", err
	}
	return sorted[len(sorted)-1].Name, nil
}

var ErrNotHierarchy = errors.New("provided types are not a consistent hierarchy")

// SortTypes sorts the given types from least specific to most specific
func SortTypes(types []ConceptType) ([]ConceptType, error) {
	ts := &typeSorter{types: make([]ConceptType, len(types))}
	copy(ts.types, types)
	sort.Sort(ts)
	if ts.invalid {
		return types, ErrNotHierarchy
	}
	return ts.types, nil
}

type typeSorter struct {
	types   []ConceptType
	invalid bool
}

func (ts *typeSorter) Len() int {
	return len(ts.types)
}

func (ts *typeSorter) Less(a, b int) bool {
	at := ts.types[a]
	bt := ts.types[b]
	if isDescendent(bt, at) {
		return true
	}
	if !isDescendent(at, bt) {
		ts.invalid = true
	}
	return false
}

func (ts *typeSorter) Swap(a, b int) {
	ts.types[a], ts.types[b] = ts.types[b], ts.types[a]
}
