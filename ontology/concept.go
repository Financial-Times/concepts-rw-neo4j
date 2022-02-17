package ontology

import (
	"errors"
)

type Relationship struct {
	UUID       string                 `json:"uuid"`
	Label      string                 `json:"label"`
	Properties map[string]interface{} `json:"properties"`
}

type NewAggregatedConcept struct {
	Properties            map[string]interface{} `json:"properties"`
	PrefUUID              string                 `json:"prefUUID,omitempty"`
	PrefLabel             string                 `json:"prefLabel,omitempty"`
	Type                  string                 `json:"type,omitempty"`
	OrganisationUUID      string                 `json:"organisationUUID,omitempty"`
	PersonUUID            string                 `json:"personUUID,omitempty"`
	AggregatedHash        string                 `json:"aggregateHash,omitempty"`
	SourceRepresentations []NewConcept           `json:"sourceRepresentations,omitempty"`
	InceptionDate         string                 `json:"inceptionDate,omitempty"`
	TerminationDate       string                 `json:"terminationDate,omitempty"`
	FigiCode              string                 `json:"figiCode,omitempty"`
	IssuedBy              string                 `json:"issuedBy,omitempty"`
	IsDeprecated          bool                   `json:"isDeprecated,omitempty"`
}

func (c NewAggregatedConcept) GetPropertyValue(propName string) (interface{}, bool) {
	val, found := c.Properties[propName]
	if !found {
		return nil, false
	}

	switch v := val.(type) {
	case []interface{}:
		return v, len(v) > 0
	case []string:
		return v, len(v) > 0
	case string:
		return v, v != ""
	case int:
		return v, v > 0
	default: // return values of unknown type but indicate that they were not validated
		return v, false
	}
}

func (c NewAggregatedConcept) GetCanonicalAuthority() string {
	for _, source := range c.SourceRepresentations {
		if source.UUID == c.PrefUUID {
			return source.Authority
		}
	}

	return ""
}

// NewConcept - could be any concept genre, subject etc
type NewConcept struct {
	Relationships     []Relationship `json:"relationships"`
	UUID              string         `json:"uuid,omitempty"`
	PrefLabel         string         `json:"prefLabel,omitempty"`
	Type              string         `json:"type,omitempty"`
	Authority         string         `json:"authority,omitempty"`
	AuthorityValue    string         `json:"authorityValue,omitempty"`
	LastModifiedEpoch int            `json:"lastModifiedEpoch,omitempty"`
	Hash              string         `json:"hash,omitempty"`
	FigiCode          string         `json:"figiCode,omitempty"`
	IssuedBy          string         `json:"issuedBy,omitempty"`
	// Organisations
	IsDeprecated bool `json:"isDeprecated,omitempty"`
}

var ErrEmptyAuthority = errors.New("invalid request, no sourceRepresentation.authority has been supplied")
var ErrUnknownAuthority = errors.New("unknown authority")
var ErrEmptyAuthorityValue = errors.New("invalid request, no sourceRepresentation.authorityValue has been supplied")

func (c NewConcept) Validate() error {
	if c.Authority == "" {
		return ErrEmptyAuthority
	}

	if !stringInArr(c.Authority, GetConfig().Authorities) {
		return ErrUnknownAuthority
	}

	if c.AuthorityValue == "" {
		return ErrEmptyAuthorityValue
	}

	return nil
}

func stringInArr(searchFor string, values []string) bool {
	for _, val := range values {
		if searchFor == val {
			return true
		}
	}
	return false
}
