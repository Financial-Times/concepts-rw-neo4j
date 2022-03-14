package ontology

import "errors"

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

func (c NewAggregatedConcept) Validate() error {
	constraintMap := map[string]bool{
		"Thing": true,
	}
	for _, ct := range GetConfig().GetConceptTypes() {
		constraintMap[ct] = true
	}

	if c.PrefLabel == "" {
		return newValidationPropertyErr(c.PrefUUID, "prefLabel", EmptyPropertyErrReason, nil)
	}

	if _, ok := constraintMap[c.Type]; !ok {
		return newValidationPropertyErr(c.PrefUUID, "type", UnknownPropertyErrReason, c.Type)
	}

	if c.SourceRepresentations == nil {
		return newValidationPropertyErr(c.PrefUUID, "sourceRepresentation", EmptyPropertyErrReason, nil)
	}

	if err := GetConfig().ValidateProperties(c.Properties); err != nil {
		return err
	}

	for _, sourceConcept := range c.SourceRepresentations {
		if err := sourceConcept.Validate(); err != nil {
			var propErr *ValidationPropertyErr
			if errors.As(err, &propErr) {
				propErr.Property = "sourceRepresentation." + propErr.Property
				return propErr
			}
			return err
		}
	}
	return nil
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

func (c NewConcept) Validate() error {

	constraintMap := map[string]bool{
		"Thing": true,
	}
	for _, ct := range GetConfig().GetConceptTypes() {
		constraintMap[ct] = true
	}

	if c.Type == "" {
		return newValidationPropertyErr(c.UUID, "type", EmptyPropertyErrReason, nil)
	}

	if _, ok := constraintMap[c.Type]; !ok {
		return newValidationPropertyErr(c.UUID, "type", UnknownPropertyErrReason, c.Type)
	}

	if c.Authority == "" {
		return newValidationPropertyErr(c.UUID, "authority", EmptyPropertyErrReason, nil)
	}

	if !stringInArr(c.Authority, GetConfig().Authorities) {
		return newValidationPropertyErr(c.UUID, "authority", UnknownPropertyErrReason, c.Authority)
	}

	if c.AuthorityValue == "" {
		return newValidationPropertyErr(c.UUID, "authorityValue", EmptyPropertyErrReason, c.Authority)
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
