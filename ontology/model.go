package ontology

// NewAggregatedConcept defined the new in memory model
type NewAggregatedConcept struct {
	GenericConcept
	PrefUUID              string             `json:"prefUUID,omitempty"`
	Type                  string             `json:"type,omitempty"`
	AggregatedHash        string             `json:"aggregateHash,omitempty"`
	SourceRepresentations []NewSourceConcept `json:"sourceRepresentations,omitempty"`
	IssuedBy              string             `json:"issuedBy,omitempty"`
}

type NewSourceConcept struct {
	GenericConcept
	UUID              string `json:"uuid,omitempty"`
	Type              string `json:"type,omitempty"`
	LastModifiedEpoch int    `json:"lastModifiedEpoch,omitempty"`
	Hash              string `json:"hash,omitempty"`
	IssuedBy          string `json:"issuedBy,omitempty"`
}

type GenericConcept struct {
	Properties map[string]interface{} `json:"properties"`
	Relations  []Relationship         `json:"relationships"`
}

type Relationship struct {
	Label       string       `json:"label"`
	Connections []Connection `json:"connections"`
}

type Connection struct {
	UUID       string                 `json:"uuid"`
	Properties map[string]interface{} `json:"properties"`
}

func (c Connection) GetPropString(label string) (string, bool) {
	val, has := c.Properties[label]
	if !has {
		return "", false
	}
	prop, is := val.(string)
	if !is {
		return "", false
	}
	return prop, true
}

func (c GenericConcept) GetProp(label string) (interface{}, bool) {
	val, has := c.Properties[label]
	return val, has
}

func (c GenericConcept) GetPropString(label string) (string, bool) {
	val, has := c.GetProp(label)
	if !has {
		return "", false
	}
	prop, is := val.(string)
	if !is {
		return "", false
	}
	return prop, true
}

func (c GenericConcept) GetPropStringSlice(label string) ([]string, bool) {
	val, has := c.GetProp(label)
	if !has {
		return nil, false
	}
	switch prop := val.(type) {
	case []string:
		return prop, true
	case []interface{}:
		var res []string
		for _, i := range prop {
			v, ok := i.(string)
			if !ok {
				return nil, false
			}
			res = append(res, v)
		}
		return res, true
	default:
		return nil, false
	}
}

func (c GenericConcept) GetPropInt(label string) (int, bool) {
	val, has := c.GetProp(label)
	if !has {
		return 0, false
	}
	switch prop := val.(type) {
	case int:
		return prop, true
	case float64:
		return int(prop), true
	default:
		return 0, false
	}
}

func (c GenericConcept) GetPropBool(label string) (bool, bool) {
	val, has := c.GetProp(label)
	if !has {
		return false, false
	}
	prop, is := val.(bool)
	if !is {
		return false, false
	}
	return prop, true
}

func (c GenericConcept) GetRelationships(label string) []Relationship {
	var result []Relationship
	for _, rel := range c.Relations {
		if rel.Label == label {
			result = append(result, rel)
		}
	}
	return result
}

func (c GenericConcept) HasRelationships(label string) bool {
	for _, rel := range c.Relations {
		if rel.Label == label {
			return true
		}
	}
	return false
}
