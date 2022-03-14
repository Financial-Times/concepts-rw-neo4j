package ontology

import "errors"

const EmptyPropertyErrReason = "empty"
const UnknownPropertyErrReason = "unknown property"
const InvalidTypePropertyErrReason = "invalid type"

type ValidationPropertyErr struct {
	ConceptUUID string
	Property    string
	Reason      string
	Value       interface{}
}

func newValidationPropertyErr(conceptUUID string, property string, reason string, value interface{}) error {
	return &ValidationPropertyErr{ConceptUUID: conceptUUID, Property: property, Reason: reason, Value: value}
}

func (e ValidationPropertyErr) Error() string {
	return e.Property + " is " + e.Reason
}

//
var ErrUnknownProperty = errors.New("unknown concept property")
var ErrInvalidPropertyValue = errors.New("invalid property value")
