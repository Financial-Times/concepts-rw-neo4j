package concepts

import (
	"encoding/json"
	"errors"
)

type mockConceptService struct {
	write      func(thing interface{}, transID string) (interface{}, error)
	read       func(uuid string, transID string) (interface{}, bool, error)
	delete     func(uuid string, transID string) error
	decodeJSON func(*json.Decoder) (interface{}, string, error)
	check      func() error
}

func (mcs *mockConceptService) Delete(uuid string, transID string) error {
	if mcs.delete != nil {
		return mcs.delete(uuid, transID)
	}
	return errors.New("not implemented")
}

func (mcs *mockConceptService) Write(thing interface{}, transID string) (interface{}, error) {
	if mcs.write != nil {
		return mcs.write(thing, transID)
	}
	return nil, errors.New("not implemented")
}

func (mcs *mockConceptService) Read(uuid string, transID string) (interface{}, bool, error) {
	if mcs.read != nil {
		return mcs.read(uuid, transID)
	}
	return nil, false, errors.New("not implemented")
}

func (mcs *mockConceptService) DecodeJSON(d *json.Decoder) (interface{}, string, error) {
	if mcs.decodeJSON != nil {
		return mcs.decodeJSON(d)
	}
	return nil, "", errors.New("not implemented")
}

func (mcs *mockConceptService) Check() error {
	if mcs.check != nil {
		return mcs.check()
	}
	return errors.New("not implemented")
}

func (mcs *mockConceptService) Initialise() error {
	return nil
}
