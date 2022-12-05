package concepts

type ConceptChanges struct {
	ChangedRecords []Event  `json:"events"`
	UpdatedIds     []string `json:"updatedIDs"`
}

type Event struct {
	ConceptType   string      `json:"type"`
	ConceptUUID   string      `json:"uuid"`
	AggregateHash string      `json:"aggregateHash"`
	TransactionID string      `json:"transactionID"`
	EventDetails  interface{} `json:"eventDetails"`
}

type ConceptEvent struct {
	Type string `json:"eventType"`
}

type ConcordanceEvent struct {
	Type  string `json:"eventType"`
	OldID string `json:"oldID"`
	NewID string `json:"newID"`
}

type ConceptChangeLogEvent struct {
	Type              string `json:"eventType"`
	AnnotationsChange bool   `json:"annotationsChange"`
	ChangeLog         string `json:"changelog"`
}
