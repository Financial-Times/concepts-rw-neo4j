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

// Map of all the possible node types so we can ensure they all have
// constraints on the uuid
var constraintMap = map[string]string{
	"Thing":                       "uuid",
	"Concept":                     "uuid",
	"Classification":              "uuid",
	"Section":                     "uuid",
	"Subject":                     "uuid",
	"SpecialReport":               "uuid",
	"Location":                    "uuid",
	"Topic":                       "uuid",
	"Genre":                       "uuid",
	"Brand":                       "uuid",
	"AlphavilleSeries":            "uuid",
	"PublicCompany":               "uuid",
	"Person":                      "uuid",
	"Organisation":                "uuid",
	"MembershipRole":              "uuid",
	"BoardRole":                   "uuid",
	"Membership":                  "uuid",
	"FinancialInstrument":         "uuid",
	"IndustryClassification":      "uuid",
	"NAICSIndustryClassification": "uuid",
}
