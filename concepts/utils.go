package concepts

import "time"

func aggregatedConceptToGraph(ac AggregatedConcept) (*Node, error) {
	// create the canonical node
	canonicalNode := &Node{
		PrimaryKey: ac.PrefUUID,
		Relations:  []*Relation{},
		Labels:     getAllLabels(ac.Type),
		Attributes: map[string]interface{}{
			"aggregateHash":        ac.AggregatedHash,
			"aliases":              ac.Aliases,
			"descriptionXML":       ac.DescriptionXML,
			"emailAddress":         ac.EmailAddress,
			"facebookPage":         ac.FacebookPage,
			"imageURL":             ac.ImageURL,
			"imageUrl":             ac.ImageURL,
			"lastModifiedEpoch":    time.Now().Unix(),
			"prefLabel":            ac.PrefLabel,
			"scopeNote":            ac.ScopeNote,
			"shortLabel":           ac.ShortLabel,
			"strapline":            ac.Strapline,
			"twitterHandle":        ac.TwitterHandle,
			"type":                 ac.Type,
			"figiCode":             ac.FigiCode,
			"inceptionDate":        ac.InceptionDate,
			"terminationDate":      ac.TerminationDate,
			"inceptionDateEpoch":   ac.InceptionDateEpoch,
			"terminationDateEpoch": ac.TerminationDateEpoch,
		},
		Options: []Option{
			PrimaryKeyName("prefUUID"),
			IfNotModified(),
		},
	}

	// go through all sources and create nodes for them
	for _, source := range ac.SourceRepresentations {
		sourceNode := &Node{
			PrimaryKey: source.UUID,
			Relations:  []*Relation{},
			Labels:     getAllLabels(source.Type),
			Attributes: map[string]interface{}{
				"aliases":              source.Aliases,
				"authority":            source.Authority,
				"authValue":            source.AuthorityValue,
				"descriptionXML":       source.DescriptionXML,
				"emailAddress":         source.EmailAddress,
				"facebookPage":         source.FacebookPage,
				"figiCode":             source.FigiCode,
				"imageURL":             source.ImageURL,
				"imageUrl":             source.ImageURL,
				"inceptionDate":        source.InceptionDate,
				"inceptionDateEpoch":   source.InceptionDateEpoch,
				"lastModifiedEpoch":    time.Now().Unix(),
				"prefLabel":            source.PrefLabel,
				"scopeNote":            source.ScopeNote,
				"shortLabel":           source.ShortLabel,
				"strapline":            source.Strapline,
				"terminationDate":      source.TerminationDate,
				"terminationDateEpoch": source.TerminationDateEpoch,
				"twitterHandle":        source.TwitterHandle,
				"type":                 source.Type,
			},
		}

		// go through all related and create nodes for them
		for _, related := range source.RelatedUUIDs {
			sourceNode.Relations = append(sourceNode.Relations, &Relation{
				Name: "IS_RELATED_TO",
				To: &Node{
					Labels:     "Thing",
					PrimaryKey: related,
					Relations: []*Relation{{
						Name: "IDENTIFIES",
						From: &Node{
							Labels:     "Identifier:UPPIdentifier",
							PrimaryKey: related,
						},
					}},
				},
			})
		}

		// go through all broader and create nodes for them
		for _, related := range source.BroaderUUIDs {
			sourceNode.Relations = append(sourceNode.Relations, &Relation{
				Name: "HAS_BROADER",
				To: &Node{
					Labels:     "Thing",
					PrimaryKey: related,
					Relations: []*Relation{{
						Name: "IDENTIFIES",
						From: &Node{
							Labels:     "Identifier:UPPIdentifier",
							PrimaryKey: related,
						},
					}},
				},
			})
		}

		// go through all parents and create nodes for them
		for _, related := range source.ParentUUIDs {
			sourceNode.Relations = append(sourceNode.Relations, &Relation{
				Name: "HAS_PARENT",
				To: &Node{
					Labels:     "Thing",
					PrimaryKey: related,
					Relations: []*Relation{{
						Name: "IDENTIFIES",
						From: &Node{
							Labels:     "Identifier:UPPIdentifier",
							PrimaryKey: related,
						},
					}},
				},
			})
		}

		// go through all membership roles and create nodes for them
		for _, related := range source.MembershipRoles {
			sourceNode.Relations = append(sourceNode.Relations, &Relation{
				Name: "HAS_ROLE",
				To: &Node{
					Labels:     "Thing",
					PrimaryKey: related.RoleUUID,
					Relations: []*Relation{{
						Name: "IDENTIFIES",
						From: &Node{
							Labels:     "Identifier:UPPIdentifier",
							PrimaryKey: related.RoleUUID,
						},
					}},
					Attributes: map[string]interface{}{
						"inceptionDate":        related.InceptionDate,
						"inceptionDateEpoch":   related.InceptionDateEpoch,
						"terminationDate":      related.TerminationDate,
						"terminationDateEpoch": related.TerminationDateEpoch,
					},
				},
			})
		}

		// create related organization node
		if related := source.OrganisationUUID; related != "" {
			sourceNode.Relations = append(sourceNode.Relations, &Relation{
				Name: "HAS_ORGANISATION",
				To: &Node{
					Labels:     "Thing",
					PrimaryKey: related,
					Relations: []*Relation{{
						Name: "IDENTIFIES",
						From: &Node{
							Labels:     "Identifier:UPPIdentifier",
							PrimaryKey: related,
						},
					}},
				},
			})
		}

		// create related organization node
		if related := source.PersonUUID; related != "" {
			sourceNode.Relations = append(sourceNode.Relations, &Relation{
				Name: "HAS_MEMBER",
				To: &Node{
					Labels:     "Thing",
					PrimaryKey: related,
					Relations: []*Relation{{
						Name: "IDENTIFIES",
						From: &Node{
							Labels:     "Identifier:UPPIdentifier",
							PrimaryKey: related,
						},
					}},
				},
			})
		}

		// create financial instrument issuer node
		if related := source.FigiCode; related != "" {
			sourceNode.Relations = append(sourceNode.Relations, &Relation{
				Name: "ISSUED_BY",
				To: &Node{
					Labels:     "Thing",
					PrimaryKey: related,
					Relations: []*Relation{{
						Name: "IDENTIFIES",
						From: &Node{
							Labels:     "Identifier:UPPIdentifier",
							PrimaryKey: related,
						},
					}},
				},
			})
		}

		// relate source node to canonical node
		canonicalNode.Relations = append(canonicalNode.Relations, &Relation{
			Name:       "EQUIVALENT_TO",
			To:         sourceNode,
			Attributes: map[string]interface{}{},
		})
	}

	return canonicalNode, nil
}
