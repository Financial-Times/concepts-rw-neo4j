{{ reserveImport "encoding/json" }}
{{ reserveImport "github.com/jmcvetta/neoism" }}

{{ define "optionalMatches" }}
{{- range $objField := .Fields }}
    {{- range $fieldDirective := $objField.Directives }}
        {{- if eq $fieldDirective.Name "relation" -}}
            {{ $relationName := index $fieldDirective.Args 0 }}
            {{- $relationTo := index $fieldDirective.Args 1 }}
                    OPTIONAL MATCH (source)-[{{$objField.Name}}Rel:{{$relationName.Value}}]->({{$objField.Name}}Node:{{$relationTo.Value}})
        {{- end }}
    {{- end }}
{{- end -}}
{{ end }}

{{ define "collectRelations" }}
{{- range $objField := .Fields }}
    {{- range $fieldDirective := $objField.Directives }}
        {{- if eq $fieldDirective.Name "relation" -}}
            {{$relationProps := index $fieldDirective.Args 2}}
            {{- if eq $objField.Type.NamedType "" }}
                {{- if not $relationProps.Value }}
                                    collect(DISTINCT {{$objField.Name}}Node.uuid) as {{ $objField.Name }},
                {{- else }}
                                    collect(DISTINCT {
                                        {{- range $relProp := $relationProps.Value -}}
                                            {{ $relProp|ucFirst }}: {{ $objField.Name }}Rel.{{ $relProp }},
                                        {{- end -}}
                                        UUID: {{ $objField.Name }}Node.uuid}) as {{ $objField.Name }},
                {{- end }}
            {{- else }}
                                    {{$objField.Name}}Node,
            {{- end }}
        {{- end -}}
    {{- end }}
{{- end -}}
{{ end }}

{{ define "sourceProperties" }}
{{- range $objField := .Fields }}
    {{- range $fieldDirective := $objField.Directives }}
        {{- if eq $fieldDirective.Name "relation" }}
            {{- if eq $objField.Type.NamedType "" }}
                            {{$objField.Name}}: {{$objField.Name}},
            {{- else }}
                            {{$objField.Name}}: {{$objField.Name}}Node.uuid,
            {{- end }}
        {{- end }}
    {{- end }}
{{- end -}}
{{ end }}

{{ define "canonicalProperties" }}
{{- range $objField := .Fields }}
    {{- $directivesCount := len $objField.Directives}}
    {{- if eq $directivesCount 0 }}
                        canonical.{{$objField.Name}} as {{$objField.Name}},
    {{- end }}
{{- end -}}
{{ end }}

{{- range $object := .Objects }}
    {{- if not $object.Definition.BuiltIn -}}
        {{- if not $object.Root -}}
            func {{$object.Name|lcFirst}}CypherReadQuery(uuid string) *neoism.CypherQuery {
                query := &neoism.CypherQuery{
                    Statement: `
                    MATCH (canonical:Thing {prefUUID:{uuid}})<-[:EQUIVALENT_TO]-(source:Thing)
                    {{- template "optionalMatches" $object }}
                    WITH
                        canonical,
                        {{- template "collectRelations" $object }}
                        source
                        ORDER BY
                            source.uuid
                    WITH
                        canonical,
                        {
                            authority: source.authority,
                            authorityValue: source.authorityValue,
                            prefLabel: source.prefLabel,
                            types: labels(source),
                            uuid: source.uuid,
                            {{- template "sourceProperties" $object }}
                            lastModifiedEpoch: source.lastModifiedEpoch
                        } as sources 
                        RETURN 
                        canonical.prefUUID as prefUUID,
                        canonical.aggregateHash as aggregateHash,
                        {{- template "canonicalProperties" $object }}
                        labels(canonical) as types,
                        collect(sources) as sourceRepresentations`,
                    Parameters: map[string]interface{}{
                        "uuid": uuid,
                    },
                }

                return query
            }

            func {{$object.Name|lcFirst}}Props(concept interface{}, id string, isSource bool) map[string]interface{} {
                var props map[string]interface{}
                tmp, _ := json.Marshal(concept)
                json.Unmarshal(tmp, &props)

                objFields := map[string]bool{
                {{- range $objField := $object.Fields }}
                    {{- $directivesCount := len $objField.Directives}}
                    {{- if eq $directivesCount 0 }}
                        {{$objField.Name|quote}}: true,
                    {{- end }}
                {{- end }}
                }

                if isSource {
                    objFields["authority"] = true
                    objFields["authorityValue"] = true
                }

                for k := range props {
                    if !objFields[k] {
                        delete(props, k)
                    }
                }

                if isSource {
                    props["uuid"] = id
                } else {
                    props["prefUUID"] = id
                }

                return props
            }

            func mapTo{{$object.Name}}(concordedConcept interface{}) interface{} {
                var model = {{$object.Name}}{}
                tmp, _ := json.Marshal(concordedConcept)
                json.Unmarshal(tmp, &model)

                return model
            }
        {{ end -}}
    {{ end -}}
{{- end }}

func CypherReadQuery(uuid string, conceptType string) *neoism.CypherQuery {
	switch conceptType {
    {{ range $object := .Objects }}
	    {{- if not $object.Definition.BuiltIn -}}
            {{- if not $object.Root -}}
                case "{{$object.Name|lcFirst}}s":
                    return {{$object.Name|lcFirst}}CypherReadQuery(uuid)
            {{ end -}}
        {{ end -}}
    {{ end }}default:
		return nil
	}
}

func Props(conceptType string, concept interface{}, id string, isSource bool) map[string]interface{} {
	switch conceptType {
    {{ range $object := .Objects }}
	    {{- if not $object.Definition.BuiltIn -}}
            {{- if not $object.Root -}}
                case "{{$object.Name|lcFirst}}s":
                    return {{$object.Name|lcFirst}}Props(concept, id, isSource)
            {{ end -}}
        {{ end -}}
    {{ end }}default:
		return map[string]interface{}{}
	}
}

func IsKnownType(conceptType string) bool {
    knownTypes := map[string]bool{
        {{ range $object := .Objects }}
            {{- if not $object.Definition.BuiltIn -}}
                {{- if not $object.Root -}}
                    "{{$object.Name|lcFirst}}s": true,
                {{ end -}}
            {{ end -}}
        {{- end }}
    }

    return knownTypes[conceptType]
}

func MapToKnownType(conceptType string, concordedConcept interface{}) interface{} {
	switch conceptType {
    {{ range $object := .Objects }}
	    {{- if not $object.Definition.BuiltIn -}}
            {{- if not $object.Root -}}
                case "{{$object.Name|lcFirst}}s":
                    return mapTo{{$object.Name}}(concordedConcept)
            {{ end -}}
        {{ end -}}
    {{ end }}default:
		return concordedConcept
	}
}
