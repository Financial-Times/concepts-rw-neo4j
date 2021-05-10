{{ reserveImport "encoding/json" }}
{{ reserveImport "github.com/jmcvetta/neoism" }}

{{- range $object := .Objects }}
    {{- if not $object.Definition.BuiltIn -}}
        {{- if not $object.Root -}}
            func {{$object.Name|lcFirst}}CypherReadQuery(uuid string) *neoism.CypherQuery {
                query := &neoism.CypherQuery{
                    Statement: 
                    {{- range $objDirective := $object.Definition.Directives }}
                        {{- if eq $objDirective.Name "cypher" -}}
                            {{$cypherStatement := index $objDirective.Arguments 0}}
                                {{$cypherStatement.Value}}
                        {{- end }}
                    {{- end }},
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
                        {{$objField.Name|quote}}: true,
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
