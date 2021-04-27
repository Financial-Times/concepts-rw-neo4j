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

