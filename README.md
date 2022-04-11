# Concept Reader/Writer for Neo4j (concept-rw-neo4j) 

[![Circle CI](https://circleci.com/gh/Financial-Times/concepts-rw-neo4j.svg?style=shield)](https://circleci.com/gh/Financial-Times/concepts-rw-neo4j)
[![Go Report Card](https://goreportcard.com/badge/github.com/Financial-Times/concepts-rw-neo4j)](https://goreportcard.com/report/github.com/Financial-Times/concepts-rw-neo4j)
[![Coverage Status](https://coveralls.io/repos/github/Financial-Times/concepts-rw-neo4j/badge.svg)](https://coveralls.io/github/Financial-Times/concepts-rw-neo4j)

__An API for reading/writing concepts into Neo4j.__ 

## Installation

        go get github.com/Financial-Times/concepts-rw-neo4j
        cd $GOPATH/src/github.com/Financial-Times/concepts-rw-neo4j
        go build -mod=readonly

## Running locally

```
Usage: concepts-rw-neo4j [OPTIONS]

A RESTful API for managing Concepts in Neo4j

Options:
      --app-system-code    System Code of the application (env $APP_SYSTEM_CODE) (default "concept-rw-neo4j")
      --app-name           Application name (env $APP_NAME) (default "Concept Rw Neo4j")
      --neo-url            neoURL must point to a leader node or to use neo4j:// scheme, otherwise writes will fail (env $NEO_URL) (default "bolt://localhost:7687")
      --port               Port to listen on (env $APP_PORT) (default 8080)
      --requestLoggingOn   Whether to log requests or not (env $REQUEST_LOGGING_ON) (default true)
      --logLevel           Level of logging to be shown (debug, info, warn, error) (env $LOG_LEVEL) (default "info")
      --dbDriverLogLevel   Db's driver logging level (debug, info, warn, error) (env $DB_DRIVER_LOG_LEVEL) (default "warn")
```

All arguments are optional, they default to a local Neo4j install on the default port (7474), application running on port 8080, batchSize of 1024.

## Testing

* Unit tests only: `go test -mod=readonly -race ./...`
* Unit and integration tests: 

    In order to execute the integration tests you must provide GITHUB_USERNAME and GITHUB_TOKEN values, because the service is depending on internal repositories.
    ```
    GITHUB_USERNAME=<username> GITHUB_TOKEN=<token> \
    docker-compose -f docker-compose-tests.yml up -d --build && \
    docker logs -f test-runner && \
    docker-compose -f docker-compose-tests.yml down -v
    ```

## Deployment

This deploys to our delivery cluster via the normal mechanism https://sites.google.com/a/ft.com/technology/systems/dynamic-semantic-publishing/coco/deploy-process

## Service Endpoints

Taxonomy refers to the concept type. Allowed concepts types (not complete list):
	alphaville-series
	genres
	locations
	memberships
	membership-roles
	people
	organisations
	sections
	topics

### PUT /{taxonomy}/{uuid}

The mandatory fields are the prefUUID, prefLabel, type, and sourceRepresentations. Inside each sourceRepresentation uuid, prefLabel, type, authority and authorityValue. 
Failure to provide mandatory fields will return 400 bad request.

Every request results in an attempt to update that concept

We run queries in batches. If a batch fails, all failing requests will get a 500 server error response.

Example PUT request:

    `curl -XPUT localhost:8080/sections/4c41f314-4548-4fb6-ac48-4618fcbfa84c \
         -H "X-Request-Id: 123" \
         -H "Content-Type: application/json" \
         -d '{
             	"prefUUID": "4c41f314-4548-4fb6-ac48-4618fcbfa84c",
             	"prefLabel": "Some pref label",
             	"type": "Section",
             	"sourceRepresentations": [{
             		"uuid": "4c41f314-4548-4fb6-ac48-4618fcbfa84c",
             		"prefLabel": "Some pref label",
             		"type": "Section",
             		"authority": "TME",
             		"authorityValue": "1234578fdh"
             	}]
             }'`

A successful PUT results in 200 and returns a json response of all updated uuids.

Example response from above request:

    `{
        "events": [
              {
                  "conceptType": "Section",
                  "conceptUUID": "4c41f314-4548-4fb6-ac48-4618fcbfa84c",
                  "eventDetails": {
                      "type": "Concept Updated"
                  }
              }
          ],
         "updatedIds": [
             "4c41f314-4548-4fb6-ac48-4618fcbfa84c"
         ]
     }`

"TME", "UPP" and "Smartlogic" are the only valid authorities, any other Authority will result in a 400 bad request response.

Invalid JSON body input or UUIDs that don't match between the path and the body will result in a 400 bad request response.

### GET /{taxonomy}/{uuid}
The internal read should return what got written 

If not found, you'll get a 404 response.

Empty fields are omitted from the response.
`curl -H "X-Request-Id: 123" localhost:8080/sections/3fa70485-3a57-3b9b-9449-774b001cd965`

### DELETE /{taxonomy}/{uuid}
Deletes a canonical concept and its concorded source concepts but only if they do not have any incoming relationships, e.g.
no content is annotated with any of the source concepts, no relationships to other concepts.

If not found, you'll get a 404 response. If the concept cannot be deleted, you'll get 400 with a response containing the reason.

`curl -XDELETE -H "X-Request-Id: 123" localhost:8080/sections/3fa70485-3a57-3b9b-9449-774b001cd965`

### Admin endpoints
Healthchecks: [http://localhost:8080/__health](http://localhost:8080/__health)
Good to Go: [http://localhost:8080/__gtg](http://localhost:8080/__gtg)
Build-Info: [http://localhost:8080/build-info](http://localhost:8080/build-info)

### Logging
This application uses logrus, the logfile is initialised in main.go and is configurable on runtime parameters

- logLevel run parameter can be set to debug for more detailed logging
- requestLoggingOn can be set to false to stop logging of http requests

