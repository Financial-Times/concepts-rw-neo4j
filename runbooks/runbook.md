# UPP - Concepts R/W Neo4j

Writes the UPP view of concepts into Neo4j

## Code

concepts-rw-neo4j

## Primary URL

<https://upp-prod-delivery-glb.upp.ft.com/__concepts-rw-neo4j/>

## Service Tier

Bronze

## Lifecycle Stage

Production

## Delivered By

content

## Supported By

content

## Known About By

- dimitar.terziev
- elitsa.pavlova
- ivan.nikolov
- kalin.arsov
- miroslav.gatsanoga
- marina.chompalova

## Host Platform

AWS

## Architecture

The service receives PUT HTTP requests containing the JSON representation of a concept from the Aggregate Concept Transformer service and writes that aggregated concept (including concordances and other concept to concept relationships) into Neo4j.

## Contains Personal Data

No

## Contains Sensitive Data

No

## Dependencies

- upp-neo4j-cluster

## Failover Architecture Type

ActiveActive

## Failover Process Type

FullyAutomated

## Failback Process Type

FullyAutomated

## Failover Details

The service is deployed in both Delivery clusters. The failover guide for the cluster is located here:
<https://github.com/Financial-Times/upp-docs/tree/master/failover-guides/delivery-cluster>

## Data Recovery Process Type

NotApplicable

## Data Recovery Details

The service does not store data, so it does not require any data recovery steps.

## Release Process Type

PartiallyAutomated

## Rollback Process Type

Manual

## Release Details

If the new release does not change the way messages are consumed or saved it is safe to deploy it without cluster failover.

## Key Management Process Type

Manual

## Key Management Details

To access the service clients need to provide basic auth credentials.
To rotate credentials you need to login to a particular cluster and update varnish-auth secrets.

## Monitoring

Service in the UPP K8s delivery clusters:

- Delivery-Prod-EU health: <https://upp-prod-delivery-eu.upp.ft.com/__health/__pods-health?service-name=concepts-rw-neo4j>
- Delivery-Prod-US health: <https://upp-prod-delivery-us.upp.ft.com/__health/__pods-health?service-name=concepts-rw-neo4j>

## First Line Troubleshooting

<https://github.com/Financial-Times/upp-docs/tree/master/guides/ops/first-line-troubleshooting>

## Second Line Troubleshooting

Please refer to the GitHub repository README for troubleshooting information.
