# Neo4j importer & web importer

Despite the name this repository now contains code for a console based import tool as well as a web based one.

They both import data from node and relationship files which can be either tab or comma delimited.

## Building the importer JAR

You can build the importer JAR by using the following command:

    mvn clean compile assembly:single

That creates a JAR under target/ which is used by the console based import tool.

## Using the console based import tool

Run the following command:

    ./neo4j-loader

Its default input is two files named 'nodes.csv' and 'relationships.csv' in the current directory which contain nodes and relationships to import. Nodes are referenced in both files using a natural key.

e.g.

nodes.csv	

	id   									name    age works_on
	b8bd1c77-2732-4687-96b3-fa2c9f25e303    Michael 37  neo4j
	ac80bc1f-d8e8-40f0-9b53-af731c635796    Selina  14
	

relationships.csv

	from 									to 										type    since   counter:int
	b8bd1c77-2732-4687-96b3-fa2c9f25e303    ac80bc1f-d8e8-40f0-9b53-af731c635796    FATHER_OF   1998-07-10  1

## Using the web based import tool

Bit esoteric at the moment as it involves changing the neo4j static JAR. Working out how it can be called from within [Neo4j browser](https://github.com/neo4j/neo4j-browser)
