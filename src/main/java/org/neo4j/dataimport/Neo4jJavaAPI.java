package org.neo4j.dataimport;


import java.util.HashMap;
import java.util.Map;

import com.googlecode.totallylazy.Sequence;

import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;

public class Neo4jJavaAPI implements Neo4jServer
{
    private final GraphDatabaseService db;
    private final int batchSize;

    public Neo4jJavaAPI( GraphDatabaseService db, int batchSize )
    {
        this.db = db;
        this.batchSize = batchSize;
    }

    @Override
    public Map<String, Long> importNodes( NodesParser nodesParser )
    {
        Map<String, Long> nodeMappings = new HashMap<String, Long>();

        Transaction tx = db.beginTx();

        for ( Map<String, Object> row : nodesParser.extractNodes() )
        {
            Node node = db.createNode();

            for ( Map.Entry<String, Object> property : row.entrySet() )
            {
                node.setProperty( property.getKey(), property.getValue() );
            }

            nodeMappings.put(row.get("id").toString(), node.getId());
        }

        tx.success();
        tx.finish();

        return nodeMappings;
    }

    @Override
    public void importRelationships( Sequence<Map<String, Object>> relationships, Map<String, Long> nodeIdMappings )
    {
        // this will blow up with big rel sizes
        int numberOfRelationshipsToImport = relationships.size();

        for ( int i = 0; i < numberOfRelationshipsToImport; i += batchSize ) {
            Sequence<Map<String, Object>> batchRels = relationships.drop( i ).take( batchSize );

            Transaction tx = db.beginTx();

            for (Map<String, Object> properties : batchRels) {
                Node sourceNode = db.getNodeById(nodeIdMappings.get(properties.get("from").toString()));
                Node destinationNode = db.getNodeById(nodeIdMappings.get(properties.get("to").toString()));

                Relationship relationship = sourceNode.createRelationshipTo(destinationNode, DynamicRelationshipType.withName(properties.get("type").toString()));
                for (Map.Entry<String, Object> property : properties.entrySet()) {
                    relationship.setProperty(property.getKey(), property.getValue());
                }
            }

            tx.success();
            tx.finish();
        }
    }


}