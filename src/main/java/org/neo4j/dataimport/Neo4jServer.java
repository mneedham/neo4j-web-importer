package org.neo4j.dataimport;


import com.googlecode.totallylazy.Callable1;
import com.googlecode.totallylazy.Pair;
import com.googlecode.totallylazy.Sequence;
import org.apache.commons.lang.StringUtils;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Neo4jServer
{
    private final GraphDatabaseService db;
    private final int batchSize;

    public Neo4jServer( GraphDatabaseService db, int batchSize )
    {
        this.db = db;
        this.batchSize = batchSize;
    }

    public Map<String, Long> importNodes( List<Map<String, Object>> nodes )
    {
        Map<String, Long> nodeMappings = new HashMap<String, Long>();

        Transaction tx = db.beginTx();

        for ( Map<String, Object> row : nodes )
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
