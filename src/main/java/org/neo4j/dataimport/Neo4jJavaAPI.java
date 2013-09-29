package org.neo4j.dataimport;


import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;

import com.googlecode.totallylazy.Callable1;
import com.googlecode.totallylazy.Group;
import com.googlecode.totallylazy.Sequence;

import org.neo4j.graphdb.*;

import static org.neo4j.dataimport.Functions.*;

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
    public Map<String, Long> importNodes(Sequence<Map<String, Object>> nodes)
    {
        Map<String, Long> nodeMappings = org.mapdb.DBMaker.newTempTreeMap();

        Sequence<Group<String, Map<String, Object>>> nodesByLabel = nodes.groupBy(label());

        for (Group<String, Map<String, Object>> labelAndNodes : nodesByLabel) {
            Transaction tx = db.beginTx();
            for ( Map<String, Object> row : labelAndNodes )
            {
                Node node = createNode(labelAndNodes.key());
                setPropertiesExcludingLabel(row, node);
                nodeMappings.put(row.get("id").toString(), node.getId());
            }

            tx.success();
            tx.finish();
        }


        return nodeMappings;
    }

    private Node createNode(Object label) {
        Node node;
        if(label.equals("")) {
            node = db.createNode();
        } else {
            node = db.createNode(DynamicLabel.label(label.toString()));
        }
        return node;
    }

    private void setPropertiesExcludingLabel(Map<String, Object> row, Node node) {
        for ( Map.Entry<String, Object> property : row.entrySet() )
        {
            if(!property.getKey().equals("label")){
                node.setProperty( property.getKey(), property.getValue() );
            }
        }
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
