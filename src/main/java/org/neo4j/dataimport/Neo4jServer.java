package org.neo4j.dataimport;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.core.MediaType;

import com.googlecode.totallylazy.Callable1;
import com.googlecode.totallylazy.Pair;
import com.googlecode.totallylazy.Sequence;
import com.sun.jersey.api.client.ClientResponse;
import org.apache.commons.lang.StringUtils;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.*;

import static com.googlecode.totallylazy.numbers.Numbers.range;

public class Neo4jServer
{
    private static final String NODE_LOOKUP = "node%s = node({%s}), node%s=node({%s})";
    private static final String CREATE_RELATIONSHIP = " CREATE %s-[:%s]->%s";

    private final GraphDatabaseService db;
    private final int batchSize;
    private int batchWithinBatchSize;
    private ExecutionEngine executionEngine;

    public Neo4jServer( GraphDatabaseService db, int batchSize )
    {
        this.db = db;
        this.batchSize = batchSize;
        this.batchWithinBatchSize = 50;
        this.executionEngine = new ExecutionEngine( db );
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
        int numberOfRelationshipsToImport = relationships.size();

        for ( int i = 0; i < numberOfRelationshipsToImport; i += batchSize ) {
            Sequence<Map<String, Object>> batchRels = relationships.drop( i ).take( batchSize );
//            int numberOfNodes = batchSize * 2;
//            Sequence<Pair<Number, Number>> nodePairs = range( 1, numberOfNodes - 1, 2 ).zip( range( 2, numberOfNodes, 2 ) );

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

//            String query = "START ";
//            query += StringUtils.join( nodePairs.zip( relationships ).map( nodeLookup() ).iterator(), ", " );
//            query += StringUtils.join( relationships.zip( nodePairs ).map( createRelationship() ).iterator(), " " );
//
//            Map<String, Object> parameters = createParametersFrom( nodeParameterMappings( nodePairs.zip( batchRels ), nodeIdMappings ) );
//
//            executionEngine.execute( query, parameters );
        }
    }

    private String createQuery( Sequence<Map<String, Object>> relationships, Sequence<Pair<Number, Number>> nodePairs )
    {
        String query = "START ";
        query += StringUtils.join( nodePairs.zip( relationships ).map( nodeLookup() ).iterator(), ", " );
        query += StringUtils.join( relationships.zip( nodePairs ).map( createRelationship() ).iterator(), " " );
        return query;
    }

    private Map<String, Object> createParametersFrom( List<Pair<Number, Number>> relationshipMappings )
    {
        Map<String, Object> params = new HashMap<String, Object>();

        for ( Pair<Number, Number> relationshipMapping : relationshipMappings )
        {
            params.put( relationshipMapping.first().toString(), relationshipMapping.second().longValue() );
        }
        return params;
    }


    private List<Pair<Number, Number>> nodeParameterMappings( final Sequence<Pair<Pair<Number, Number>, Map<String,
            Object>>> relationshipMappings, Map<String,
            Long> nodeIdMappings )
    {
        List<Pair<Number, Number>> pairs = new ArrayList<Pair<Number, Number>>();
        for ( Pair<Pair<Number, Number>, Map<String, Object>> sequenceIdsToRelationship : relationshipMappings )
        {
            final Map<String, Object> relationship = sequenceIdsToRelationship.second();
            Pair<Number, Number> sequenceIds = sequenceIdsToRelationship.first();

            Long from = nodeIdMappings.get( relationship.get( "from" ).toString() );
            Long to = nodeIdMappings.get( relationship.get( "to" ).toString() );

            pairs.add( Pair.<Number, Number>pair( sequenceIds.first(), from ) );
            pairs.add( Pair.<Number, Number>pair( sequenceIds.second(), to ) );
        }
        return pairs;
    }

    private Callable1<? super Pair<Map<String, Object>, Pair<Number, Number>>, ?> createRelationship()
    {
        return new Callable1<Pair<Map<String, Object>, Pair<Number, Number>>, Object>()
        {
            public Object call( Pair<Map<String, Object>, Pair<Number, Number>> mapPairPair ) throws Exception
            {
                String sourceNode = "node" + mapPairPair.second().first();
                String destinationNode = "node" + mapPairPair.second().second();
                String relationshipType = mapPairPair.first().get( "type" ).toString();
                return String.format( CREATE_RELATIONSHIP, sourceNode, relationshipType, destinationNode );
            }
        };
    }

    private Callable1<Pair<Pair<Number, Number>, Map<String, Object>>, Object> nodeLookup()
    {
        return new Callable1<Pair<Pair<Number, Number>, Map<String, Object>>, Object>()
        {
            public Object call( final Pair<Pair<Number, Number>, Map<String, Object>> pairMapPair ) throws Exception
            {
                return String.format( NODE_LOOKUP,
                        pairMapPair.first().first(),
                        pairMapPair.first().first(),
                        pairMapPair.first().second(),
                        pairMapPair.first().second() );
            }
        };
    }

}
