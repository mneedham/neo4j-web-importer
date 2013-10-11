package org.neo4j.dataimport;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.ws.rs.core.MediaType;

import com.googlecode.totallylazy.Callable1;
import com.googlecode.totallylazy.Group;
import com.googlecode.totallylazy.Pair;
import com.googlecode.totallylazy.Sequence;
import com.googlecode.totallylazy.Sequences;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import org.apache.commons.lang.StringUtils;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;

import static com.googlecode.totallylazy.numbers.Numbers.range;

public class Neo4jTransactionalAPI implements Neo4jServer
{
    private static final String NODE_LOOKUP = "node%s = node({%s}), node%s=node({%s})";
    private static final String CREATE_RELATIONSHIP = " CREATE %s-[:%s]->%s";
    private static final String CREATE_NODE = "CREATE (node {properties}) SET node :%s RETURN node.id, " +
            "ID(node) AS nodeId";
    private static final String CREATE_NODE_WITHOUT_LABEL = "CREATE (node {properties}) RETURN node.id, " +
            "ID(node) AS nodeId";
    private static final String CYPHER_URI = "%s/db/data/cypher";
    private static final String TRANSACTIONAL_URI = "%s/db/data/transaction/commit";

    private Client client;
    private int batchSize;
    private int batchWithinBatchSize;

    private List<Long> building = new ArrayList<Long>();
    private List<Long> querying = new ArrayList<Long>();
    private final String cypherUri;
    private final String transactionalUri;

    public Neo4jTransactionalAPI( Client client, int batchSize, int batchWithinBatchSize, String neo4jServerLocation )
    {
        this.batchSize = batchSize;
        this.batchWithinBatchSize = batchWithinBatchSize;
        this.client = client;

        cypherUri = String.format( CYPHER_URI, neo4jServerLocation );
        transactionalUri = String.format( TRANSACTIONAL_URI, neo4jServerLocation );
    }

    public Map<String, Long> importNodes( Sequence<Map<String, Object>> nodes )
    {
        int nodeBatchSize = 10000;
        System.out.println( "Importing nodes in batches of " + nodeBatchSize );
        Map<String, Long> nodeMappings = org.mapdb.DBMaker.newTempTreeMap();
        Sequence<Group<String, Map<String, Object>>> nodesByLabel = nodes.groupBy( Functions.label() );

        for ( Group<String, Map<String, Object>> labelAndNodes : nodesByLabel )
        {
            int size = labelAndNodes.size();
            for ( int i = 0; i < size; i += nodeBatchSize )
            {
                ObjectNode cypherQuery = JsonNodeFactory.instance.objectNode();

                if ( labelAndNodes.key().equals( "" ) )
                {
                    cypherQuery.put( "query", CREATE_NODE_WITHOUT_LABEL );
                }
                else
                {
                    cypherQuery.put( "query", String.format( CREATE_NODE, labelAndNodes.key() ) );
                }

                cypherQuery.put( "params", createProperties( createParams( labelAndNodes.drop(i).take(nodeBatchSize) ) ) );

                ClientResponse clientResponse = client.resource( cypherUri ).
                        accept( MediaType.APPLICATION_JSON ).
                        entity( cypherQuery, MediaType.APPLICATION_JSON ).
                        post( ClientResponse.class );

                for ( JsonNode mappingAsJsonNode : clientResponse.getEntity( JsonNode.class ).get( "data" ) )
                {
                    ArrayNode mapping = (ArrayNode) mappingAsJsonNode;
                    nodeMappings.put( mapping.get( 0 ).asText(), mapping.get( 1 ).asLong() );
                }
                System.out.print( "." );
            }


        }
        System.out.println( );
        System.out.println( "Total nodes imported: " + nodeMappings.size() );

        return nodeMappings;
    }

    private ObjectNode createProperties( ArrayNode params )
    {
        ObjectNode properties = JsonNodeFactory.instance.objectNode();
        properties.put( "properties", params );
        return properties;
    }

    private ArrayNode createParams( Sequence<Map<String, Object>> labelAndNodes )
    {
        ArrayNode params = JsonNodeFactory.instance.arrayNode();
        for ( Map<String, Object> row : labelAndNodes )
        {
            row.remove( "label" );

            ObjectNode jsonRow = JsonNodeFactory.instance.objectNode();
            for ( Map.Entry<String, Object> property : row.entrySet() )
            {
                jsonRow.put( property.getKey(), property.getValue().toString() );
            }

            params.add( jsonRow );
        }
        return params;
    }


    public void importRelationships( Sequence<Map<String, Object>> relationships, Map<String, Long> nodeMappings )
    {
        System.out.println( "Importing relationships in batches of " + batchSize );
        int numberOfRelationshipsImported  = 0;

        Sequence<Map<String, Object>> batchOfRelationships;
        while(!(batchOfRelationships = relationships.take(batchSize)).isEmpty()) {
            long startOfBatch = System.currentTimeMillis();
            long beforeBuildingQuery = System.currentTimeMillis();
            ObjectNode query = JsonNodeFactory.instance.objectNode();
            ArrayNode statements = JsonNodeFactory.instance.arrayNode();
            for ( int j = 0; j < batchSize; j += batchWithinBatchSize )
            {
                long beforeBatch = System.currentTimeMillis();
                final Sequence<Map<String, Object>> relationshipsBatch = batchOfRelationships.drop( j ).take(
                        batchWithinBatchSize );

                ObjectNode statement = createStatement( relationshipsBatch, nodeMappings );
                System.out.println("creating statement: " + (System.currentTimeMillis() - beforeBatch));
                statements.add( statement );
            }

            query.put( "statements", statements );
            building.add( System.currentTimeMillis() - beforeBuildingQuery );
            System.out.println("building: " + (System.currentTimeMillis() - beforeBuildingQuery));

            long beforePosting = System.currentTimeMillis();

            client.resource( transactionalUri ).
                    accept( MediaType.APPLICATION_JSON ).
                    entity( query, MediaType.APPLICATION_JSON ).
                    header( "X-Stream", true ).
                    post( ClientResponse.class );
            querying.add( System.currentTimeMillis() - beforePosting );
            System.out.println("querying: " + (System.currentTimeMillis() - beforePosting));

            System.out.print( "." );
            System.out.println(System.currentTimeMillis() - startOfBatch);

            numberOfRelationshipsImported += batchSize;
            relationships = relationships.drop(batchSize);
        }

        System.out.println();
        System.out.println( "Total relationships imported: " + numberOfRelationshipsImported );
    }


    private ObjectNode createStatement( Sequence<Map<String, Object>> relationships, Map<String, Long> nodeIdMappings )
    {
        long beforeStatement = System.currentTimeMillis();
        int numberOfNodes = batchSize * 2;
        Sequence<Pair<Number, Number>> nodePairs = range( 1, numberOfNodes - 1, 2 ).zip( range( 2, numberOfNodes, 2 ) );
        System.out.println("nodePairs: " + (System.currentTimeMillis() - beforeStatement));

        long beforeQuery = System.currentTimeMillis();
        ObjectNode cypherQuery = JsonNodeFactory.instance.objectNode();
        String query = createQuery( relationships, nodePairs );
        cypherQuery.put( "statement", query );
        System.out.println("createQuery: " + (System.currentTimeMillis() - beforeQuery));

        long beforeParams = System.currentTimeMillis();
        cypherQuery.put( "parameters", createParametersFrom( nodeParameterMappings( nodePairs.zip( relationships ),
                nodeIdMappings ) ) );
        System.out.println("params: " + (System.currentTimeMillis() - beforeParams));

        return cypherQuery;
    }

    private String createQuery( Sequence<Map<String, Object>> relationships, Sequence<Pair<Number, Number>> nodePairs )
    {
        String query = "START ";
        long beforePairs = System.currentTimeMillis();
        query += StringUtils.join( nodePairs.zip( relationships ).map( nodeLookup() ).iterator(), ", " );
        System.out.println( "relationships = " + relationships );
        System.out.println( "nodePairs = " + nodePairs );
        System.out.println("pairs: " + (System.currentTimeMillis() - beforePairs));

        long beforeRels = System.currentTimeMillis();
        query += StringUtils.join( relationships.zip( nodePairs ).map( createRelationship() ).iterator(), " " );
        System.out.println("rels: " + (System.currentTimeMillis() - beforeRels));

        return query;
    }

    private ObjectNode createParametersFrom( List<Pair<Number, Number>> relationshipMappings )
    {
        ObjectNode params = JsonNodeFactory.instance.objectNode();

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
