package org.neo4j.dataimport;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.ws.rs.core.MediaType;

import com.googlecode.totallylazy.Callable1;
import com.googlecode.totallylazy.Group;
import com.googlecode.totallylazy.Pair;
import com.googlecode.totallylazy.Sequence;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import org.apache.commons.lang.StringUtils;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;

import static com.googlecode.totallylazy.Sequences.sequence;
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
    private int nodeBatchSize;

    public Neo4jTransactionalAPI( Client client, int batchSize, int batchWithinBatchSize, String neo4jServerLocation, int nodeBatchSize )
    {
        this. nodeBatchSize = nodeBatchSize;
        this.batchSize = batchSize;
        this.batchWithinBatchSize = batchWithinBatchSize;
        this.client = client;

        cypherUri = String.format( CYPHER_URI, neo4jServerLocation );
        transactionalUri = String.format( TRANSACTIONAL_URI, neo4jServerLocation );
    }

    public Map<String, Long> importNodes( Sequence<Map<String, Object>> nodes )
    {

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

                cypherQuery.put( "params", createProperties( createParams( labelAndNodes.drop(i).take( nodeBatchSize ) ) ) );

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


    @Override
    public void importRelationships( Iterator<Map<String, Object>> relationships, Map<String, Long> nodeIdMappings )
    {
        System.out.println( "Importing relationships in batches of " + batchSize );

        ObjectNode root = JsonNodeFactory.instance.objectNode();
        ArrayNode statements = JsonNodeFactory.instance.arrayNode();
        root.put( "statements", statements );

        int numberProcessed = 0;
        while(relationships.hasNext()) {
            numberProcessed ++;
            Map<String, Object> relationship = relationships.next();

            ObjectNode statement = JsonNodeFactory.instance.objectNode();
            statement.put( "statement", buildQuery( relationship ) );
            statement.put( "parameters", buildParameters( nodeIdMappings, relationship ) );

            statements.add( statement );

            if(numberProcessed % batchSize == 0) {
                postTransaction( root, statements );
            }
        }

        postTransaction( root, statements );

        System.out.println();

    }

    private void postTransaction( ObjectNode root, ArrayNode statements )
    {
        client.resource( transactionalUri ).
                accept( MediaType.APPLICATION_JSON ).
                entity( root, MediaType.APPLICATION_JSON ).
                header( "X-Stream", true ).
                post( ClientResponse.class );

        statements.removeAll();
        System.out.print( "." );
    }

    private String buildQuery( Map<String, Object> relationship )
    {
        return String.format( "START node1 = node({1}), node2 = node({2}) CREATE node1-[:%s " +
                "{relationshipProperties}]->node2",
                relationship.get( "type" ) );
    }

    private ObjectNode buildParameters( Map<String, Long> nodeIdMappings, Map<String, Object> relationship )
    {
        ObjectNode parameters = JsonNodeFactory.instance.objectNode();
        parameters.put( "1", nodeIdMappings.get( relationship.get( "from" ) ) );
        parameters.put( "2", nodeIdMappings.get( relationship.get( "to" ) ) );


        ObjectNode relationshipProperties = JsonNodeFactory.instance.objectNode();

        for ( Map.Entry<String, Object> property : relationship.entrySet() )
        {
            String key = property.getKey();
            if( key.equals( "from" ) || key.equals( "to" ) || key.equals( "type" ))  continue;

            relationshipProperties.put( key, property.getValue().toString());
        }

        parameters.put( "relationshipProperties", relationshipProperties );
        return parameters;
    }


    private ObjectNode createStatement( List<Map<String, Object>> relationships, Map<String, Long> nodeIdMappings )
    {
        int numberOfNodes = batchSize * 2;
        Sequence<Pair<Number, Number>> nodePairs = range( 1, numberOfNodes - 1, 2 ).zip( range( 2, numberOfNodes, 2 ) );

        ObjectNode cypherQuery = JsonNodeFactory.instance.objectNode();
        String query = createQuery( relationships, nodePairs );
        cypherQuery.put( "statement", query );

        cypherQuery.put( "parameters", createParametersFrom( nodeParameterMappings( nodePairs.zip( relationships ),
                nodeIdMappings ) ) );

        return cypherQuery;
    }

    private String createQuery( List<Map<String, Object>> relationships, Sequence<Pair<Number, Number>> nodePairs )
    {
        String query = "START ";
        query += StringUtils.join( nodePairs.zip( relationships ).map( nodeLookup() ).iterator(), ", " );
        query += StringUtils.join( sequence(relationships).zip( nodePairs ).map( createRelationship() ).iterator(), " " );
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
