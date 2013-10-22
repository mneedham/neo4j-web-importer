package org.neo4j.dataimport;

import java.util.Iterator;
import java.util.Map;
import javax.ws.rs.core.MediaType;

import com.googlecode.totallylazy.Group;
import com.googlecode.totallylazy.Sequence;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;

import static com.googlecode.totallylazy.Sequences.sequence;
import static com.googlecode.totallylazy.numbers.Numbers.range;

public class Neo4jTransactionalAPI implements Neo4jServer
{
    private static final String CREATE_NODE = "CREATE (node {properties}) SET node :%s RETURN node.id, " +
            "ID(node) AS nodeId";
    private static final String CREATE_NODE_WITHOUT_LABEL = "CREATE (node {properties}) RETURN node.id, " +
            "ID(node) AS nodeId";
    private String CREATE_RELATIONSHIP = "START node1 = node({1}), node2 = node({2}) CREATE node1-[:%s " +
            "{relationshipProperties}]->node2";

    private static final String CYPHER_URI = "%s/db/data/cypher";
    private static final String TRANSACTIONAL_URI = "%s/db/data/transaction/commit";

    private Client client;
    private int batchSize;

    private final String cypherUri;
    private final String transactionalUri;
    private int nodeBatchSize;


    public Neo4jTransactionalAPI( Client client, int batchSize, String neo4jServerLocation, int nodeBatchSize )
    {
        this.nodeBatchSize = nodeBatchSize;
        this.batchSize = batchSize;
        this.client = client;

        cypherUri = String.format( CYPHER_URI, neo4jServerLocation );
        transactionalUri = String.format( TRANSACTIONAL_URI, neo4jServerLocation );
    }

    public Map<String, Long> importNodes( Iterator<Map<String, Object>> nodes )
    {
        System.out.println( "Importing nodes in batches of " + nodeBatchSize );

        ObjectNode root = JsonNodeFactory.instance.objectNode();
        ArrayNode statements = JsonNodeFactory.instance.arrayNode();
        root.put( "statements", statements );

        Map<String, Long> nodeMappings = org.mapdb.DBMaker.newTempTreeMap();
        int numberProcessed = 0;
        while ( nodes.hasNext() )
        {
            numberProcessed++;
            Map<String, Object> node = nodes.next();

            ObjectNode statement = JsonNodeFactory.instance.objectNode();
            statement.put( "statement", buildNodeQuery( node ) );

            ObjectNode parameters = JsonNodeFactory.instance.objectNode();
            parameters.put("properties", buildNodeParameters( node ));

            statement.put( "parameters",  parameters);

            statements.add( statement );

            if ( numberProcessed % nodeBatchSize == 0 )
            {
                ClientResponse clientResponse = postTransaction( root, statements );
                collectNodeMappings( nodeMappings, clientResponse );
            }
        }

        ClientResponse clientResponse = postTransaction( root, statements );
        collectNodeMappings( nodeMappings, clientResponse );

        System.out.println();


        return nodeMappings;
    }

    private void collectNodeMappings( Map<String, Long> nodeMappings, ClientResponse clientResponse )
    {
        ArrayNode data = (ArrayNode) clientResponse.getEntity( JsonNode.class ).get( "results" );
        for ( JsonNode mappingAsJsonNode : data )
        {
            ArrayNode mapping = (ArrayNode) mappingAsJsonNode.get("data").get(0).get("row");
            nodeMappings.put( mapping.get( 0 ).asText(), mapping.get( 1 ).asLong() );
        }
    }

    private JsonNode buildNodeParameters( Map<String, Object> properties )
    {
        ObjectNode jsonRow = JsonNodeFactory.instance.objectNode();
        for ( Map.Entry<String, Object> row : properties.entrySet() )
        {
            if ( row.getKey().equals( "label" ) )
            {
                continue;
            }
            jsonRow.put( row.getKey(), row.getValue().toString() );
        }
        return jsonRow;
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
        while ( relationships.hasNext() )
        {
            numberProcessed++;
            Map<String, Object> relationship = relationships.next();

            ObjectNode statement = JsonNodeFactory.instance.objectNode();
            statement.put( "statement", buildRelationshipQuery( relationship ) );
            statement.put( "parameters", buildRelationshipParameters( nodeIdMappings, relationship ) );

            statements.add( statement );


            if ( numberProcessed % batchSize == 0 )
            {
                postTransaction( root, statements );
            }
        }

        postTransaction( root, statements );

        System.out.println();

    }

    private ClientResponse postTransaction( ObjectNode root, ArrayNode statements )
    {
        ClientResponse response = client.resource( transactionalUri ).
                accept( MediaType.APPLICATION_JSON ).
                entity( root, MediaType.APPLICATION_JSON ).
                header( "X-Stream", true ).
                post( ClientResponse.class );

        statements.removeAll();
        System.out.print( "." );
        return response;
    }

    private String buildRelationshipQuery( Map<String, Object> relationship )
    {
        return String.format( CREATE_RELATIONSHIP, relationship.get( "type" ) );
    }

    private String buildNodeQuery( Map<String, Object> properties )
    {
        if ( properties.get( "label" ) == null || properties.get( "label" ).toString().equals( "" ) )
        {
            return CREATE_NODE_WITHOUT_LABEL;
        }

        return String.format( CREATE_NODE, properties.get( "label" ).toString() );
    }

    private ObjectNode buildRelationshipParameters( Map<String, Long> nodeIdMappings, Map<String, Object> relationship )
    {
        ObjectNode parameters = JsonNodeFactory.instance.objectNode();
        parameters.put( "1", nodeIdMappings.get( relationship.get( "from" ) ) );
        parameters.put( "2", nodeIdMappings.get( relationship.get( "to" ) ) );


        ObjectNode relationshipProperties = JsonNodeFactory.instance.objectNode();

        for ( Map.Entry<String, Object> property : relationship.entrySet() )
        {
            String key = property.getKey();
            if ( key.equals( "from" ) || key.equals( "to" ) || key.equals( "type" ) )
            {
                continue;
            }

            relationshipProperties.put( key, property.getValue().toString() );
        }

        parameters.put( "relationshipProperties", relationshipProperties );
        return parameters;
    }


}
