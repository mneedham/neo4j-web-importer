package org.neo4j.dataimport;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.ws.rs.core.MediaType;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.jaxrs.JacksonJsonProvider;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import org.junit.Before;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class Neo4jImporterTest {
    @Before
    public void clearDb() {
        ObjectNode cypherQuery = JsonNodeFactory.instance.objectNode();
        cypherQuery.put("query", "START n = node(*) MATCH n-[r?]-m DELETE m,r,n");
        cypherQuery.put("params", JsonNodeFactory.instance.objectNode());
        postCypherQuery(jerseyClient(), cypherQuery);
    }

    @Test
    public void shouldImportTwoNodesWithLabelsAndARelationshipBetweenThem() {
        Client client = jerseyClient();

        NodesParser nodesParser = mock(NodesParser.class);
        RelationshipsParser relationshipsParser = mock(RelationshipsParser.class);

        ArrayNode createNodesParserParameters = JsonNodeFactory.instance.arrayNode();
        createNodesParserParameters.add(node("1", "Mark", "Person"));
        createNodesParserParameters.add(node("2", "Andreas", "Person"));
        createNodesParserParameters.add(node("3", "Peter", "Person"));
        createNodesParserParameters.add(node("4", "Michael", "Person"));
        createNodesParserParameters.add(node("5", "Jim", "Person"));
        createNodesParserParameters.add(node("6", "Thing", "Ting"));
        when(nodesParser.queryParameters()).thenReturn( createNodesParserParameters );

        List<Map<String, Object>> relationshipsProperties = new ArrayList<Map<String, Object>>();
        relationshipsProperties.add(relationship("1", "2", "FRIEND_OF"));
        relationshipsProperties.add(relationship("2", "3", "FRIEND_OF"));

        when( relationshipsParser.relationships()).thenReturn(relationshipsProperties);

        new Neo4jImporter(nodesParser, relationshipsParser, 1, "http://localhost:7474", new Neo4jTransactionalAPI( client, 1, 1, "http://localhost:7474" ) ).run();

        String query = " START n = node(*)";
        query       += " MATCH n-[:FRIEND_OF]->p2";
        query       += " RETURN n.name, p2.name, LABELS(n), LABELS(p2)";

        ObjectNode cypherQuery = JsonNodeFactory.instance.objectNode();
        cypherQuery.put("query", query);
        cypherQuery.put("params", JsonNodeFactory.instance.objectNode());

        ClientResponse clientResponse = postCypherQuery(client, cypherQuery);

        System.out.println( "clientResponse = " + clientResponse );

        JsonNode rows = clientResponse.getEntity(JsonNode.class).get("data");

        assertEquals(2, rows.size());
        assertEquals("[\"Mark\",\"Andreas\",[\"Person\"],[\"Person\"]]", rows.get(0).toString());
        assertEquals("[\"Andreas\",\"Peter\",[\"Person\"],[\"Person\"]]", rows.get(1).toString());
    }

    private ClientResponse postCypherQuery(Client client, ObjectNode cypherQuery) {
        return client.
                resource("http://localhost:7474/db/data/cypher").
                accept( MediaType.APPLICATION_JSON).
                entity(cypherQuery, MediaType.APPLICATION_JSON).
                post(ClientResponse.class);
    }

    private Map<String, Object> relationship(String from, String to, String type) {
        Map<String, Object> relationship = new HashMap<String, Object>();
        relationship.put("from", from);
        relationship.put("to", to);
        relationship.put("type", type);
        return relationship;
    }

    private ObjectNode node( String id, String name, String label ) {
        ObjectNode node = JsonNodeFactory.instance.objectNode();
        node.put("id", id);
        node.put("name", name);
        node.put("label", label);
        return node;
    }

    private static Client jerseyClient() {
        DefaultClientConfig defaultClientConfig = new DefaultClientConfig();
        defaultClientConfig.getClasses().add( JacksonJsonProvider.class );
        return Client.create( defaultClientConfig );
    }

}

