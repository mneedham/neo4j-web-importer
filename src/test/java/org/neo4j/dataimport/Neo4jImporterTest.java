package org.neo4j.dataimport;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.ws.rs.core.MediaType;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.jaxrs.JacksonJsonProvider;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import org.junit.Before;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.dataimport.DataCreator.*;

public class Neo4jImporterTest {
    @Before
    public void clearDb() {
        postCypherQuery(jerseyClient(), cypherQuery( "START n = node(*) MATCH n-[r?]-m DELETE m,r,n" ) );
    }

    @Test
    public void shouldImportTwoNodesWithLabelsAndARelationshipBetweenThem() {
        //given
        NodesParser nodesParser = mock(NodesParser.class);
        List<Map<String, Object>> nodes = new ArrayList<Map<String, Object>>();
        nodes.add(nodeWithLabel("1", "Mark", "Person"));
        nodes.add(nodeWithLabel("2", "Andreas", "Person"));
        nodes.add(nodeWithLabel("3", "Peter", "Person"));
        nodes.add(nodeWithLabel("4", "Michael", "Person"));
        nodes.add(nodeWithLabel("5", "Jim", "Person"));
        nodes.add(nodeWithLabel("6", "Thing", "Ting"));
        when(nodesParser.extractNodes()).thenReturn(nodes);

        RelationshipsParser relationshipsParser = mock(RelationshipsParser.class);
        List<Map<String, Object>> relationshipsProperties = new ArrayList<Map<String, Object>>();
        relationshipsProperties.add(relationship("1", "2", "FRIEND_OF"));
        relationshipsProperties.add(relationship("2", "3", "FRIEND_OF"));
        when( relationshipsParser.relationships()).thenReturn(relationshipsProperties);

        // when
        importer(jerseyClient(), nodesParser, relationshipsParser).run();

        // then
        String query = " START n = node(*)";
        query       += " MATCH n-[:FRIEND_OF]->p2";
        query       += " RETURN n.name, p2.name, LABELS(n), LABELS(p2)";

        JsonNode rows = postCypherQuery(jerseyClient(), cypherQuery( query ) ).getEntity(JsonNode.class).get("data");

        assertEquals(2, rows.size());
        assertEquals("[\"Mark\",\"Andreas\",[\"Person\"],[\"Person\"]]", rows.get(0).toString());
        assertEquals("[\"Andreas\",\"Peter\",[\"Person\"],[\"Person\"]]", rows.get(1).toString());
    }

    @Test
    public void shouldNotImportLabelAsAPropertyOnANode() {
        // given
        List<Map<String, Object>> nodes = new ArrayList<Map<String, Object>>();
        nodes.add(nodeWithLabel("1", "Mark", "Person"));
        NodesParser nodesParser = mock(NodesParser.class);
        when(nodesParser.extractNodes()).thenReturn(nodes);

        RelationshipsParser relationshipsParser = mock(RelationshipsParser.class);
        when( relationshipsParser.relationships()).thenReturn(new ArrayList<Map<String, Object>>());

        //when
        importer(jerseyClient(), nodesParser, relationshipsParser).run();

        // then
        String query = " START n = node(*) RETURN n.name, n.label";
        JsonNode rows = postCypherQuery(jerseyClient(), cypherQuery( query ) ).getEntity(JsonNode.class).get("data");

        assertEquals(1, rows.size());
        assertEquals("[\"Mark\",null]", rows.get(0).toString());
    }

    @Test
    public void shouldImportWhenOnlySomeNodesHaveLabels() {
        // given
        NodesParser nodesParser = mock(NodesParser.class);
        List<Map<String, Object>> nodes = new ArrayList<Map<String, Object>>();
        nodes.add(nodeWithLabel("1", "Mark", "Person"));
        nodes.add(nodeWithLabel("2", "OtherMark", ""));
        when(nodesParser.extractNodes()).thenReturn(nodes);

        RelationshipsParser relationshipsParser = mock(RelationshipsParser.class);
        List<Map<String, Object>> relationshipsProperties = new ArrayList<Map<String, Object>>();
        when( relationshipsParser.relationships()).thenReturn(relationshipsProperties);

        //when
        importer(jerseyClient(), nodesParser, relationshipsParser).run();

        // then
        String query = " START n = node(*) RETURN n.name, labels(n)";
        ClientResponse clientResponse = postCypherQuery(jerseyClient(), cypherQuery( query ) );
        JsonNode rows = clientResponse.getEntity(JsonNode.class).get("data");

        assertEquals(2, rows.size());
        assertEquals("[\"Mark\",[\"Person\"]]", rows.get(0).toString());
        assertEquals("[\"OtherMark\",[]]", rows.get(1).toString());
    }

    @Test
    public void shouldImportNodesWithoutLabel() {
        // given
        NodesParser nodesParser = mock(NodesParser.class);
        List<Map<String, Object>> nodes = new ArrayList<Map<String, Object>>();
        nodes.add(nodeWithoutLabel("1", "Mark"));
        when(nodesParser.extractNodes()).thenReturn(nodes);

        RelationshipsParser relationshipsParser = mock(RelationshipsParser.class);
        when( relationshipsParser.relationships()).thenReturn(new ArrayList<Map<String, Object>>());

        // when
        importer(jerseyClient(), nodesParser, relationshipsParser).run();

        // then
        String query = " START n = node(*) RETURN n.name";

        JsonNode rows = postCypherQuery(jerseyClient(), cypherQuery( query ) ).getEntity(JsonNode.class).get("data");

        assertEquals(1, rows.size());
        assertEquals("[\"Mark\"]", rows.get(0).toString());
    }

    private Neo4jImporter importer(Client client, NodesParser nodesParser, RelationshipsParser relationshipsParser) {
        return new Neo4jImporter(nodesParser, relationshipsParser, 1, "http://localhost:7474",
                new Neo4jTransactionalAPI(client, 1, 1, "http://localhost:7474"));
    }

    private ObjectNode cypherQuery( String query )
    {
        ObjectNode cypherQuery = JsonNodeFactory.instance.objectNode();
        cypherQuery.put("query", query);
        cypherQuery.put("params", JsonNodeFactory.instance.objectNode());
        return cypherQuery;
    }

    private ClientResponse postCypherQuery(Client client, ObjectNode cypherQuery) {
        return client.
                resource("http://localhost:7474/db/data/cypher").
                accept( MediaType.APPLICATION_JSON).
                entity(cypherQuery, MediaType.APPLICATION_JSON).
                post(ClientResponse.class);
    }

    private static Client jerseyClient() {
        DefaultClientConfig defaultClientConfig = new DefaultClientConfig();
        defaultClientConfig.getClasses().add( JacksonJsonProvider.class );
        return Client.create( defaultClientConfig );
    }

}