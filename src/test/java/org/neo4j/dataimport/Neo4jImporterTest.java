package org.neo4j.dataimport;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
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
import org.junit.Test;

import static com.googlecode.totallylazy.Sequences.sequence;
import static junit.framework.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.dataimport.DataCreator.*;

public class Neo4jImporterTest extends RestServerTestBase {
    private String hostPort = "http://localhost:" + PORT;

    @Test
    public void shouldImportTwoNodesWithLabelsAndARelationshipBetweenThem() throws IOException
    {
        //given
        NodesParser nodesParser = mock(NodesParser.class);
        List<Map<String, Object>> nodes = new ArrayList<Map<String, Object>>();
        nodes.add(nodeWithLabel("1", "Mark", "Person"));
        nodes.add(nodeWithLabel( "2", "Andreas", "Person" ));
        nodes.add(nodeWithLabel( "3", "Peter", "Person" ));
        nodes.add(nodeWithLabel( "4", "Michael", "Person" ));
        nodes.add(nodeWithLabel( "5", "Jim", "Person" ));
        nodes.add(nodeWithLabel( "6", "Thing", "Ting" ));
        when(nodesParser.extractNodes()).thenReturn( nodes );

        RelationshipsParser relationshipsParser = mock(RelationshipsParser.class);
        List<Map<String, Object>> relationshipsProperties = new ArrayList<Map<String, Object>>();
        relationshipsProperties.add(relationship("1", "2", "FRIEND_OF"));
        relationshipsProperties.add(relationship("2", "3", "FRIEND_OF"));
        when( relationshipsParser.relationships()).thenReturn( relationshipsProperties.iterator());

        // when
        importer(jerseyClient(), nodesParser, relationshipsParser ).run();

        // then
        String query = " START n = node(*)";
        query       += " MATCH n-[:FRIEND_OF]->p2";
        query       += " RETURN n.name, p2.name, LABELS(n), LABELS(p2)";

        JsonNode rows = postCypherQuery(jerseyClient(), cypherQuery( query ) ).getEntity( JsonNode.class ).get("data");

        assertEquals(2, rows.size());
        assertEquals("[\"Mark\",\"Andreas\",[\"Person\"],[\"Person\"]]", rows.get(0).toString());
        assertEquals("[\"Andreas\",\"Peter\",[\"Person\"],[\"Person\"]]", rows.get(1).toString());
    }

    @Test
    public void shouldNotImportLabelAsAPropertyOnANode() throws IOException
    {
        // given
        List<Map<String, Object>> nodes = new ArrayList<Map<String, Object>>();
        nodes.add( nodeWithLabel( "1", "Mark", "Person" ) );
        NodesParser nodesParser = mock(NodesParser.class);
        when(nodesParser.extractNodes()).thenReturn(nodes);

        RelationshipsParser relationshipsParser = mock(RelationshipsParser.class);
        when( relationshipsParser.relationships()).thenReturn( emptyIterator() );

        //when
        importer(jerseyClient(), nodesParser, relationshipsParser ).run();

        // then
        String query = " START n = node(*) RETURN n.name, n.label";
        JsonNode rows = postCypherQuery(jerseyClient(), cypherQuery( query ) ).getEntity(JsonNode.class).get("data");

        assertEquals(1, rows.size());
        assertEquals("[\"Mark\",null]", rows.get(0).toString());
    }

    private Iterator<Map<String, Object>> emptyIterator()
    {
        return Collections.<Map<String, Object>>emptyList().iterator();
    }

    @Test
    public void shouldImportWhenOnlySomeNodesHaveLabels() throws IOException
    {
        // given
        NodesParser nodesParser = mock(NodesParser.class);
        List<Map<String, Object>> nodes = new ArrayList<Map<String, Object>>();
        nodes.add(nodeWithLabel("1", "Mark", "Person"));
        nodes.add(nodeWithLabel( "2", "OtherMark", "" ));
        when(nodesParser.extractNodes()).thenReturn(nodes);

        RelationshipsParser relationshipsParser = mock(RelationshipsParser.class);
        List<Map<String, Object>> relationshipsProperties = new ArrayList<Map<String, Object>>();
        when( relationshipsParser.relationships()).thenReturn(relationshipsProperties.iterator());

        //when
        importer(jerseyClient(), nodesParser, relationshipsParser ).run();

        // then
        String query = " START n = node(*) RETURN n.name, labels(n)";
        ClientResponse clientResponse = postCypherQuery( jerseyClient(), cypherQuery( query ) );
        JsonNode rows = clientResponse.getEntity(JsonNode.class).get("data");

        assertEquals(2, rows.size());
        assertEquals("[\"Mark\",[\"Person\"]]", rows.get(0).toString());
        assertEquals("[\"OtherMark\",[]]", rows.get(1).toString());
    }

    @Test
    public void shouldImportNodesWithoutLabel() throws IOException
    {
        // given
        NodesParser nodesParser = mock(NodesParser.class);
        List<Map<String, Object>> nodes = new ArrayList<Map<String, Object>>();
        nodes.add(nodeWithoutLabel("1", "Mark"));
        when(nodesParser.extractNodes()).thenReturn(nodes);

        RelationshipsParser relationshipsParser = mock(RelationshipsParser.class);
        when( relationshipsParser.relationships()).thenReturn(emptyIterator());

        // when
        importer(jerseyClient(), nodesParser, relationshipsParser ).run();

        // then
        String query = " START n = node(*) RETURN n.name";

        JsonNode rows = postCypherQuery(jerseyClient(), cypherQuery( query ) ).getEntity(JsonNode.class).get("data");

        assertEquals(1, rows.size());
        assertEquals("[\"Mark\"]", rows.get(0).toString());
    }

    private Neo4jImporter importer( Client client, NodesParser nodesParser, RelationshipsParser relationshipsParser ) {
        return new Neo4jImporter(nodesParser, relationshipsParser,
                new Neo4jTransactionalAPI(client, 1, hostPort, 1));
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
                resource(hostPort + "/db/data/cypher").
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
