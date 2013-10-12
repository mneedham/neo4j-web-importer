package org.neo4j.dataimport;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.googlecode.totallylazy.Iterators;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.hasItem;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class NodesParserTest
{
    @Test
    public void shouldCreateCypherQueryParametersFromFile() throws IOException
    {
        NodesParser nodes = new NodesParser(new File("src/main/resources/nodes.csv"));

        List<Map<String, Object>> actualParameters = Iterators.toList( nodes.extractNodes() );

        assertThat(actualParameters, hasItem(createNode( "1", "Mark" )));
        assertThat(actualParameters, hasItem(createNode( "2", "Andreas" )));
    }

    @Test
    public void shouldBlowUpIfIdNotProvidedInHeaderLine() {
        NodesParser nodes = new NodesParser(new File("src/main/resources/nodes_no_header.csv"));

        try {
            nodes.checkFileExists();
            fail("Should have thrown Runtime Exception");
        } catch(Exception ex) {
            assertEquals("No header line found or 'id' field missing in nodes file", ex.getMessage());
        }
    }

    private Map<String, Object> createNode(String id, String name) {
        Map<String, Object> node = new HashMap<String, Object>();
        node.put("id", id);
        node.put("name", name);
        return node;
    }
}
