package org.neo4j.dataimport;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.googlecode.totallylazy.Iterators;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;


public class RelationshipsParserTest {
    @Test
    public void shouldCreateCollectionOfRelationships() throws IOException
    {
        RelationshipsParser relationshipsParser = new RelationshipsParser(new File("src/main/resources/relationships.csv"));

        List<Map<String,Object>> expectedRelationships = new ArrayList<Map<String, Object>>();

        HashMap<String, Object> relationship = new HashMap<String, Object>();
        relationship.put("from", "1");
        relationship.put("to", "2");
        relationship.put("type", "FRIEND");
        relationship.put("timeInMonths", "3");

        expectedRelationships.add(relationship);

        assertEquals( expectedRelationships, Iterators.toList( relationshipsParser.relationships() ) );
    }

    @Test
    public void shouldBlowUpIfFromToOrTypeNotProvidedInHeaderLine() {
        RelationshipsParser relationshipsParser = new RelationshipsParser(new File("src/main/resources/nodes_no_header.csv"));

        try {
            relationshipsParser.checkFileExists();
            fail("Should have thrown Runtime Exception");
        } catch(Exception ex) {
            assertEquals("No header line found or 'from', 'to' or 'type' fields missing in relationships file", ex.getMessage());
        }
    }

    @Test
    public void shouldBlowUpIfFileDoesNotExist() {
        RelationshipsParser relationshipsParser = new RelationshipsParser(new File("src/main/resources/nofile.csv"));

        try {
            relationshipsParser.checkFileExists();
            fail("Should have thrown Runtime Exception");
        } catch(Exception ex) {
            assertEquals("Could not find relationships file", ex.getMessage());
        }
    }


}
