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
}
