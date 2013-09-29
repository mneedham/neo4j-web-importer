package org.neo4j.dataimport;


import com.googlecode.totallylazy.Sequence;
import com.sun.jersey.api.client.Client;
import org.codehaus.jackson.JsonNode;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.test.ImpermanentGraphDatabase;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.googlecode.totallylazy.Sequences.sequence;
import static junit.framework.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.dataimport.DataCreator.*;
import static org.neo4j.test.mocking.GraphMock.node;

public class Neo4jJavaAPITest {

    private ImpermanentGraphDatabase db;

    @Before
    public void clearDb() {
        db = new ImpermanentGraphDatabase();
        new ExecutionEngine(db).execute("START n = node(*) MATCH n-[r?]-m DELETE m,r,n");
    }

    @Test
    public void shouldImportNodesWithLabels() {
        List<Map<String, Object>> nodes = new ArrayList<Map<String, Object>>();
        nodes.add(nodeWithLabel("1", "Mark", "Person"));
        nodes.add(nodeWithLabel("2", "Andreas", ""));

        new Neo4jJavaAPI(db, 1).importNodes(sequence(nodes));

        String query = " START n = node(*)";
        query       += " RETURN n.name, LABELS(n), n.label";


        ExecutionResult result = new ExecutionEngine(db).execute(query);

        Sequence<Map<String,Object>> rows = sequence(result);

        assertEquals("{n.name=Mark, n.label=null, LABELS(n)=[Person]}", rows.get(0).toString());
        assertEquals("{n.name=Andreas, n.label=null, LABELS(n)=[]}", rows.get(0).toString());
    }

    @Test
    public void shouldImportNodesWithoutLabels() {
        List<Map<String, Object>> nodes = new ArrayList<Map<String, Object>>();
        nodes.add(DataCreator.nodeWithoutLabel("1", "Mark"));
        nodes.add(DataCreator.nodeWithoutLabel("2", "Andreas"));

        new Neo4jJavaAPI(db, 1).importNodes(sequence(nodes));

        String query = " START n = node(*)";
        query       += " RETURN n.name, LABELS(n), n.label";


        ExecutionResult result = new ExecutionEngine(db).execute(query);

        Sequence<Map<String,Object>> rows = sequence(result);

        assertEquals("{n.name=Mark, n.label=null, LABELS(n)=[]}", rows.get(0).toString());
        assertEquals("{n.name=Andreas, n.label=null, LABELS(n)=[]}", rows.get(0).toString());
    }
}
