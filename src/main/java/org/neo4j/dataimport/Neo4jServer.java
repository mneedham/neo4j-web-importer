package org.neo4j.dataimport;

import java.util.Iterator;
import java.util.Map;

import com.googlecode.totallylazy.Sequence;

public interface Neo4jServer
{
    Map<String, Long> importNodes( Iterator<Map<String, Object>> nodes);
    void importRelationships( Iterator<Map<String, Object>> relationships, Map<String, Long> nodeIdMappings );
}
