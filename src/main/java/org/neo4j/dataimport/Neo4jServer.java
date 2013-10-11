package org.neo4j.dataimport;

import java.util.Iterator;
import java.util.Map;

import com.googlecode.totallylazy.Sequence;

public interface Neo4jServer
{
    Map<String, Long> importNodes(Sequence<Map<String, Object>> nodes);
    void importRelationships( Sequence<Map<String, Object>> relationships, Map<String, Long> nodeIdMappings );
    void importRelationships2( Iterator<Map<String, Object>> relationships, Map<String, Long> nodeIdMappings );
}
