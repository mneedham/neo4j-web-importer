package org.neo4j.dataimport;

import java.util.Map;

import com.googlecode.totallylazy.Sequence;

public interface Neo4jServer
{
    Map<String, Long> importNodes( NodesParser nodesParser );
    void importRelationships( Sequence<Map<String, Object>> relationships, Map<String, Long> nodeIdMappings );
}
