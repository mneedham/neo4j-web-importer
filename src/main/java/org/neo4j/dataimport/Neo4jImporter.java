package org.neo4j.dataimport;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import org.codehaus.jackson.jaxrs.JacksonJsonProvider;

import static com.googlecode.totallylazy.Sequences.sequence;

public class Neo4jImporter {

    private Neo4jServer neo4jServer;
    private NodesParser nodesParser;
    private RelationshipsParser relationshipsParser;

    public Neo4jImporter(Neo4jServer neojServer, NodesParser nodesParser, RelationshipsParser relationshipsParser ) {
        this.neo4jServer = neojServer;
        this.nodesParser = nodesParser;
        this.relationshipsParser = relationshipsParser;
    }

    public void run() {
        System.out.println("Importing data into your neo4j database...");

        System.out.println("Importing nodes...");
        Map<String,Long> nodeMappingIds = neo4jServer.importNodes( nodesParser );

        System.out.println( "Importing relationships..." );
        neo4jServer.importRelationships( sequence( relationshipsParser.relationships() ), nodeMappingIds );
    }

    public static void main( String[] args ) throws IOException
    {
        int batchSize = 50;
        if(args.length > 0 && args[0] != null) {
            batchSize = Integer.valueOf(args[0]);
        }

        int batchWithinBatchSize = 25;
        if(args.length > 1 && args[1] != null) {
            batchWithinBatchSize = Integer.valueOf(args[0]);
        }

        NodesParser nodes = new NodesParser(new File("nodes.csv"));
        RelationshipsParser relationshipsParser = new RelationshipsParser(new File("relationships.csv"));
        Neo4jServer neo4jServer = new Neo4jTransactionalAPI(jerseyClient(), batchSize, batchWithinBatchSize );

        new Neo4jImporter( neo4jServer, nodes, relationshipsParser ).run();
    }

    private static Client jerseyClient() {
        DefaultClientConfig defaultClientConfig = new DefaultClientConfig();
        defaultClientConfig.getClasses().add( JacksonJsonProvider.class );
        return Client.create( defaultClientConfig );
    }
}
