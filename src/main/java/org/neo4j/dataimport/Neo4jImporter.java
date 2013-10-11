package org.neo4j.dataimport;

import java.io.IOException;
import java.util.Map;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import javax.inject.Inject;
import io.airlift.command.Command;
import io.airlift.command.Option;
import io.airlift.command.SingleCommand;
import org.codehaus.jackson.jaxrs.JacksonJsonProvider;

import static com.googlecode.totallylazy.Sequences.sequence;

@Command(name = "neo4j-loader", description = "tool for loading data into neo4j")
public class Neo4jImporter
{
    @Inject
    public io.airlift.command.HelpOption helpOption;

    @Option(name = {"-nb", "--nodebatchsize"}, title="# nodes to commit in a tx", description = "(default: 10000)")
    private int nodeBatchSize = 10000;

    @Option(name = {"-c", "--commit"}, title="# rels to commit in a tx", description = "(default: 200)")
    private int batchSize = 200;

    @Option(name = {"-n", "--nodesfile"}, title="Path to nodes file", description = "(default: nodes.csv)")
    private NodesParser nodesParser = new NodesParser( "nodes.csv" );

    @Option(name = {"-r", "--relsfile"}, title="Path to relationships file", description = "(default: relationships.csv)")
    private RelationshipsParser relationshipsParser = new RelationshipsParser( "relationships.csv" );

    @Option(name = {"-db"}, description = "host:port of neo4j server (default: http://localhost:7474)")
    private String neo4jServerLocation = "http://localhost:7474";
    private Neo4jServer neo4jServer;


    public Neo4jImporter()
    {
        // needed by airline
    }

    public Neo4jImporter( NodesParser nodesParser, RelationshipsParser relationshipsParser, int batchSize,
                          String neo4jServerLocation, Neo4jServer neo4jServer )
    {
        this.nodesParser = nodesParser;
        this.relationshipsParser = relationshipsParser;
        this.batchSize = batchSize;
        this.neo4jServerLocation = neo4jServerLocation;
        this.neo4jServer = neo4jServer;
    }

    public void run()
    {
        if ( neo4jServer == null )
        {
            neo4jServer = new Neo4jTransactionalAPI( jerseyClient(), batchSize, 200, neo4jServerLocation, nodeBatchSize );
        }

        nodesParser.checkFileExists();
        relationshipsParser.checkFileExists();


        Map<String, Long> nodeMappingIds = neo4jServer.importNodes(sequence(nodesParser.extractNodes()));
        neo4jServer.importRelationships( relationshipsParser.relationships(), nodeMappingIds );
    }

    public static void main( String[] args ) throws IOException
    {
        Neo4jImporter neo4jImporter = SingleCommand.singleCommand( Neo4jImporter.class ).parse( args );

        if ( neo4jImporter.helpOption.showHelpIfRequested() )
        {
            return;
        }

        neo4jImporter.run();
    }

    private static Client jerseyClient()
    {
        DefaultClientConfig defaultClientConfig = new DefaultClientConfig();
        defaultClientConfig.getClasses().add( JacksonJsonProvider.class );
        return Client.create( defaultClientConfig );
    }
}
