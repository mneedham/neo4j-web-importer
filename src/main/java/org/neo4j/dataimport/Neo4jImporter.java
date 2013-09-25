package org.neo4j.dataimport;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import org.codehaus.jackson.jaxrs.JacksonJsonProvider;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import static org.kohsuke.args4j.ExampleMode.ALL;

import static com.googlecode.totallylazy.Sequences.sequence;

public class Neo4jImporter
{
    @Option(name="--commit", aliases = "-c", usage="number of relationships to commit in a transaction", required = false)
    private int batchSize = 200;



    public void run( String[] args )
    {
        CmdLineParser parser = new CmdLineParser( this );

        try
        {
            parser.parseArgument( args );
        }
        catch ( CmdLineException e )
        {
            // if there's a problem in the command line,
            // you'll get this exception. this will report
            // an error message.
            System.err.println(e.getMessage());
            System.err.println("./neo4j-loader");
            // print the list of available options
            parser.printUsage(System.err);
            System.err.println();

            // print option sample. This is useful some time
            System.err.println("  e.g. : ./neo4j-loader "+parser.printExample(ALL));

            return;
        }

//        neo4jloader -db localhost:7474 -nodefile "/location/nodes.csv" -relfile "/location/rels.csv" -commit 200
//        "Command-line standard is usually either -n or --nodefile"

        int batchWithinBatchSize = 50;

        NodesParser nodesParser = new NodesParser( new File( "nodes.csv" ) );
        RelationshipsParser relationshipsParser = new RelationshipsParser( new File( "relationships.csv" ) );
        Neo4jServer neo4jServer = new Neo4jTransactionalAPI( jerseyClient(), batchSize, batchWithinBatchSize );

        System.out.println( "Importing data into your neo4j database..." );

        System.out.println( "Importing nodes..." );
        Map<String, Long> nodeMappingIds = neo4jServer.importNodes( nodesParser );

        System.out.println( "Importing relationships..." );
        neo4jServer.importRelationships( sequence( relationshipsParser.relationships() ), nodeMappingIds );
    }

    public static void main( String[] args ) throws IOException
    {
        new Neo4jImporter().run( args );
    }

    private static Client jerseyClient()
    {
        DefaultClientConfig defaultClientConfig = new DefaultClientConfig();
        defaultClientConfig.getClasses().add( JacksonJsonProvider.class );
        return Client.create( defaultClientConfig );
    }
}
