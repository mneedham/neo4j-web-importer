package org.neo4j.dataimport;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.sun.jersey.core.header.FormDataContentDisposition;
import com.sun.jersey.multipart.FormDataParam;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.GraphDatabaseAPI;

import static com.googlecode.totallylazy.Sequences.sequence;

@Path("/import")
public class ImportResource
{
    private final GraphDatabaseService database;

    public ImportResource( @Context GraphDatabaseService database )
    {
        this.database = database;
    }

    @GET
    @Produces(MediaType.TEXT_HTML)
    public Response index()
    {
        return Response.ok().entity( renderView() ).build();
    }

    private String renderView()
    {
        StringBuilder stringBuilder = null;
        try
        {
            BufferedReader reader = new BufferedReader( new FileReader(
                    "/Users/markneedham/projects/neo4j-web-importer/src/main/resources/index.html" ) );
            stringBuilder = new StringBuilder();
            String ls = System.getProperty( "line.separator" );

            String line = null;
            while ( (line = reader.readLine()) != null )
            {
                stringBuilder.append( line );
                stringBuilder.append( ls );
            }
        }
        catch ( Exception ignored )
        {

        }

        return stringBuilder.toString();
    }

    @POST
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    @Produces(MediaType.APPLICATION_JSON)
    public Response uploadFile(
            @FormDataParam("nodes") InputStream nodesInputStream,
            @FormDataParam("nodes") FormDataContentDisposition nodesFilesDetails,
            @FormDataParam("relationships") InputStream relationshipsInputStream,
            @FormDataParam("relationships") FormDataContentDisposition relationshipsFilesDetails )
    {
        Neo4jServer neo4jServer = new Neo4jServer( database, 200 );

        String nodesFileLocation = uploadFileLocation( nodesFilesDetails );
        writeToFile( nodesInputStream, nodesFileLocation );

        String relationshipsFileLocation = uploadFileLocation( relationshipsFilesDetails );
        writeToFile( relationshipsInputStream, relationshipsFileLocation );

        List<Map<String, Object>> nodes = new NodesParser( new File( nodesFileLocation ) ).extractNodes();

        Map<String, Long> nodeMappings = neo4jServer.importNodes( nodes );

        List<Map<String, Object>> relationships = new RelationshipsParser( new File( relationshipsFileLocation ) )
                .relationships();

        neo4jServer.importRelationships( sequence( relationships ), nodeMappings );

        String output = "File uploaded to : " + nodesFileLocation;
        return Response.status( 200 ).entity( output ).build();
    }

    private String uploadFileLocation( FormDataContentDisposition fileDetail )
    {
        String fileName = fileDetail.getFileName() + "-" + System.currentTimeMillis();
        return new File( ((GraphDatabaseAPI) database).getStoreDir() + "/../import" ).toPath() + "/" + fileName;
    }

    private void writeToFile( InputStream uploadedInputStream,
                              String uploadedFileLocation )
    {
        OutputStream out = null;
        File f = new File( uploadedFileLocation );
        try
        {

            out = new FileOutputStream( f );
            int read = 0;
            byte[] bytes = new byte[1024];

            out = new FileOutputStream( new File( uploadedFileLocation ) );
            while ( (read = uploadedInputStream.read( bytes )) != -1 )
            {
                out.write( bytes, 0, read );
            }
            out.flush();
        }
        catch ( IOException e )
        {

            e.printStackTrace();
        }
        finally
        {
            if ( out != null )
            {
                try
                {
                    uploadedInputStream.close();
                    out.close();
                }
                catch ( IOException e )
                {
                    e.printStackTrace();
                }
            }
        }

    }


}
