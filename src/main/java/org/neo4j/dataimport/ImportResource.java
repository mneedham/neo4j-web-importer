package org.neo4j.dataimport;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
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
    private final Neo4jServer neo4jServer;

    public ImportResource( @Context GraphDatabaseService database )
    {
        this.database = database;
        this.neo4jServer = new Neo4jServer( database, 10000 );
    }

    @POST
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/nodes")
    public Response uploadNodesFile(@QueryParam(value = "correlationId") String correlationId,
                                    @FormDataParam("nodes") InputStream inputStream,
                                    @FormDataParam("nodes") FormDataContentDisposition fileDetails) {
        return uploadFile( correlationId, inputStream, fileDetails );
    }

    @POST
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/relationships")
    public Response uploadRelationshipsFile(@QueryParam(value = "correlationId") String correlationId,
                                    @FormDataParam("relationships") InputStream inputStream,
                                    @FormDataParam("relationships") FormDataContentDisposition fileDetails) {
        return uploadFile( correlationId, inputStream, fileDetails );
    }

    private Response uploadFile( String correlationId, InputStream inputStream, FormDataContentDisposition fileDetails )
    {
        File importDirectory = createImportDirectory( correlationId );
        String fileLocation = uploadFileLocation( fileDetails, importDirectory );
        FileHelper.writeToFile( inputStream, fileLocation );

        String output = "File uploaded to : " + fileLocation;
        return Response.status( 200 ).entity( output ).build();
    }

    @POST
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/incremental")
    public Response process(@QueryParam(value = "correlationId") String correlationId,
                            @QueryParam(value = "lastNodesFile") String nodesFile,
                            @QueryParam(value = "lastRelationshipsFile") String relationshipsFile) {
        File importDirectory = createImportDirectory( correlationId );
        String nodesFileLocation = importDirectory.getPath() + "/" + nodesFile;
        String relationshipsFileLocation = importDirectory.getPath() + "/" + relationshipsFile;

        // look at whether making this return a sequence deals with it lazily
        List<Map<String, Object>> nodes = new NodesParser( new File( nodesFileLocation ) ).extractNodes();

        Map<String, Long> nodeMappings = neo4jServer.importNodes( nodes );

        List<Map<String, Object>> relationships = new RelationshipsParser( new File( relationshipsFileLocation ) ).relationships();

        neo4jServer.importRelationships( sequence( relationships ), nodeMappings );

        String output = "File uploaded to : " + nodesFileLocation;
        return Response.status( 200 ).entity( output ).build();
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
        File importDirectory = createImportDirectory( String.valueOf(System.currentTimeMillis() ));

        String nodesFileLocation = uploadFileLocation( nodesFilesDetails, importDirectory );
        FileHelper.writeToFile(nodesInputStream, nodesFileLocation);

        String relationshipsFileLocation = uploadFileLocation( relationshipsFilesDetails, importDirectory );
        FileHelper.writeToFile(relationshipsInputStream, relationshipsFileLocation);

        // look at whether making this return a sequence deals with it lazily
        List<Map<String, Object>> nodes = new NodesParser( new File( nodesFileLocation ) ).extractNodes();

        Map<String, Long> nodeMappings = neo4jServer.importNodes( nodes );

        List<Map<String, Object>> relationships = new RelationshipsParser( new File( relationshipsFileLocation ) )
                .relationships();

        neo4jServer.importRelationships( sequence( relationships ), nodeMappings );

        String output = "File uploaded to : " + nodesFileLocation;
        return Response.status( 200 ).entity( output ).build();
    }

    private File createImportDirectory( String folderExtension )
    {
        File importDirectory = new File( ((GraphDatabaseAPI) database).getStoreDir() + "/../import/" + folderExtension );
        importDirectory.mkdir();
        return importDirectory;
    }

    private String uploadFileLocation( FormDataContentDisposition fileDetail, File importDirectory )
    {
        return importDirectory.toPath() + "/" + fileDetail.getFileName();
    }


}
