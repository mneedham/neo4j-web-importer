package org.neo4j.dataimport;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.googlecode.totallylazy.Option;
import com.googlecode.totallylazy.Pair;
import com.googlecode.totallylazy.Predicate;
import com.googlecode.totallylazy.Sequence;
import com.sun.jersey.core.header.FormDataContentDisposition;
import com.sun.jersey.multipart.FormDataParam;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.GraphDatabaseAPI;

import static com.googlecode.totallylazy.Sequences.sequence;

@Path("/import")
public class ImportResource
{
    private final GraphDatabaseService database;
    private final Neo4jJavaAPI neo4jJavaAPI;
    private final Executor executor = Executors.newSingleThreadExecutor();
    private static Map<String, CSVImportJob> runningJobs = new HashMap<String, CSVImportJob>();

    public ImportResource( @Context GraphDatabaseService database )
    {
        this.database = database;
        this.neo4jJavaAPI = new Neo4jJavaAPI( database, 10000 );
    }

    @POST
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/nodes")
    public Response uploadNodesFile( @QueryParam(value = "correlationId") String correlationId,
                                     @FormDataParam("nodes") InputStream inputStream,
                                     @FormDataParam("nodes") FormDataContentDisposition fileDetails ) throws IOException
    {


        String fileLocation = uploadFile( correlationId, inputStream, fileDetails );

        String header = new NodesParser( new File( fileLocation ) ).header();
        Pair<FileType, Integer> fileType = FileInvestigator.mostLikelyFileType( header );
        ObjectNode fileUploadResponse = buildResponse( fileLocation, header, fileType );

        String response = new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString( fileUploadResponse );
        return Response.status( 200 ).entity( response ).type( MediaType.APPLICATION_JSON ).build();
    }

    @POST
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/relationships")
    public Response uploadRelationshipsFile( @QueryParam(value = "correlationId") String correlationId,
                                             @FormDataParam("relationships") InputStream inputStream,
                                             @FormDataParam("relationships") FormDataContentDisposition fileDetails )
            throws
            IOException
    {
        String fileLocation = uploadFile( correlationId, inputStream, fileDetails );

        String header = new RelationshipsParser( new File( fileLocation ) ).header();
        Pair<FileType, Integer> fileType = FileInvestigator.mostLikelyFileType( header );
        ObjectNode fileUploadResponse = buildResponse( fileLocation, header, fileType );

        String response = new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString( fileUploadResponse );
        return Response.status( 200 ).entity( response ).type( MediaType.APPLICATION_JSON ).build();
    }

    private ObjectNode buildResponse( String fileLocation, String header, Pair<FileType, Integer> fileType )
    {
        ObjectNode response = JsonNodeFactory.instance.objectNode();
        response.put( "path", fileLocation );
        response.put( "fileType", fileType.first().friendlyName() );
        response.put( "enumFileType", fileType.first().name() );
        response.put( "header", header );
        return response;
    }

    private String uploadFile( String correlationId, InputStream inputStream, FormDataContentDisposition fileDetails )
    {
        File importDirectory = createImportDirectory( correlationId );
        String fileLocation = uploadFileLocation( fileDetails, importDirectory );
        FileHelper.writeToFile( inputStream, fileLocation );

        return new File( fileLocation ).getAbsolutePath();

    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/status/{correlationId}")
    public Response howIsMyJobDoing( @PathParam(value = "correlationId") String correlationId ) throws IOException
    {
        System.out.println( "runningJobs = " + runningJobs );
        System.out.println( "correlationId = " + correlationId );

        JsonNode jobStatus = runningJobs.get( correlationId ).toJson();
        String response = new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString( jobStatus );

        return Response.ok().entity( response ).type( MediaType.APPLICATION_JSON ).build();
    }

    @POST
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/incremental")
    public Response process( @QueryParam(value = "correlationId") String correlationId,
                             @QueryParam(value = "files") String filesAsString ) throws IOException
    {
        try
        {
            File importDirectory = createImportDirectory( correlationId );
            Sequence<JsonNode> files = sequence( new ObjectMapper().readTree( filesAsString ) );

            JsonNode nodeFile = files.find( isNodeCSVFile() ).get();
            FileType nodesFileType = FileType.valueOf( nodeFile.get( "type" ).asText() );
            String nodesFileLocation = importDirectory.getPath() + "/" + nodeFile.get( "name" ).asText();

            Option<JsonNode> potentialRelationshipFile = files.find( isRelationshipCSVFile() );
            if ( !potentialRelationshipFile.isEmpty() )
            {
                JsonNode relationshipFile = potentialRelationshipFile.get();
                FileType relationshipsFileType = FileType.valueOf( relationshipFile.get( "type" ).asText() );
                String relationshipsFileLocation = importDirectory.getPath() + "/" + relationshipFile.get( "name" ).asText();

                CSVImportJob job = new CSVImportJob( correlationId, nodesFileLocation, relationshipsFileLocation,
                        nodesFileType, relationshipsFileType );

                executor.execute( job );

                return Response
                        .created( URI.create( "http://localhost:7474/tools/import/status/" + correlationId ) )
                        .build();
            }
            else
            {
                return Response.status( Response.Status.BAD_REQUEST ).entity( "No relationships file specified" ).build();
            }
        }
        catch ( Exception e )
        {
            StringWriter writer = new StringWriter();
            e.printStackTrace( new PrintWriter( writer ) );
            return Response.serverError().entity( writer.toString() ).build();
        }
    }

    private Predicate<JsonNode> isNodeCSVFile()
    {
        return new Predicate<JsonNode>()
        {
            @Override
            public boolean matches( JsonNode root )
            {
                return FileType.valueOf( root.get( "type" ).asText() ).isNodesCSV();
            }
        };
    }

    private Predicate<JsonNode> isRelationshipCSVFile()
    {
        return new Predicate<JsonNode>()
        {
            @Override
            public boolean matches( JsonNode root )
            {
                return FileType.valueOf( root.get( "type" ).asText() ).isRelationshipsCSV();
            }
        };
    }

    class CSVImportJob implements Runnable
    {
        private final String nodesFileLocation;
        private final String relationshipsFileLocation;
        private String correlationId;
        private FileType nodesFileType;
        private FileType relationshipsFileType;
        private boolean finished = false;
        private Exception exception;

        CSVImportJob( String correlationId, String nodesFileLocation, String relationshipsFileLocation, FileType
                nodesFileType, FileType relationshipsFileType )
        {
            this.nodesFileLocation = nodesFileLocation;
            this.relationshipsFileLocation = relationshipsFileLocation;
            this.correlationId = correlationId;
            this.nodesFileType = nodesFileType;
            this.relationshipsFileType = relationshipsFileType;
            runningJobs.put( correlationId, this );
        }

        public void run()
        {
            try
            {
                // look at whether making this return a sequence deals with it lazily
                NodesParser nodesParser = new NodesParser( new File( nodesFileLocation ), nodesFileType );

                Map<String, Long> nodeMappings = neo4jJavaAPI.importNodes( sequence( nodesParser.extractNodes() ) );

                List<Map<String, Object>> relationships = new RelationshipsParser( new File(
                        relationshipsFileLocation ), relationshipsFileType ).relationships();

                neo4jJavaAPI.importRelationships( sequence( relationships ), nodeMappings );
            }
            catch ( Exception e )
            {
                this.exception = e;
            }
            finally
            {
                finished = true;
            }
        }

        public JsonNode toJson()
        {
            ObjectNode root = JsonNodeFactory.instance.objectNode();
            root.put( "correlationId", correlationId );
            root.put( "finished", finished );

            if ( exception != null )
            {
                StringWriter writer = new StringWriter();
                exception.printStackTrace( new PrintWriter( writer ) );
                root.put( "exception", writer.toString() );
            }

            return root;
        }
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
        File importDirectory = createImportDirectory( String.valueOf( System.currentTimeMillis() ) );

        String nodesFileLocation = uploadFileLocation( nodesFilesDetails, importDirectory );
        FileHelper.writeToFile( nodesInputStream, nodesFileLocation );

        String relationshipsFileLocation = uploadFileLocation( relationshipsFilesDetails, importDirectory );
        FileHelper.writeToFile( relationshipsInputStream, relationshipsFileLocation );

        // look at whether making this return a sequence deals with it lazily
//        run( nodesFileLocation, relationshipsFileLocation );

        String output = "File uploaded to : " + nodesFileLocation;
        return Response.status( 200 ).entity( output ).build();
    }

    private File createImportDirectory( String folderExtension )
    {
        File importDirectory = new File( ((GraphDatabaseAPI) database).getStoreDir() + "/../import/" +
                folderExtension );
        importDirectory.mkdir();
        return importDirectory;
    }

    private String uploadFileLocation( FormDataContentDisposition fileDetail, File importDirectory )
    {
        return importDirectory.toPath() + "/" + fileDetail.getFileName();
    }


}
