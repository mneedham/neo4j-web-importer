package org.neo4j.dataimport;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import au.com.bytecode.opencsv.CSVReader;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;

public class NodesParser
{
    private final File path;
    private final FileType fileType;

    public NodesParser(String path) {
        this(new File(path));
    }

    public NodesParser( File path ) {
        this(path, FileType.NODES_TAB_DELIMITED_CSV);
    }

    public NodesParser( File path, FileType fileType )
    {
        this.path = path;
        this.fileType = fileType;
    }

    public List<Map<String, Object>> extractNodes() {
        List<Map<String, Object>> nodes = new ArrayList<Map<String, Object>>(  );

        try {
            CSVReader reader = new CSVReader(new FileReader( path ), fileType.separator());

            String[] header = reader.readNext();
            if (header == null || !Arrays.asList( header ).contains("id")) {
                throw new RuntimeException("No header line found or 'id' field missing in nodes file");
            }

            String[] nextLine;
            while ((nextLine = reader.readNext()) != null) {
                Map<String, Object> node = new HashMap<String, Object>(  );
                for(int i=0; i < nextLine.length; i++) {
                    node.put(header[i], nextLine[i]);
                }
                nodes.add(node);
            }
        } catch (FileNotFoundException e) {
            throw new RuntimeException( e );
        } catch (IOException e) {
            throw new RuntimeException( e );
        }

        return nodes;
    }

    public String header() throws IOException
    {
        return new BufferedReader(new FileReader( path )).readLine();
    }

    public void checkFileExists()
    {
        try {
            new CSVReader(new FileReader( path ), fileType.separator());
            System.out.println( "Using nodes file ["  + path + "]" );
        } catch (FileNotFoundException e) {
            throw new RuntimeException("Could not find nodes file", e );
        }
    }
}
