package org.neo4j.dataimport;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import au.com.bytecode.opencsv.CSVReader;
import org.apache.commons.lang.StringUtils;
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

    public Iterator<Map<String, Object>> extractNodes() throws IOException
    {

        final CSVReader csvReader = new CSVReader( new BufferedReader( new FileReader( path ) ), fileType.separator() );
        final String[] fields = csvReader.readNext();

        return new Iterator<Map<String, Object>>()
        {
            String[] data = csvReader.readNext();

            @Override
            public boolean hasNext()
            {
                return data != null;
            }

            @Override
            public Map<String, Object> next()
            {
                final Map<String, Object> properties = new LinkedHashMap<String, Object>();
                for ( int i = 0; i < data.length; i++ )
                {
                    properties.put(fields[i], data[i]);

                }

                try
                {
                    data = csvReader.readNext();
                }
                catch ( IOException e )
                {
                    data = null;
                }

                return properties;
            }

            @Override
            public void remove()
            {
                throw new UnsupportedOperationException();
            }
        };
    }

    public String header() throws IOException
    {
        return new BufferedReader(new FileReader( path )).readLine();
    }

    public void checkFileExists()
    {
        CSVReader csvReader = null;
        try {
            csvReader = new CSVReader( new FileReader( path ), fileType.separator() );
            String[] fields = csvReader.readNext();

            if (fields == null || !Arrays.asList( fields ).contains("id"))
                throw new RuntimeException("No header line found or 'id' field missing in nodes file");

            System.out.println( "Using nodes file ["  + path + "]" );
        } catch (FileNotFoundException e) {
            throw new RuntimeException("Could not find nodes file", e );
        }
        catch ( IOException e )
        {
            throw new RuntimeException(e);
        }
        finally {
            if(csvReader != null) {
                try
                {
                    csvReader.close();
                }
                catch ( IOException e )
                {
                    throw new RuntimeException(e);
                }
            }
        }
    }
}
