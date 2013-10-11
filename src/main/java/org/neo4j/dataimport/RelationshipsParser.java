package org.neo4j.dataimport;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import au.com.bytecode.opencsv.CSVReader;

public class RelationshipsParser
{
    private final File relationshipsPath;
    private final FileType fileType;

    public RelationshipsParser( String relationshipsPath )
    {
        this( new File( relationshipsPath ) );
    }

    public RelationshipsParser( File relationshipsPath )
    {
        this( relationshipsPath, FileType.RELATIONSHIPS_TAB_DELIMITED_CSV );
    }

    public RelationshipsParser( File path, FileType fileType )
    {
        this.relationshipsPath = path;
        this.fileType = fileType;
    }

    public Iterator<Map<String, Object>> relationships() throws IOException
    {
        FileReader reader = new FileReader( relationshipsPath );

        final CSVReader csvReader = new CSVReader( new BufferedReader( reader ), fileType.separator() );
        final String[] fields = csvReader.readNext();

        final Map<String, Object> properties = new LinkedHashMap<String, Object>();
        initialiseAsNull( properties, fields );

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
                int i = 0;
                for ( Map.Entry<String, Object> row : properties.entrySet() )
                {
                    row.setValue( data[i++] );
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

    private void initialiseAsNull( Map<String, Object> relationship, String[] fields )
    {
        for ( String field : fields )
        {
            relationship.put( field, null );
        }
    }


    public String header() throws IOException
    {
        return new BufferedReader( new FileReader( relationshipsPath ) ).readLine();
    }

    public void checkFileExists()
    {
        try
        {
            new CSVReader( new FileReader( relationshipsPath ), fileType.separator() );
            System.out.println( "Using relationships file [" + relationshipsPath + "]" );
        }
        catch ( FileNotFoundException e )
        {
            throw new RuntimeException( "Could not find relationships file", e );
        }
    }
}
