package org.neo4j.dataimport;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import au.com.bytecode.opencsv.CSVReader;
import com.googlecode.totallylazy.Function;
import com.googlecode.totallylazy.Sequence;
import com.googlecode.totallylazy.Sequences;

import static java.util.Arrays.asList;

import static com.googlecode.totallylazy.Predicates.notNullValue;

public class RelationshipsParser
{
    private final File relationshipsPath;
    private final FileType fileType;

    public RelationshipsParser(String relationshipsPath) {
        this(new File(relationshipsPath));
    }

    public RelationshipsParser( File relationshipsPath ) {
        this(relationshipsPath, FileType.RELATIONSHIPS_TAB_DELIMITED_CSV);
    }

    public RelationshipsParser( File path, FileType fileType )
    {
        this.relationshipsPath = path;
        this.fileType = fileType;
    }

    public static Function<Map<String, Object>> readLine( final CSVReader reader, final String[] fields ) {
        return new Function<Map<String, Object>>() {
            public Map<String, Object> call() throws Exception {
                String[] result = reader.readNext();
                if (result == null) {
                    reader.close();
                    return null;
                }
                Map<String, Object> relationship = new HashMap<String, Object>();
                for(int i=0; i < result.length; i++) {
                    relationship.put(fields[i], result[i]);
                }
                return relationship;
            }
        };
    }

    public Sequence<Map<String, Object>> relationships() {
        try
        {
            FileReader reader = new FileReader( relationshipsPath );
            String[] fields = fields();
            return Sequences.repeat(readLine(new CSVReader( reader, fileType.separator()), fields)).drop(1).takeWhile(notNullValue(Map.class)).memorise();
        }
        catch ( FileNotFoundException e )
        {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            return Sequences.empty();
        }
        catch ( IOException e )
        {
            return Sequences.empty();
        }

//        List<Map<String, Object>> relationships = new ArrayList<Map<String, Object>>();
//
//        try {
//            CSVReader reader = new CSVReader(new FileReader(relationshipsPath), fileType.separator());
//
//            String[] header = reader.readNext();
//            if (header == null || !asList(header).contains("from") || !asList(header).contains("to") || !asList(header).contains("type") ) {
//                throw new RuntimeException("No header line found or 'from', 'to', 'type' fields missing in relationships file");
//            }
//
//            String[] nextLine;
//            while ((nextLine = reader.readNext()) != null) {
//                Map<String, Object> relationship = new HashMap<String, Object>();
//                for(int i=0; i < nextLine.length; i++) {
//                    relationship.put(header[i], nextLine[i]);
//                }
//                relationships.add(relationship);
//            }
//        } catch (FileNotFoundException e) {
//            e.printStackTrace();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//
//        return relationships;
    }

    public String[] fields() throws IOException
    {
        return new CSVReader(new FileReader( relationshipsPath ), fileType.separator()).readNext();
    }

    public String header() throws IOException
    {
        return new BufferedReader(new FileReader(relationshipsPath)).readLine();
    }

    public void checkFileExists()
    {
        try {
            new CSVReader(new FileReader( relationshipsPath ), fileType.separator());
            System.out.println( "Using relationships file ["  + relationshipsPath + "]" );
        } catch (FileNotFoundException e) {
            throw new RuntimeException("Could not find relationships file", e );
        }
    }
}
