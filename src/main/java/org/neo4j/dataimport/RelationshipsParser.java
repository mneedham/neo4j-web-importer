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

import static java.util.Arrays.asList;

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

    public List<Map<String, Object>> relationships() {
        List<Map<String, Object>> relationships = new ArrayList<Map<String, Object>>();

        try {
            CSVReader reader = new CSVReader(new FileReader(relationshipsPath), fileType.separator());

            String[] header = reader.readNext();
            if (header == null || !asList(header).contains("from") || !asList(header).contains("to") || !asList(header).contains("type") ) {
                throw new RuntimeException("No header line found or 'from', 'to', 'type' fields missing in relationships file");
            }

            String[] nextLine;
            while ((nextLine = reader.readNext()) != null) {
                Map<String, Object> relationship = new HashMap<String, Object>();
                for(int i=0; i < nextLine.length; i++) {
                    relationship.put(header[i], nextLine[i]);
                }
                relationships.add(relationship);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return relationships;
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
