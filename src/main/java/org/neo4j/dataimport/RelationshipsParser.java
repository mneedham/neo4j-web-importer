package org.neo4j.dataimport;

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
    private File relationshipsPath;

    public RelationshipsParser( File relationshipsPath ) {
        this.relationshipsPath = relationshipsPath;
    }

    public List<Map<String, Object>> relationships() {
        List<Map<String, Object>> relationships = new ArrayList<Map<String, Object>>();

        try {
            CSVReader reader = new CSVReader(new FileReader(relationshipsPath), '\t');

            String[] header = reader.readNext();
            if (header == null || !asList(header).contains("from") || !asList(header).contains("to") || !asList(header).contains("type") ) {
                throw new RuntimeException("No header line found or 'from', 'to', 'type' fields missing in nodes.csv");
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
}
