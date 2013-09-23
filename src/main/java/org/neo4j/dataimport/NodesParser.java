package org.neo4j.dataimport;

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

public class NodesParser
{
    private final File nodesPath;

    public NodesParser( File nodesPath ) {
        this.nodesPath = nodesPath;
    }

    public List<Map<String, Object>> extractNodes() {
        List<Map<String, Object>> nodes = new ArrayList<Map<String, Object>>(  );

        try {
            CSVReader reader = new CSVReader(new FileReader(nodesPath), '\t');

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
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return nodes;
    }
}
