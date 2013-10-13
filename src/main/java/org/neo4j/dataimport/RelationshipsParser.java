package org.neo4j.dataimport;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import au.com.bytecode.opencsv.CSVReader;

public class RelationshipsParser {
    private final File path;
    private final FileType fileType;

    public RelationshipsParser(String path) {
        this(new File(path));
    }

    public RelationshipsParser(File path) {
        this(path, FileType.RELATIONSHIPS_TAB_DELIMITED_CSV);
    }

    public RelationshipsParser(File path, FileType fileType) {
        this.path = path;
        this.fileType = fileType;
    }

    public Iterator<Map<String, Object>> relationships() throws IOException {
        FileReader reader = new FileReader(path);

        final CSVReader csvReader = new CSVReader(new BufferedReader(reader), fileType.separator());
        final String[] fields = csvReader.readNext();

        final Map<String, Object> properties = new LinkedHashMap<>();
        initialiseAsNull(properties, fields);

        return new Iterator<Map<String, Object>>() {
            String[] data = csvReader.readNext();

            @Override
            public boolean hasNext() {
                return data != null;
            }

            @Override
            public Map<String, Object> next() {
                int i = 0;
                for (Map.Entry<String, Object> row : properties.entrySet()) {
                    row.setValue(data[i++]);
                }

                try {
                    data = csvReader.readNext();
                } catch (IOException e) {
                    data = null;
                }

                return properties;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    private void initialiseAsNull(Map<String, Object> relationship, String[] fields) {
        for (String field : fields) {
            relationship.put(field, null);
        }
    }


    public String header() throws IOException {
        return new BufferedReader(new FileReader(path)).readLine();
    }

    public void checkFileExists() {
        CSVReader csvReader = null;
        try {
            csvReader = new CSVReader(new FileReader(path), fileType.separator());
            String[] fields = csvReader.readNext();

            if (headerInvalid(fields)) {
                throw new RuntimeException("No header line found or 'from', 'to' or 'type' fields missing in relationships file");
            }

            System.out.println("Using relationships file [" + path + "]");
        } catch (FileNotFoundException e) {
            throw new RuntimeException("Could not find relationships file", e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            if (csvReader != null) {
                try {
                    csvReader.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    private boolean headerInvalid(String[] fields) {
        return fields == null || !Arrays.asList(fields).contains("from") || !Arrays.asList(fields).contains("to")
                || !Arrays.asList(fields).contains("type");
    }
}