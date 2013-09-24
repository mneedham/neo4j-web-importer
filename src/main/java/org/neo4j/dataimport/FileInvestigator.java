package org.neo4j.dataimport;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.googlecode.totallylazy.Pair;

public class FileInvestigator
{

    public static Pair<FileType, Integer> mostLikelyFileType( String header )
    {
        Map.Entry<String, Integer> separatorAndCount = mostLikelySeparator( header );
        String separator = separatorAndCount.getKey();

        FileType fileType;

        String[] fields = header.split( separator );
        if(fields[0].equals("from") && fields[1].equals("to") && fields[2].equals("type")) {
            if(separator.equals(",")) {
                fileType = FileType.RELATIONSHIPS_COMMA_DELIMITED_CSV;
            } else  {
                fileType = FileType.RELATIONSHIPS_TAB_DELIMITED_CSV;
            }
        } else {
            if(separator.equals(",")) {
                fileType = FileType.NODES_COMMA_DELIMITED_CSV;
            } else  {
                fileType = FileType.NODES_TAB_DELIMITED_CSV;
            }
        }

        return Pair.pair( fileType, separatorAndCount.getValue() );
    }

    public static Map.Entry<String, Integer> mostLikelySeparator( String header )
    {
        List<String> separators = new ArrayList<String>();
        separators.add("\\t");
        separators.add(",");

        Map<String, Integer> separatorsCount = new HashMap<String, Integer>();
        for ( String separator : separators )
        {
            separatorsCount.put(separator, header.split(separator).length);
        }

        Map.Entry<String,Integer> maxEntry = null;
        for(Map.Entry<String,Integer> entry : separatorsCount.entrySet()) {
            if (maxEntry == null || entry.getValue() > maxEntry.getValue()) {
                maxEntry = entry;
            }
        }

        return maxEntry;
    }
}
