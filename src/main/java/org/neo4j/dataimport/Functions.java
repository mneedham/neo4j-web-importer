package org.neo4j.dataimport;

import com.googlecode.totallylazy.Callable1;

import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: markhneedham
 * Date: 29/09/2013
 * Time: 18:09
 * To change this template use File | Settings | File Templates.
 */
public class Functions {
    public static Callable1<Map<String, Object>, String> label()
    {
        return new Callable1<Map<String, Object>, String>()
        {
            @Override
            public String call( Map<String, Object> row ) throws Exception
            {
                Object label = row.get("label");
                if(label == null ) {
                    return "";
                }
                return label.toString();
            }
        };
    }
}
