package org.neo4j.dataimport;

import java.util.HashMap;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: markhneedham
 * Date: 29/09/2013
 * Time: 17:43
 * To change this template use File | Settings | File Templates.
 */
public class DataCreator {
    public static Map<String, Object> nodeWithLabel(String id, String name, String label) {
        Map<String, Object> node = new HashMap<String, Object>();
        node.put("id", id);
        node.put("name", name);
        node.put("label", label);
        return node;
    }

    public static Map<String, Object> nodeWithoutLabel(String id, String name) {
        Map<String, Object> node = new HashMap<String, Object>();
        node.put("id", id);
        node.put("name", name);
        return node;
    }

    public static Map<String, Object> relationship(String from, String to, String type) {
        Map<String, Object> relationship = new HashMap<String, Object>();
        relationship.put("from", from);
        relationship.put("to", to);
        relationship.put("type", type);
        return relationship;
    }
}
