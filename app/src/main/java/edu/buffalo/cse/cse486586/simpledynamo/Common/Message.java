package edu.buffalo.cse.cse486586.simpledynamo.Common;

import java.io.Serializable;
import java.util.HashMap;

/**
 * SimpleDynamo
 * <p/>
 * Created by darrenxyli on 4/9/15.
 * Changed by darrenxyli on 4/9/15 1:17 PM.
 */
public class Message implements Serializable {

    private static final long serialVersionUID = 5393191541861509332L;

    public static enum type {
        INSERT, DELETE, QUERY,
        INSERT_ACK, DELETE_ACK, QUERY_ACK,
        JOIN, STABILIZATION, REPLICA
    };

    public type msgType;
    public int originPort; // who sends this
    public int forwardPort; // who will be sented next
    public String key; // key to store
    public String value; // value to store
    public int version; // version to store

    public HashMap<String, String> batch = new HashMap<>();
}
