package edu.buffalo.cse.cse486586.simpledynamo.Common;

import android.util.Pair;

import java.util.HashMap;

/**
 * SimpleDynamo
 * <p/>
 * Created by darrenxyli on 4/9/15.
 * Changed by darrenxyli on 4/9/15 1:17 PM.
 */
public class Message {

    public String msgType;
    public int originPort; // who sends this
    public int forwardPort; // who will be sented next
    public String key; // key to store
    public String value; // value to store
    public int version; // version to store
    public HashMap<String, Pair<String, Integer>> batch;

    public Message(String type, String k, String v, int vers, int oPort, int fPort) {
        this.msgType = (type != null) ? type : Type.STABILIZATION;
        this.originPort = (oPort != -1) ? oPort : -1;
        this.forwardPort = fPort;
        this.key = (k != null) ? k : "";
        this.value = (v != null) ? v : "";
        this.version = vers;
        this.batch = new HashMap<>();
    }

    public static class Type {
        public static final String INSERT = "INSERT";
        public static final String DELETE = "DELETE";
        public static final String QUERY = "QUERY";
        public static final String ACK = "ACK";
        public static final String JOIN = "JOIN";
        public static final String STABILIZATION = "STABILIZATION";
        public static final String REPLICA = "REPLICA";
        public static final String UPDATE = "UPDATE";
    }

}
