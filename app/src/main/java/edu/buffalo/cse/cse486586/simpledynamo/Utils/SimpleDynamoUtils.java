package edu.buffalo.cse.cse486586.simpledynamo.Utils;

import android.net.Uri;
import android.util.Log;
import android.util.Pair;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.util.HashMap;

import edu.buffalo.cse.cse486586.simpledynamo.Common.Message;

/**
 * SimpleDynamo
 * <p/>
 * Created by darrenxyli on 4/9/15.
 * Changed by darrenxyli on 4/9/15 11:24 AM.
 */
public class SimpleDynamoUtils {

    public static final int SERVER_PORT = 10000;
    public static final int WRITE_CONFIRM_NODE_NUM = 2;
    public static final int READ_CONFIRM_NODE_NUM = 2;
    public static final String DATABASE_AUTHORITY = "edu.buffalo.cse.cse486586.simpledynamo.provider";
    public static final String DATABASE_SCHEME = "content";
    public static final Uri DATABASE_CONTENT_URL = buildUri(DATABASE_SCHEME, DATABASE_AUTHORITY);
    private static String TAG = SimpleDynamoUtils.class.getSimpleName();

    /**
     * buildUri() demonstrates how to build a URI for a ContentProvider.
     *
     * @param scheme    String
     * @param authority String
     * @return the URI
     */
    public static Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }


    /**
     * generate hash
     *
     * @param input
     * @return <code>String</code> hash key
     */
    public static String genHash(String input) {
        MessageDigest sha1 = null;
        try {
            sha1 = MessageDigest.getInstance("SHA-1");
        } catch (NoSuchAlgorithmException e) {
            Log.e(TAG, e.toString());
        }
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }


    /**
     * convert Message to JSON String
     *
     * @param msg
     * @return
     */
    public static String toJSON(Message msg) {
        JSONObject obj = new JSONObject();
        JSONArray array = new JSONArray();

        try {
            obj.put("originPort", msg.originPort);
            obj.put("forwardPort", msg.forwardPort);
            obj.put("key", msg.key);
            obj.put("value", msg.value);
            obj.put("version", msg.version);
            obj.put("msgType", msg.msgType);

            for (String k : msg.batch.keySet()) {
                Pair<String, Integer> p = msg.batch.get(k);
                String value = p.first;
                int version = p.second;

                JSONObject ob = new JSONObject();
                ob.put("key", k);
                ob.put("value", value);
                ob.put("version", version);
                array.put(ob);
            }

            obj.put("batch", array);

        } catch (JSONException e) {
            e.printStackTrace();
        }
        return obj.toString();
    }


    /**
     * convert json string to Message
     *
     * @param json
     * @return
     */
    public static Message parseJSON(String json) {
        Message m = new Message(null, null, null, -1, -1, -1);
        HashMap<String, Pair<String, Integer>> h = new HashMap<>();

        try {
            JSONObject j = new JSONObject(json);
            JSONArray arr = j.getJSONArray("batch");
            m.msgType = j.getString("msgType");
            m.originPort = j.getInt("originPort");
            m.forwardPort = j.getInt("forwardPort");
            m.key = j.getString("key");
            m.value = j.getString("value");
            m.version = j.getInt("version");

            for (int i = 0; i < arr.length(); i++) {
                JSONObject o = arr.getJSONObject(i);
                String key = o.getString("key");
                String value = o.getString("value");
                int version = o.getInt("version");

                Pair<String, Integer> p = new Pair<>(value, version);
                h.put(key, p);
            }
            m.batch = h;
        } catch (JSONException e) {
            e.printStackTrace();
        }
        return m;
    }

}
