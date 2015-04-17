package edu.buffalo.cse.cse486586.simpledynamo.Utils;

import android.net.Uri;
import android.util.Log;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;

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

}
