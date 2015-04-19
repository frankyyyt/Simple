package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;
import android.telephony.TelephonyManager;
import android.util.Log;
import android.util.Pair;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Hashtable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import edu.buffalo.cse.cse486586.simpledynamo.Common.LooperThread;
import edu.buffalo.cse.cse486586.simpledynamo.Common.Membership;
import edu.buffalo.cse.cse486586.simpledynamo.Common.Message;
import edu.buffalo.cse.cse486586.simpledynamo.Store.DatabaseHelper;
import edu.buffalo.cse.cse486586.simpledynamo.Store.DatabaseSchema;
import edu.buffalo.cse.cse486586.simpledynamo.Utils.SimpleDynamoUtils;

public class SimpleDynamoProvider extends ContentProvider {

    private static String TAG;
    private static int localPort;
    private final Object dbLock = new Object(); // lock for database
    Context context; // application context
    DatabaseHelper dbHlp; // database helper
    SQLiteDatabase db; // database object
    private Membership membership;
    private LooperThread sendLooper;
    private LooperThread processLooper;
    private ExecutorService requestExecPool;
    private AtomicInteger enQueueCounter;

    /**
     * Calculate the port number that this AVD listens on.
     *
     * @return port
     */
    private int getLocalPort() {
        TelephonyManager tel =
                (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        return (Integer.parseInt(portStr) * 2);
    }

    /**
     * Elegantly quit the process
     */
    public void quit() {
        sendLooper.quit();
        processLooper.quit();
        requestExecPool.shutdown();
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        return processDelete(uri, selection, selectionArgs);
    }

    public int processDelete(Uri uri, String selection, String[] selectionArgs) {

        String key = (selection != null && selection.contains("\"")) ? selection.substring(1, selection.length() - 1) : selection;

        int i = 0;

        switch (key) {
            case "@":

                Log.d(TAG, "DELETE @");

                synchronized (dbLock) {
                    i = db.delete(DatabaseSchema.DatabaseEntry.TABLE_NAME, null, null);
                }

                Log.d(TAG, "DELETE @ finished");

                return i;

            case "*":

                Log.d(TAG, "DELETE *");

                synchronized (dbLock) {
                    i += db.delete(DatabaseSchema.DatabaseEntry.TABLE_NAME, null, null);
                }

                for (int n = 0; n < membership.REMOTEAVD.size(); n++) {
                    int forward = membership.REMOTEAVD.get(n) * 2;
                    if (forward == localPort) continue;

                    Message delMsg = new Message(Message.Type.DELETE, "@", null, -1, localPort, forward);

                    // send to everyone and wait response and plus result
                }

                return i;

            default:

                // Step 1: Identify the node that holds the key.
                int[] c = membership.findPreferenceList(key);

                // When item is not belonging to current node
                // forward the item to node which it belongs to
                // When item is belonging to current node
                // insert the item into local database firstly
                // then notify the next 2 successor to replicate.

                int coordinator = c[0] * 2;
                int replicA = c[1] * 2;
                int replicB = c[2] * 2;

                int replyNum = 0;

                if (localPort != coordinator) {

                    Message del = new Message(Message.Type.DELETE, key, null, -1, localPort, coordinator);

                    // Step 2: sending requests to coordinator.
                    Log.d(TAG, "DELETE -- FORWARD:" + coordinator + " KEY:" + key);

                    // Step 3: waiting for response from coordinator

                    if (sendDelete(del)) {
                        replyNum++;
                    }

                    // Step 4: process reply and repackage response

                } else {

                    // Step 2: sending requests to coordinator.
                    localDelete(SimpleDynamoUtils.DATABASE_CONTENT_URL, key, null);

                    replyNum++;

                }

                // Step 3: waiting for response from coordinator
                // In there, wait another W-1 reply from replication
                // If repA success, replyNum ++
                // If repB success, replyNum ++

                Message del1 = new Message(Message.Type.DELETE, key, null, -1, localPort, replicA);
                Message del2 = new Message(Message.Type.DELETE, key, null, -1, localPort, replicB);

                if (sendDelete(del1)) replyNum++;
                if (sendDelete(del2)) replyNum++;

                // Step 4: process reply and repackage response
                return (replyNum >= SimpleDynamoUtils.READ_CONFIRM_NODE_NUM) ? i : 0;
        }
    }

    @Override
    public String getType(Uri uri) {
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        return processInsert(uri, values);
    }

    public Uri processInsert(Uri u, ContentValues values) {
        String key = values.getAsString(DatabaseSchema.DatabaseEntry.COLUMN_NAME_KEY);
        String hashKey = SimpleDynamoUtils.genHash(key);
        String value = values.getAsString(DatabaseSchema.DatabaseEntry.COLUMN_NAME_VALUE);


        // Step 1: Identify the node that holds the key.
        int[] c = membership.findPreferenceList(key);
        int coordinator = c[0] * 2;
        int replicA = c[1] * 2;
        int replicB = c[2] * 2;

        // When item is not belonging to current node
        // forward the item to node which it belongs to
        // When item is belonging to current node
        // insert the item into local database firstly
        // then notify the next 2 successor to replicate.

        int replyNum = 0;
        int version = -1;

        boolean coorSuccess = false;
        boolean repASuccess = false;
        boolean repBSuccess = false;

        if (localPort != coordinator) {

            Message ins = new Message(Message.Type.INSERT, key, value, -1, localPort, coordinator);

            // Step 2: sending requests to coordinator.

            Log.d(TAG, "INSERT -- FORWARD:" + coordinator + " KEY:" + key);
            version = sendInsert(ins);

            if (version != -1) coorSuccess = true;

        } else {

            // Step 2: sending requests to coordinator.
            int n = getKeyVersion(hashKey);
            coorSuccess = true;
            version = (n >= version) ? n : version;

        }

        // Step 3: waiting for response from coordinator
        // In there, wait another W-1 reply from replication
        // If repA success, replyNum ++
        // If repB success, replyNum ++

        // Replication
        Message repA = new Message(Message.Type.INSERT, key, value, -1, localPort, replicA);

        Log.d(TAG, "REPLICA -- TO:" + replicA + "KEY:" + key);
        int v1 = sendInsert(repA);
        if (v1 > -1) repASuccess = true;
        version = (v1 >= version) ? v1 : version;

        // Replication
        Message repB = new Message(Message.Type.INSERT, key, value, -1, localPort, replicB);

        Log.d(TAG, "REPLICA -- TO:" + replicB + "KEY:" + key);
        int v2 = sendInsert(repB);
        if (v2 > -1) repBSuccess = true;
        version = (v2 >= version) ? v2 : version;

        // Step 4: process reply and repackage response

        Message u1 = new Message(Message.Type.UPDATE, key, value, version + 1, localPort, coordinator);
        Message u2 = new Message(Message.Type.UPDATE, key, value, version + 1, localPort, replicA);
        Message u3 = new Message(Message.Type.UPDATE, key, value, version + 1, localPort, replicB);


        sendLooper.mHandler.post(new SendThread(u1));
        sendLooper.mHandler.post(new SendThread(u2));
        sendLooper.mHandler.post(new SendThread(u3));

        if (!coorSuccess) {
            Log.e(TAG, "TIMEOUT ABOUT SENDING " + Message.Type.UPDATE + " TO " + coordinator);
            enQueue(key, value, Message.Type.UPDATE, version, coordinator);
        }

        if (!repASuccess) {
            Log.e(TAG, "TIMEOUT ABOUT SENDING " + Message.Type.UPDATE + " TO " + replicA);
            enQueue(key, value, Message.Type.UPDATE, version, replicA);
        }

        if (!repBSuccess) {
            Log.e(TAG, "TIMEOUT ABOUT SENDING " + Message.Type.UPDATE + " TO " + replicB);
            enQueue(key, value, Message.Type.UPDATE, version, replicB);
        }


        return (replyNum >= SimpleDynamoUtils.WRITE_CONFIRM_NODE_NUM) ? u : null;
    }

    @Override
    public boolean onCreate() {


        context = getContext();
        dbHlp = new DatabaseHelper(context);
        db = dbHlp.getWritableDatabase();

        localPort = getLocalPort();
        TAG = String.valueOf(localPort) + " " + SimpleDynamoProvider.class.getSimpleName();

        membership = new Membership();
        sendLooper = new LooperThread();
        processLooper = new LooperThread();
        requestExecPool = Executors.newCachedThreadPool();

        enQueueCounter = new AtomicInteger();

        sendLooper.start();
        processLooper.start();

        // Start thread listening on port
        new Thread(new Server(context)).start();

        // Start failure handler
        new Thread(new DealerWithFailure()).start();

        return true;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection,
                        String[] selectionArgs, String sortOrder) {
        return processQuery(uri, projection, selection, selectionArgs, sortOrder);
    }

    public Cursor processQuery(Uri uri, String[] projection, String selection,
                               String[] selectionArgs, String sortOrder) {
        Cursor cur = null;
        Cursor cursor = null;

        String key = (selection != null && selection.contains("\"")) ? selection.substring(1, selection.length() - 1) : selection;
        String hashKey = (key != null) ? SimpleDynamoUtils.genHash(key) : "";
        Hashtable<String, Pair<String, Integer>> decision = new Hashtable<String, Pair<String, Integer>>() {
        };

        switch (key) {
            case "@":

                Log.d(TAG, "QUERY @");
                cur = new MatrixCursor(new String[]{
                        DatabaseSchema.DatabaseEntry.COLUMN_NAME_KEY,
                        DatabaseSchema.DatabaseEntry.COLUMN_NAME_VALUE});

                cursor = localQuery(SimpleDynamoUtils.DATABASE_CONTENT_URL, null, "@", null, null);

                if (cursor != null) {
                    int keyIndex = cursor.getColumnIndex(DatabaseSchema.DatabaseEntry.COLUMN_NAME_KEY);
                    int valueIndex = cursor.getColumnIndex(DatabaseSchema.DatabaseEntry.COLUMN_NAME_VALUE);
                    int versionIndex = cursor.getColumnIndex(DatabaseSchema.DatabaseEntry.COLUMN_NAME_VERSION);
                    if (keyIndex != -1 && valueIndex != -1 && versionIndex != -1) {
                        if (cursor.moveToFirst()) {
                            do {
                                ((MatrixCursor) cur).addRow(new String[]{cursor.getString(keyIndex), cursor.getString(valueIndex)});
                            } while (cursor.moveToNext());
                        }
                    }
                    cursor.close();
                }

                Log.d(TAG, "QUERY @ finished");

                return cur;

            case "*":

                cur = new MatrixCursor(new String[]{
                        DatabaseSchema.DatabaseEntry.COLUMN_NAME_KEY,
                        DatabaseSchema.DatabaseEntry.COLUMN_NAME_VALUE});

                Log.d(TAG, "QUERY *");

                cursor = localQuery(SimpleDynamoUtils.DATABASE_CONTENT_URL, null, "@", null, null);

                if (cursor != null) {
                    int keyIndex = cursor.getColumnIndex(DatabaseSchema.DatabaseEntry.COLUMN_NAME_KEY);
                    int valueIndex = cursor.getColumnIndex(DatabaseSchema.DatabaseEntry.COLUMN_NAME_VALUE);
                    int versionIndex = cursor.getColumnIndex(DatabaseSchema.DatabaseEntry.COLUMN_NAME_VERSION);
                    if (keyIndex != -1 && valueIndex != -1 && versionIndex != -1) {
                        if (cursor.moveToFirst()) {
                            do {

                                // Put into hashtable firstly
                                decision.put(
                                        cursor.getString(keyIndex),
                                        new Pair<>(cursor.getString(valueIndex), cursor.getInt(versionIndex))
                                );

                            } while (cursor.moveToNext());
                        }
                    }
                    cursor.close();
                }

                for (int n = 0; n < membership.REMOTEAVD.size(); n++) {
                    int forward = membership.REMOTEAVD.get(n) * 2;
                    if (forward == localPort) continue;

                    Message queMsg = new Message(Message.Type.QUERY, "@", null, -1, localPort, forward);

                    // send to everyone and wait response and plus result

                    cursor = sendQuery(queMsg);

                    if (cursor != null) {
                        int keyIndex = cursor.getColumnIndex(DatabaseSchema.DatabaseEntry.COLUMN_NAME_KEY);
                        int valueIndex = cursor.getColumnIndex(DatabaseSchema.DatabaseEntry.COLUMN_NAME_VALUE);
                        int versionIndex = cursor.getColumnIndex(DatabaseSchema.DatabaseEntry.COLUMN_NAME_VERSION);
                        if (keyIndex != -1 && valueIndex != -1 && versionIndex != -1) {
                            if (cursor.moveToFirst()) {
                                do {

                                    // If key not in hashtable, put this in
                                    // Else negotiation the final decision

                                    String k = cursor.getString(keyIndex);
                                    String newU = cursor.getString(valueIndex);
                                    int newV = cursor.getInt(versionIndex);

                                    Pair<String, Integer> p = decision.get(k);
                                    if (p == null) {
                                        decision.put(k, new Pair<>(newU, newV));
                                    } else {

                                        int oldV = p.second;

                                        if (newV > oldV) {
                                            decision.put(k, new Pair<>(newU, newV));
                                        }
                                    }

                                } while (cursor.moveToNext());
                            }
                        }
                        cursor.close();
                    }
                }

                for (String k : decision.keySet()) {
                    Pair<String, Integer> p = decision.get(k);
                    String u = p.first;
                    ((MatrixCursor) cur).addRow(new String[]{k, u});
                }

                return cur;

            default:

                // Step 1: Identify the node that holds the key.
                int[] c = membership.findPreferenceList(key);
                int coordinator = c[0] * 2;
                int replicA = c[1] * 2;
                int replicB = c[2] * 2;

                // When item is not belonging to current node
                // forward the item to node which it belongs to
                // When item is belonging to current node
                // query data from local database firstly
                // query data from replication
                // then decide the final version data.

                int replyNum = 0;
                int decVersion = -1;
                String decKey = "";
                String decValue = "";

                cur = new MatrixCursor(new String[]{
                        DatabaseSchema.DatabaseEntry.COLUMN_NAME_KEY,
                        DatabaseSchema.DatabaseEntry.COLUMN_NAME_VALUE
                });
                Cursor tempCurs;

                if (localPort != coordinator) {

                    Message que = new Message(Message.Type.QUERY, key, null, -1, localPort, coordinator);

                    // Step 2: sending requests to coordinator.

                    Log.d(TAG, "QUERY -- FORWARD:" + coordinator + "KEY:" + key);

                    tempCurs = sendQuery(que);

                    if (tempCurs != null) {

                        replyNum++;

                        int keyIndex = tempCurs.getColumnIndex(DatabaseSchema.DatabaseEntry.COLUMN_NAME_KEY);
                        int valueIndex = tempCurs.getColumnIndex(DatabaseSchema.DatabaseEntry.COLUMN_NAME_VALUE);
                        int versionIndex = tempCurs.getColumnIndex(DatabaseSchema.DatabaseEntry.COLUMN_NAME_VERSION);

                        if (keyIndex != -1 && valueIndex != -1 && versionIndex != -1) {
                            if (tempCurs.moveToFirst()) {
                                if (tempCurs.getInt(versionIndex) > decVersion) {
                                    decKey = tempCurs.getString(keyIndex);
                                    decValue = tempCurs.getString(valueIndex);
                                    decVersion = tempCurs.getInt(versionIndex);
                                }
                            }
                        }

                        tempCurs.close();
                    }

                } else {

                    // Step 2: sending requests to coordinator.

                    tempCurs = localQuery(SimpleDynamoUtils.DATABASE_CONTENT_URL, null, key, null, null);

                    if (tempCurs != null) {

                        replyNum++;

                        int keyIndex = tempCurs.getColumnIndex(DatabaseSchema.DatabaseEntry.COLUMN_NAME_KEY);
                        int valueIndex = tempCurs.getColumnIndex(DatabaseSchema.DatabaseEntry.COLUMN_NAME_VALUE);
                        int versionIndex = tempCurs.getColumnIndex(DatabaseSchema.DatabaseEntry.COLUMN_NAME_VERSION);

                        if (keyIndex != -1 && valueIndex != -1 && versionIndex != -1) {
                            if (tempCurs.moveToFirst()) {
                                if (tempCurs.getInt(versionIndex) > decVersion) {
                                    decKey = tempCurs.getString(keyIndex);
                                    decValue = tempCurs.getString(valueIndex);
                                    decVersion = tempCurs.getInt(versionIndex);
                                }
                            }
                        }

                        tempCurs.close();
                    }

                }

                // Step 3: waiting for response from coordinator
                // In there, wait another R-1 reply from replication
                // If repA success, replyNum ++
                // If repB success, replyNum ++

                Message que1 = new Message(Message.Type.QUERY, key, null, -1, localPort, replicA);

                Log.d(TAG, "QUERY -- REPLICA:" + replicA + "KEY:" + key);

                tempCurs = sendQuery(que1);

                if (tempCurs != null) {

                    replyNum++;

                    int keyIndex = tempCurs.getColumnIndex(DatabaseSchema.DatabaseEntry.COLUMN_NAME_KEY);
                    int valueIndex = tempCurs.getColumnIndex(DatabaseSchema.DatabaseEntry.COLUMN_NAME_VALUE);
                    int versionIndex = tempCurs.getColumnIndex(DatabaseSchema.DatabaseEntry.COLUMN_NAME_VERSION);

                    if (keyIndex != -1 && valueIndex != -1 && versionIndex != -1) {
                        if (tempCurs.moveToFirst()) {

                            if (tempCurs.getInt(versionIndex) > decVersion) {
                                decKey = tempCurs.getString(keyIndex);
                                decValue = tempCurs.getString(valueIndex);
                                decVersion = tempCurs.getInt(versionIndex);
                            }
                        }
                    }

                    tempCurs.close();
                }


                Message que2 = new Message(Message.Type.QUERY, key, null, -1, localPort, replicB);

                Log.d(TAG, "QUERY -- REPLICA:" + replicB + "KEY:" + key);

                tempCurs = sendQuery(que2);

                if (tempCurs != null) {

                    replyNum++;

                    int keyIndex = tempCurs.getColumnIndex(DatabaseSchema.DatabaseEntry.COLUMN_NAME_KEY);
                    int valueIndex = tempCurs.getColumnIndex(DatabaseSchema.DatabaseEntry.COLUMN_NAME_VALUE);
                    int versionIndex = tempCurs.getColumnIndex(DatabaseSchema.DatabaseEntry.COLUMN_NAME_VERSION);

                    if (keyIndex != -1 && valueIndex != -1 && versionIndex != -1) {
                        if (tempCurs.moveToFirst()) {

                            if (tempCurs.getInt(versionIndex) > decVersion) {
                                decKey = tempCurs.getString(keyIndex);
                                decValue = tempCurs.getString(valueIndex);
                                decVersion = tempCurs.getInt(versionIndex);
                            }
                        }
                    }

                    tempCurs.close();
                }


                // Step 4: process reply and repackage response

                if (replyNum >= SimpleDynamoUtils.READ_CONFIRM_NODE_NUM) {

                    // UPDATE 3 device

                    Message u1 = new Message(Message.Type.UPDATE, decKey, decValue, decVersion, localPort, coordinator);
                    Message u2 = new Message(Message.Type.UPDATE, decKey, decValue, decVersion, localPort, replicA);
                    Message u3 = new Message(Message.Type.UPDATE, decKey, decValue, decVersion, localPort, replicB);


                    sendLooper.mHandler.post(new SendThread(u1));
                    sendLooper.mHandler.post(new SendThread(u2));
                    sendLooper.mHandler.post(new SendThread(u3));


                    // repackage cursor to response

                    ((MatrixCursor) cur).addRow(new String[]{
                            decKey,
                            decValue
                    });

                    return cur;

                } else {

                    // Fail to query
                    return null;

                }
        }

    }

    @Override
    public int update(Uri uri, ContentValues values, String selection,
                      String[] selectionArgs) {

        long rows;

        String key = values.getAsString(DatabaseSchema.DatabaseEntry.COLUMN_NAME_KEY);
        String hashKey = SimpleDynamoUtils.genHash(key);
        int version = values.getAsInteger(DatabaseSchema.DatabaseEntry.COLUMN_NAME_VERSION);

        if (version <= getKeyVersion(hashKey)) return 0;

        synchronized (dbLock) {
//            rows = db.update(DatabaseSchema.DatabaseEntry.TABLE_NAME, values, selection + "=?", selectionArgs);
            rows = db.replace(DatabaseSchema.DatabaseEntry.TABLE_NAME, null, values);
        }

        return (int) (long) rows;
    }


    /**
     * query the version of key from database, where the parameter is hash key of selection.
     *
     * @param hashKey hashKey
     * @return version, number of version of key
     */
    private int getKeyVersion(String hashKey) {
        Cursor cur = null;
        int version = 0;
        int versionIndex = 0;

        synchronized (dbLock) {
            cur = db.query(
                    true,
                    DatabaseSchema.DatabaseEntry.TABLE_NAME,
                    new String[]{DatabaseSchema.DatabaseEntry.COLUMN_NAME_VERSION},
                    DatabaseSchema.DatabaseEntry.COLUMN_NAME_HASHKEY + "=?",
                    new String[]{hashKey},
                    null, null, null, null
            );
        }

        if (cur != null) {
            versionIndex = cur.getColumnIndex(DatabaseSchema.DatabaseEntry.COLUMN_NAME_VERSION);
            if (versionIndex != -1) {
                if (cur.moveToFirst()) {
                    version = cur.getInt(versionIndex);
                }
            }
            cur.close();
        }

        return version;
    }

    /**
     * Insert the values into local, where there is the main insert doing.
     *
     * @param uri    Uri
     * @param values ContentValues
     * @return uri
     */
    public Uri localInsert(Uri uri, ContentValues values) {

        String key = values.getAsString(DatabaseSchema.DatabaseEntry.COLUMN_NAME_KEY);
        String value = values.getAsString(DatabaseSchema.DatabaseEntry.COLUMN_NAME_VALUE);
        String hashKey = SimpleDynamoUtils.genHash(key);

        // Increase the version
        int version = getKeyVersion(hashKey) + 1;
        values.put(DatabaseSchema.DatabaseEntry.COLUMN_NAME_VERSION, version);
        values.put(DatabaseSchema.DatabaseEntry.COLUMN_NAME_HASHKEY, hashKey);

        // TODO: Before insert, delete the former version data
        // Probably not delete former version
        // Cuz Update operation will handle it.

        long newRowId;
        synchronized (dbLock) {
            newRowId = db.replace(DatabaseSchema.DatabaseEntry.TABLE_NAME, null, values);
        }

        Log.d(TAG, "Insert:(" + newRowId + ") Version: " + version + " KEY: " + key + " VALUE: " + value + " HASHKEY: " + hashKey);

        return Uri.withAppendedPath(SimpleDynamoUtils.DATABASE_CONTENT_URL, String.valueOf(newRowId));
    }

    /**
     * query in the local
     *
     * @param uri
     * @param projection
     * @param selection
     * @param selectionArgs
     * @param sortOrder
     * @return
     */
    public Cursor localQuery(Uri uri, String[] projection, String selection,
                             String[] selectionArgs, String sortOrder) {

        Cursor cur = null;

        if (selection.equals("@")) {
            synchronized (dbLock) {

                cur = db.query(
                        true,
                        DatabaseSchema.DatabaseEntry.TABLE_NAME,
                        new String[]{
                                DatabaseSchema.DatabaseEntry.COLUMN_NAME_KEY,
                                DatabaseSchema.DatabaseEntry.COLUMN_NAME_HASHKEY,
                                DatabaseSchema.DatabaseEntry.COLUMN_NAME_VALUE,
                                DatabaseSchema.DatabaseEntry.COLUMN_NAME_VERSION
                        },
                        null, null, null, null, null, null
                );
            }
            cur.setNotificationUri(context.getContentResolver(), uri);
        } else {
            String hashKey = SimpleDynamoUtils.genHash(selection);

            synchronized (dbLock) {

                cur = db.query(
                        true,
                        DatabaseSchema.DatabaseEntry.TABLE_NAME,
                        new String[]{
                                DatabaseSchema.DatabaseEntry.COLUMN_NAME_HASHKEY,
                                DatabaseSchema.DatabaseEntry.COLUMN_NAME_KEY,
                                DatabaseSchema.DatabaseEntry.COLUMN_NAME_VALUE,
                                DatabaseSchema.DatabaseEntry.COLUMN_NAME_VERSION
                        },
                        DatabaseSchema.DatabaseEntry.COLUMN_NAME_HASHKEY + "=?",
                        new String[]{hashKey},
                        null, null, null, null
                );
            }
            cur.setNotificationUri(context.getContentResolver(), uri);
        }

        return cur;
    }

    /**
     * Delete in the local
     *
     * @param uri
     * @param selection
     * @param selectionArgs
     * @return
     */
    public int localDelete(Uri uri, String selection, String[] selectionArgs) {

        int i = 0;

        if (selection.equals("@")) {
            synchronized (dbLock) {
                i = db.delete(DatabaseSchema.DatabaseEntry.TABLE_NAME, null, null);
            }
        } else {
            String hashKey = SimpleDynamoUtils.genHash(selection);


            synchronized (dbLock) {
                i = db.delete(DatabaseSchema.DatabaseEntry.TABLE_NAME, DatabaseSchema.DatabaseEntry.COLUMN_NAME_HASHKEY + " = ?", new String[]{hashKey});
            }

            Log.d(TAG, "DELETE Success: KEY:" + selection);

        }

        return i;
    }


    /**
     * Client for sending insert message
     *
     * @param msg Message
     * @return <code>true</code> if success ack; <code>false</code> if failure ack.
     */
    public int sendInsert(Message msg) {

        Socket socket = null;
        int result = -1;
        try {

            // Create socket
            socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), msg.forwardPort);

            socket.setSoTimeout(5000);

            // Create out stream
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());

            // Write message
            out.writeUTF(SimpleDynamoUtils.toJSON(msg));
            out.flush();

            Log.d(TAG, "Sent INSERT to " + msg.forwardPort + ", Waiting for reply...");

            // Wait back
            DataInputStream in = new DataInputStream(socket.getInputStream());

            Message reply = SimpleDynamoUtils.parseJSON(in.readUTF());

            if (reply.msgType.equals(Message.Type.ACK)) {
                Log.d(TAG, "ACK - from " + msg.forwardPort);
                result = reply.version;
            }

            in.close();
            out.close();

        } catch (IOException e) {

            Log.e(TAG, e.toString());
            e.printStackTrace();
            return result;

        } finally {

            // Close socket
            try {
                if (socket != null) socket.close();
            } catch (IOException e) {
                Log.e(TAG, e.toString());
                e.printStackTrace();
            }

        }

        return result;

    }


    /**
     * Client for sending query message
     *
     * @param msg Message
     * @return <code>Cursor</code>
     */
    public Cursor sendQuery(Message msg) {

        Socket socket = null;
        Cursor result = new MatrixCursor(new String[]{
                DatabaseSchema.DatabaseEntry.COLUMN_NAME_KEY,
                DatabaseSchema.DatabaseEntry.COLUMN_NAME_VALUE,
                DatabaseSchema.DatabaseEntry.COLUMN_NAME_VERSION
        });

        try {

            // Create socket
            socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), msg.forwardPort);

            socket.setSoTimeout(5000);

            // Create out stream
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());

            // Write message
            out.writeUTF(SimpleDynamoUtils.toJSON(msg));
            out.flush();

            Log.d(TAG, "Sent QUERY to " + msg.forwardPort + ", Waiting for reply...");

            // Wait back
            DataInputStream in = new DataInputStream(socket.getInputStream());

            Message reply = SimpleDynamoUtils.parseJSON(in.readUTF());

            if (reply.msgType.equals(Message.Type.ACK)) {
                Pair<String, Integer> p;
                Log.d(TAG, "ACK - from " + msg.forwardPort);
                for (String k : reply.batch.keySet()) {
                    p = reply.batch.get(k);
                    ((MatrixCursor) result).addRow(new String[]{k, p.first, p.second.toString()});
                }
            }

            in.close();
            out.close();

        } catch (IOException e) {

            Log.e(TAG, e.toString());
            e.printStackTrace();
            return null;

        } finally {

            // Close socket
            try {
                if (socket != null) socket.close();
            } catch (IOException e) {
                Log.e(TAG, e.toString());
                e.printStackTrace();
            }

        }

        return result;

    }


    /**
     * send delete message
     *
     * @param msg
     * @return <code>boolean</code> delete success or not
     */
    private boolean sendDelete(Message msg) {
        Socket socket = null;

        try {

            // Create socket
            socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), msg.forwardPort);

            socket.setSoTimeout(5000);

            // Create out stream
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());

            // Write message
            out.writeUTF(SimpleDynamoUtils.toJSON(msg));
            out.flush();

            Log.d(TAG, "Sent DELETE to " + msg.forwardPort + ", Waiting for reply...");

            // Wait back
            DataInputStream in = new DataInputStream(socket.getInputStream());

            Message reply = SimpleDynamoUtils.parseJSON(in.readUTF());

            if (reply.msgType.equals(Message.Type.ACK)) {
                Log.d(TAG, "ACK - from " + msg.forwardPort);

                in.close();
                out.close();

                return true;
            }

            in.close();
            out.close();

        } catch (IOException e) {

            Log.e(TAG, e.toString());
            e.printStackTrace();
            return false;

        } finally {

            // Close socket
            try {
                if (socket != null) socket.close();
            } catch (IOException e) {
                Log.e(TAG, e.toString());
                e.printStackTrace();
            }

        }

        return false;
    }

    public void enQueue(String key, String value, String type, int version, int port) {
        int id = enQueueCounter.incrementAndGet();
        ContentValues cv = new ContentValues();
        cv.put(DatabaseSchema.QueueEntry.COLUMN_NAME_ID, id);
        cv.put(DatabaseSchema.QueueEntry.COLUMN_NAME_KEY, key);
        cv.put(DatabaseSchema.QueueEntry.COLUMN_NAME_TYPE, type);
        cv.put(DatabaseSchema.QueueEntry.COLUMN_NAME_VALUE, value);
        cv.put(DatabaseSchema.QueueEntry.COLUMN_NAME_VERSION, version);
        cv.put(DatabaseSchema.QueueEntry.COLUMN_NAME_PORT, port);
        synchronized (dbLock) {
            db.replace(DatabaseSchema.QueueEntry.TABLE_NAME, null, cv);
        }
        Log.d(TAG, "TYPE:" + type + " KEY:" + key + " PORT:" + port + " INSERT IN QUEUE");
        return;
    }

    public Message outQueue() {
        Message msg = null;
        synchronized (dbLock) {
            Cursor cursor = db.query(
                    DatabaseSchema.QueueEntry.TABLE_NAME,
                    new String[]{
                            DatabaseSchema.QueueEntry.COLUMN_NAME_ID,
                            DatabaseSchema.QueueEntry.COLUMN_NAME_KEY,
                            DatabaseSchema.QueueEntry.COLUMN_NAME_TYPE,
                            DatabaseSchema.QueueEntry.COLUMN_NAME_VALUE,
                            DatabaseSchema.QueueEntry.COLUMN_NAME_VERSION,
                            DatabaseSchema.QueueEntry.COLUMN_NAME_PORT
                    },
                    null, null, null, null,
                    DatabaseSchema.QueueEntry.COLUMN_NAME_ID + " ASC",
                    "1"
            );

            if (cursor != null) {
                int idIndex = cursor.getColumnIndex(DatabaseSchema.QueueEntry.COLUMN_NAME_ID);
                int typeIndex = cursor.getColumnIndex(DatabaseSchema.QueueEntry.COLUMN_NAME_TYPE);
                int keyIndex = cursor.getColumnIndex(DatabaseSchema.QueueEntry.COLUMN_NAME_KEY);
                int valueIndex = cursor.getColumnIndex(DatabaseSchema.QueueEntry.COLUMN_NAME_VALUE);
                int versionIndex = cursor.getColumnIndex(DatabaseSchema.QueueEntry.COLUMN_NAME_VERSION);
                int portIndex = cursor.getColumnIndex(DatabaseSchema.QueueEntry.COLUMN_NAME_PORT);
                if (keyIndex != -1 && valueIndex != -1 && versionIndex != -1) {
                    if (cursor.moveToFirst()) {
                        msg = new Message(
                                cursor.getString(typeIndex),
                                cursor.getString(keyIndex),
                                cursor.getString(valueIndex),
                                cursor.getInt(versionIndex),
                                localPort,
                                cursor.getInt(portIndex)
                        );
                        int id = cursor.getInt(idIndex);
                        db.delete(DatabaseSchema.QueueEntry.TABLE_NAME,
                                DatabaseSchema.QueueEntry.COLUMN_NAME_ID + " = ?",
                                new String[]{String.valueOf(id)});

                        Log.d(TAG, "TYPE:" + msg.msgType + " KEY:" + msg.key + " PORT:" + msg.forwardPort + " OUT OF QUEUE");
                    }
                }
                cursor.close();
            }
        }

        return msg;
    }

    /**
     * Socket Server
     */
    public class Server implements Runnable {

        Context context;

        public Server(Context c) {
            this.context = c;
        }

        @Override
        public void run() {

            // Create server socket
            ServerSocket serverSocket = null;
            Socket clientSocket = null;
            try {
                serverSocket = new ServerSocket(SimpleDynamoUtils.SERVER_PORT);
            } catch (IOException e) {
                Log.e(TAG, e.toString());
                e.printStackTrace();
            }

            while (true) {
                try {

                    clientSocket = serverSocket.accept();

                    DataInputStream input = new DataInputStream(clientSocket.getInputStream());

                    Message msg = SimpleDynamoUtils.parseJSON(input.readUTF());

                    Log.d(TAG, "Received Message: " + msg.msgType + ", From: " + msg.originPort);

                    switch (msg.msgType) {

                        case Message.Type.INSERT:

                            processLooper.mHandler.post(new OnReceiveInsert(clientSocket, msg));

                            break;

                        case Message.Type.REPLICA:

                            Message replicaAck = new Message(Message.Type.ACK, null, null, -1, -1, -1);

                            try {
                                clientSocket.setSoTimeout(1000);
                                // Create out stream
                                DataOutputStream iOut = new DataOutputStream(clientSocket.getOutputStream());
                                // Write message
                                iOut.writeUTF(SimpleDynamoUtils.toJSON(replicaAck));
                                iOut.flush();
                            } catch (IOException e) {
                                e.printStackTrace();
                            }

                            Log.d(TAG, "REPLICA Reply sent");

                            processLooper.mHandler.post(new OnReceiveReplica(clientSocket, msg));

                            break;

                        case Message.Type.DELETE:

                            Message deleteAck = new Message(Message.Type.ACK, null, null, -1, -1, -1);

                            try {
                                // Create out stream
                                DataOutputStream iOut = new DataOutputStream(clientSocket.getOutputStream());
                                // Write message
                                iOut.writeUTF(SimpleDynamoUtils.toJSON(deleteAck));
                                iOut.flush();
                            } catch (IOException e) {
                                e.printStackTrace();
                            }

                            Log.d(TAG, "DELETE Reply sent");

                            processLooper.mHandler.post(new OnReceiveDelete(clientSocket, msg));

                            break;

                        case Message.Type.QUERY:

                            processLooper.mHandler.post(new OnReceiveQuery(clientSocket, msg));

                            break;

                        case Message.Type.UPDATE:

                            Message updateAck = new Message(Message.Type.ACK, null, null, -1, -1, -1);

                            try {
                                // Create out stream
                                DataOutputStream iOut = new DataOutputStream(clientSocket.getOutputStream());
                                // Write message
                                iOut.writeUTF(SimpleDynamoUtils.toJSON(updateAck));
                                iOut.flush();
                            } catch (IOException e) {
                                e.printStackTrace();
                            }

                            Log.d(TAG, "DELETE Reply sent");

                            processLooper.mHandler.post(new OnReceiveUpdate(clientSocket, msg));

                            break;

                        default:

                            input.close();
                            clientSocket.close();

                            break;
                    }

                } catch (NullPointerException | IOException e) {
                    Log.e(TAG, e.toString());
                    e.printStackTrace();
                }

            }
        }

        private String TAG = localPort + " " + Server.class.getSimpleName();


    }

    public class OnReceiveInsert implements Runnable {

        Socket socket;
        String key;
        String value;

        public OnReceiveInsert(Socket s, Message msg) {
            this.socket = s;
            this.key = msg.key;
            this.value = msg.value;
        }

        @Override
        public void run() {

            String hashKey = SimpleDynamoUtils.genHash(key);
            int v = getKeyVersion(hashKey);

            Message insertAck = new Message(Message.Type.ACK, null, null, v, -1, -1);

            try {
                // Create out stream
                DataOutputStream iOut = new DataOutputStream(socket.getOutputStream());
                // Write message
                iOut.writeUTF(SimpleDynamoUtils.toJSON(insertAck));
                iOut.flush();
            } catch (IOException e) {
                e.printStackTrace();
            }

            Log.d(TAG, "INSERT Reply sent");

        }
    }

    public class OnReceiveReplica implements Runnable {

        Socket socket;
        String key;
        String value;

        public OnReceiveReplica(Socket s, Message msg) {
            this.socket = s;
            this.key = msg.key;
            this.value = msg.value;
        }

        @Override
        public void run() {
            ContentValues cv = new ContentValues();
            cv.put(DatabaseSchema.DatabaseEntry.COLUMN_NAME_KEY, key);
            cv.put(DatabaseSchema.DatabaseEntry.COLUMN_NAME_VALUE, value);

            localInsert(SimpleDynamoUtils.DATABASE_CONTENT_URL, cv);

            Log.d(TAG, "REPLICA " + key + " in " + localPort + ", send back ACK");

        }
    }

    public class OnReceiveQuery implements Runnable {

        Socket socket;
        String key;

        public OnReceiveQuery(Socket s, Message msg) {
            this.socket = s;
            this.key = msg.key;
        }

        @Override
        public void run() {
            Cursor cur = localQuery(SimpleDynamoUtils.DATABASE_CONTENT_URL, null, key, null, null);

            Log.d(TAG, "QUERY " + key + " in " + localPort + ", send back ACK");

            Message reply = new Message(Message.Type.ACK, null, null, -1, -1, -1);

            if (cur != null) {

                int keyIndex = cur.getColumnIndex(DatabaseSchema.DatabaseEntry.COLUMN_NAME_KEY);
                int valueIndex = cur.getColumnIndex(DatabaseSchema.DatabaseEntry.COLUMN_NAME_VALUE);
                int versionIndex = cur.getColumnIndex(DatabaseSchema.DatabaseEntry.COLUMN_NAME_VERSION);

                if (keyIndex != -1 && valueIndex != -1 && versionIndex != -1) {
                    if (cur.moveToFirst()) {
                        do {
                            Pair<String, Integer> v = new Pair<>(cur.getString(valueIndex), cur.getInt(versionIndex));
                            reply.batch.put(cur.getString(keyIndex), v);
                        } while (cur.moveToNext());
                    }
                }
                cur.close();
            }

            try {
                socket.setSoTimeout(1000);
                // Create out stream
                DataOutputStream iOut = new DataOutputStream(socket.getOutputStream());
                // Write message
                iOut.writeUTF(SimpleDynamoUtils.toJSON(reply));
                iOut.flush();
            } catch (IOException e) {
                e.printStackTrace();
            }

            Log.d(TAG, "QUERY Reply sent");
        }
    }

    public class OnReceiveUpdate implements Runnable {

        Socket socket;
        String key;
        String value;
        String hashKey;
        int version;

        public OnReceiveUpdate(Socket s, Message msg) {
            this.socket = s;
            this.key = msg.key;
            this.hashKey = SimpleDynamoUtils.genHash(key);
            this.value = msg.value;
            this.version = msg.version;
        }

        @Override
        public void run() {

            try {
                socket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

            ContentValues up = new ContentValues();
            up.put(DatabaseSchema.DatabaseEntry.COLUMN_NAME_KEY, key);
            up.put(DatabaseSchema.DatabaseEntry.COLUMN_NAME_HASHKEY, hashKey);
            up.put(DatabaseSchema.DatabaseEntry.COLUMN_NAME_VALUE, value);
            up.put(DatabaseSchema.DatabaseEntry.COLUMN_NAME_VERSION, version);

            update(SimpleDynamoUtils.DATABASE_CONTENT_URL, up,
                    DatabaseSchema.DatabaseEntry.COLUMN_NAME_HASHKEY,
                    new String[]{hashKey});
        }
    }

    public class OnReceiveDelete implements Runnable {

        Socket socket;
        String key;

        public OnReceiveDelete(Socket s, Message msg) {
            this.socket = s;
            this.key = msg.key;
        }

        @Override
        public void run() {
            localDelete(SimpleDynamoUtils.DATABASE_CONTENT_URL, key, null);

            Log.d(TAG, "DELETE " + key + " in " + localPort + ", send back ACK");

        }
    }

    /**
     * Send Thread
     */
    public class SendThread implements Runnable {

        Message msg;
        Socket socket = null;
        private String TAG = SendThread.class.getSimpleName();

        public SendThread(Message m) {
            this.msg = m;
        }

        @Override
        public void run() {
            try {

                // Create socket
                socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), msg.forwardPort);

                // Create out stream
                DataOutputStream out = new DataOutputStream(socket.getOutputStream());

                // Write message
                out.writeUTF(SimpleDynamoUtils.toJSON(msg));
                out.flush();

                Log.d(TAG, "Send from Client Thread");

                Log.d(TAG, "Sent " + msg.msgType + " to " + msg.forwardPort + ", Waiting for reply...");

                // Wait back
                DataInputStream in = new DataInputStream(socket.getInputStream());

                Message reply = SimpleDynamoUtils.parseJSON(in.readUTF());

                if (reply.msgType.equals(Message.Type.ACK)) {
                    Log.d(TAG, "ACK - from " + msg.forwardPort);
                }

                in.close();
                out.close();

            } catch (IOException e) {
                Log.e(TAG, "TIMEOUT ABOUT SENDING " + msg.msgType + " TO " + msg.forwardPort);
                Log.e(TAG, e.toString());
                enQueue(msg.key, msg.value, msg.msgType, msg.version, msg.forwardPort);
            } finally {

                // Close socket
                try {
                    if (socket != null) socket.close();
                } catch (IOException e) {
                    Log.e(TAG, e.toString());
                    e.printStackTrace();
                }

            }
        }
    }

    public class DealerWithFailure implements Runnable {

        @Override
        public void run() {
            while (true) {
                Message retry = outQueue();
                if (retry != null) {
                    processLooper.mHandler.post(new SendThread(retry));
                }

                try {
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

    }
}
