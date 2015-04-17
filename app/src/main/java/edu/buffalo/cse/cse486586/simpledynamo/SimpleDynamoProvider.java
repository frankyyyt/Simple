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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Hashtable;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import edu.buffalo.cse.cse486586.simpledynamo.Client.SendThread;
import edu.buffalo.cse.cse486586.simpledynamo.Common.LooperThread;
import edu.buffalo.cse.cse486586.simpledynamo.Common.Membership;
import edu.buffalo.cse.cse486586.simpledynamo.Common.Message;
import edu.buffalo.cse.cse486586.simpledynamo.Store.DatabaseHelper;
import edu.buffalo.cse.cse486586.simpledynamo.Store.DatabaseSchema;
import edu.buffalo.cse.cse486586.simpledynamo.Utils.SimpleDynamoUtils;

public class SimpleDynamoProvider extends ContentProvider {

    private static String TAG = SimpleDynamoProvider.class.getSimpleName();

    private static int localPort;
    private final Object dbLock = new Object(); // lock for database
    Context context; // application context
    DatabaseHelper dbHlp; // database helper
    SQLiteDatabase db; // database object
    private Membership membership;
    private LooperThread sendLooper;
    private LooperThread processLooper;
    private ExecutorService requestExecPool;


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
     *
     */
    public void quit() {
        sendLooper.quit();
        processLooper.quit();
        requestExecPool.shutdown();
    }

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
        int r;
        r = requestCoordination(2, selection);
		return r;
	}

	@Override
	public String getType(Uri uri) {
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {

        Uri uriReturn;
        uriReturn = requestCoordination(0, values);

		return uriReturn;
	}

	@Override
	public boolean onCreate() {

        context = getContext();
        dbHlp = new DatabaseHelper(context);
        db = dbHlp.getWritableDatabase();

        localPort = getLocalPort();

        membership = new Membership();
        sendLooper = new LooperThread();
        processLooper = new LooperThread();
        requestExecPool = Executors.newCachedThreadPool();


        sendLooper.start();
        processLooper.start();

        // Start thread listening on port
        new Thread(new Server(context)).start();

        return true;
    }

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {

        Cursor cur;
        cur = requestCoordination(1, selection);
		return cur;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {

        int rows;

        synchronized (dbLock) {
            rows = db.update(DatabaseSchema.DatabaseEntry.TABLE_NAME, values, selection + "=?", selectionArgs);
        }

        return rows;
    }

    private <P, R> R requestCoordination(int flag, P parameters) {

        // There are 4 states for request coordination:
        // 1. Identify the node that holds the key.
        // 2. Sending request to coordinator.
        // 3. Wait for response from coordinator.
        // 4. Process reply and repackage response.


        // When request is insert(flag=0), the type P is ContentValues
        // When request is query(flag=1), the type P is String
        // When request is delete(flag=2), the type P is String

        switch (flag) {
            case 0:
                Uri u = null;
                Callable<Uri> insert = new ProcessInsert((ContentValues)parameters);
                Future<Uri> ins = requestExecPool.submit(insert);
                try {
                    u = ins.get();
                } catch (InterruptedException | ExecutionException e) {
                    Log.e(TAG, "insert error");
                    e.printStackTrace();
                }
                return (R)u;
            case 1:
                Cursor cur = null;
                Callable<Cursor> query = new ProcessQuery(String.valueOf(parameters));
                Future<Cursor> que = requestExecPool.submit(query);
                try {
                    cur = que.get();
                } catch (InterruptedException | ExecutionException e) {
                    Log.e(TAG, "query error");
                    e.printStackTrace();
                }
                return (R)cur;
            case 2:
                int i = 0;
                Callable<Integer> delete = new ProcessDelete(String.valueOf(parameters));
                Future<Integer> del = requestExecPool.submit(delete);
                try {
                    i = del.get();
                } catch (InterruptedException | ExecutionException e) {
                    Log.e(TAG, "delete error");
                    e.printStackTrace();
                }
                return (R)Integer.valueOf(i);
            default:
                return null;
        }

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

        // TODO: Before insert, delete the former version data
        // Probably not delete former version
        // Cuz Update operation will handle it.

        long newRowId;
        synchronized (dbLock) {
            newRowId = db.insertOrThrow(DatabaseSchema.DatabaseEntry.TABLE_NAME, null, values);
        }

        Log.d(TAG, "Insert:(" + newRowId + ") Version: " + version + " KEY: " + key + " VALUE: " + value);

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
                                DatabaseSchema.DatabaseEntry.COLUMN_NAME_HASHKEY,
                                DatabaseSchema.DatabaseEntry.COLUMN_NAME_KEY,
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
                i = delete(uri, selection, null);
            }
        } else {
            String hashKey = SimpleDynamoUtils.genHash(selection);

            synchronized (dbLock) {
                i = delete(uri, DatabaseSchema.DatabaseEntry.COLUMN_NAME_HASHKEY, new String[]{hashKey});
            }
        }

        return i;
    }

    /**
     * Client for sending insert message
     *
     * @param msg Message
     * @return <code>true</code> if success ack; <code>false</code> if failure ack.
     */
    public boolean sendInsert(Message msg) {

        Socket socket = null;
        boolean result = false;
        try {

            // Create socket
            socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), msg.forwardPort);

            socket.setSoTimeout(500);

            // Create out stream
            ObjectOutputStream out = new ObjectOutputStream(
                    new BufferedOutputStream(socket.getOutputStream()));

            // Write message
            out.writeObject(msg);
            out.flush();

            Log.d(TAG, "Sent to " + msg.forwardPort + ", Waiting for reply...");

            // Wait back
            ObjectInputStream in = new ObjectInputStream(
                    new BufferedInputStream(socket.getInputStream()));

            Message reply = (Message) in.readObject();

            if (reply.msgType == Message.type.ACK) {
                Log.d(TAG, "ACK - from " + msg.forwardPort);
                result = true;
            }

            in.close();
            out.close();

        } catch (ClassNotFoundException | IOException e) {

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

            socket.setSoTimeout(500);

            // Create out stream
            ObjectOutputStream out = new ObjectOutputStream(
                    new BufferedOutputStream(socket.getOutputStream()));

            // Write message
            out.writeObject(msg);
            out.flush();

            Log.d(TAG, "Sent to " + msg.forwardPort + ", Waiting for reply...");

            // Wait back
            ObjectInputStream in = new ObjectInputStream(
                    new BufferedInputStream(socket.getInputStream()));

            Message reply = (Message) in.readObject();

            if (reply.msgType == Message.type.ACK) {
                Pair<String, Integer> p;
                Log.d(TAG, "ACK - from " + msg.forwardPort);
                for (String k : reply.batch.keySet()) {
                    p = reply.batch.get(k);
                    ((MatrixCursor) result).addRow(new String[]{k, p.first, p.second.toString()});
                }
            }

            in.close();
            out.close();

        } catch (ClassNotFoundException | IOException e) {

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

            socket.setSoTimeout(500);

            // Create out stream
            ObjectOutputStream out = new ObjectOutputStream(
                    new BufferedOutputStream(socket.getOutputStream()));

            // Write message
            out.writeObject(msg);
            out.flush();

            Log.d(TAG, "Sent DELETE to " + msg.forwardPort + ", Waiting for reply...");

            // Wait back
            ObjectInputStream in = new ObjectInputStream(
                    new BufferedInputStream(socket.getInputStream()));

            Message reply = (Message) in.readObject();

            if (reply.msgType == Message.type.ACK) {
                Log.d(TAG, "ACK - from " + msg.forwardPort);

                in.close();
                out.close();

                return true;
            }

            in.close();
            out.close();

        } catch (ClassNotFoundException | IOException e) {

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


    /**
     * Class of Callable function
     */
    private class ProcessInsert implements Callable<Uri> {

        ContentValues values;

        public ProcessInsert(ContentValues cv) {
            this.values = cv;
        }

        @Override
        public Uri call() throws Exception {
            Uri u = null;
            String key = values.getAsString(DatabaseSchema.DatabaseEntry.COLUMN_NAME_KEY);
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

            if (localPort != coordinator) {

                Message ins = new Message();
                ins.forwardPort = coordinator;
                ins.msgType = Message.type.INSERT;
                ins.key = key;
                ins.value = value;

                // Step 2: sending requests to coordinator.

                // sendLooper.mHandler.post(new SendThread(ins));
                Log.d(TAG, "INSERT -- FORWARD:" + coordinator + " KEY:" + key);
                if (sendInsert(ins)) {
                    replyNum++;
                    Log.d(TAG, ins.forwardPort + " successfully insert");
                } else {
                    Log.e(TAG, ins.forwardPort + " fail to insert");
                }

            } else {

                // Step 2: sending requests to coordinator.

                u = localInsert(SimpleDynamoUtils.DATABASE_CONTENT_URL, values);
                replyNum = (u != null) ? replyNum + 1 : replyNum;
                Log.d(TAG, "INSERT -- LOCAL:" + localPort + " KEY:" + key);

            }

            // Step 3: waiting for response from coordinator
            // In there, wait another W-1 reply from replication
            // If repA success, replyNum ++
            // If repB success, replyNum ++

            // Replication
            Message repA = new Message();
            repA.msgType = Message.type.REPLICA;
            repA.forwardPort = replicA;
            repA.key = key;
            repA.value = value;

            Log.d(TAG, "REPLICA -- TO:" + replicA + "KEY:" + key);
            if (sendInsert(repA)) {
                replyNum++;
                Log.d(TAG, repA.forwardPort + " successfully replication");
            } else {
                Log.e(TAG, repA.forwardPort + " fail to replication");
            }

            // Replication
            Message repB = new Message();
            repA.msgType = Message.type.REPLICA;
            repB.forwardPort = replicB;
            repB.key = key;
            repB.value = value;

            Log.d(TAG, "REPLICA -- TO:" + replicB + "KEY:" + key);
            if (sendInsert(repB)) {
                replyNum++;
                Log.d(TAG, repB.forwardPort + " successfully replication");
            } else {
                Log.e(TAG, repB.forwardPort + " fail to replication");
            }

            // Step 4: process reply and repackage response
            return (replyNum >= SimpleDynamoUtils.WRITE_CONFIRM_NODE_NUM) ? u : null;
        }
    }


    /**
     * Class of Callable function to handle query
     *
     */
    private class ProcessQuery implements Callable<Cursor> {

        String key;
        String hashKey;
        Hashtable<String, Pair<String, Integer>> decision = new Hashtable<String, Pair<String, Integer>>() {
        };

        public ProcessQuery(String selection) {
            key = (selection != null && selection.contains("\"")) ? selection.substring(1, selection.length() - 1) : selection;
            hashKey = (key != null) ? SimpleDynamoUtils.genHash(key) : "";
        }

        @Override
        public Cursor call() {
            Cursor cur = null;

            switch (key) {
                case "@":

                    Log.d(TAG, "QUERY @");

                    cur = localQuery(SimpleDynamoUtils.DATABASE_CONTENT_URL, null, "@", null, null);

                    Log.d(TAG, "QUERY @ finished");

                    return cur;

                case "*":

                    Cursor cursor = null;
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
                                            new Pair<String, Integer>(cursor.getString(valueIndex),
                                                    cursor.getInt(versionIndex))
                                    );
                                    // ((MatrixCursor) cur).addRow(new String[]{
                                    //       cursor.getString(keyIndex), cursor.getString(valueIndex)});

                                } while (cursor.moveToNext());
                            }
                        }
                        cursor.close();
                    }

                    for (int n = 0; n < membership.REMOTEAVD.size(); n++) {
                        int forward = membership.REMOTEAVD.get(n) * 2;
                        if (forward == localPort) continue;

                        Message queMsg = new Message();
                        queMsg.msgType = Message.type.QUERY;
                        queMsg.key = "\"@\"";
                        queMsg.forwardPort = forward;

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
                                            decision.put(k, new Pair<String, Integer>(newU, newV));
                                        } else {

                                            int oldV = p.second;

                                            if (newV > oldV) {
                                                decision.put(k, new Pair<String, Integer>(newU, newV));
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
                    Message decision = new Message();
                    decision.version = -1;
                    cur = new MatrixCursor(new String[]{
                            DatabaseSchema.DatabaseEntry.COLUMN_NAME_KEY,
                            DatabaseSchema.DatabaseEntry.COLUMN_NAME_VALUE
                    });
                    Cursor tempCurs;

                    if (localPort != coordinator) {

                        Message ins = new Message();
                        ins.forwardPort = coordinator;
                        ins.msgType = Message.type.QUERY;
                        ins.key = key;

                        // Step 2: sending requests to coordinator.

                        // sendLooper.mHandler.post(new SendThread(ins));
                        Log.d(TAG, "QUERY -- FORWARD:" + coordinator + "KEY:" + key);

                        tempCurs = sendQuery(ins);

                        if (tempCurs != null) {

                            replyNum++;

                            int keyIndex = tempCurs.getColumnIndex(DatabaseSchema.DatabaseEntry.COLUMN_NAME_KEY);
                            int valueIndex = tempCurs.getColumnIndex(DatabaseSchema.DatabaseEntry.COLUMN_NAME_VALUE);
                            int versionIndex = tempCurs.getColumnIndex(DatabaseSchema.DatabaseEntry.COLUMN_NAME_VERSION);

                            if (keyIndex != -1 && valueIndex != -1 && versionIndex != -1) {
                                if (tempCurs.moveToFirst()) {
                                    if (tempCurs.getInt(versionIndex) > decision.version) {
                                        decision.key = tempCurs.getString(keyIndex);
                                        decision.value = tempCurs.getString(valueIndex);
                                        decision.version = tempCurs.getInt(versionIndex);
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
                                    if (tempCurs.getInt(versionIndex) > decision.version) {
                                        decision.key = tempCurs.getString(keyIndex);
                                        decision.value = tempCurs.getString(valueIndex);
                                        decision.version = tempCurs.getInt(versionIndex);
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

                    Message que1 = new Message();
                    que1.forwardPort = replicA;
                    que1.msgType = Message.type.QUERY;
                    que1.key = key;

                    Log.d(TAG, "QUERY -- REPLICA:" + replicA + "KEY:" + key);

                    tempCurs = sendQuery(que1);

                    if (tempCurs != null) {

                        replyNum++;

                        int keyIndex = tempCurs.getColumnIndex(DatabaseSchema.DatabaseEntry.COLUMN_NAME_KEY);
                        int valueIndex = tempCurs.getColumnIndex(DatabaseSchema.DatabaseEntry.COLUMN_NAME_VALUE);
                        int versionIndex = tempCurs.getColumnIndex(DatabaseSchema.DatabaseEntry.COLUMN_NAME_VERSION);

                        if (keyIndex != -1 && valueIndex != -1 && versionIndex != -1) {
                            if (tempCurs.moveToFirst()) {

                                if (tempCurs.getInt(versionIndex) > decision.version) {
                                    decision.key = tempCurs.getString(keyIndex);
                                    decision.value = tempCurs.getString(valueIndex);
                                    decision.version = tempCurs.getInt(versionIndex);
                                }
                            }
                        }

                        tempCurs.close();
                    }


                    Message que2 = new Message();
                    que2.forwardPort = replicB;
                    que2.msgType = Message.type.QUERY;
                    que2.key = key;

                    Log.d(TAG, "QUERY -- REPLICA:" + replicB + "KEY:" + key);

                    tempCurs = sendQuery(que2);

                    if (tempCurs != null) {

                        replyNum++;

                        int keyIndex = tempCurs.getColumnIndex(DatabaseSchema.DatabaseEntry.COLUMN_NAME_KEY);
                        int valueIndex = tempCurs.getColumnIndex(DatabaseSchema.DatabaseEntry.COLUMN_NAME_VALUE);
                        int versionIndex = tempCurs.getColumnIndex(DatabaseSchema.DatabaseEntry.COLUMN_NAME_VERSION);

                        if (keyIndex != -1 && valueIndex != -1 && versionIndex != -1) {
                            if (tempCurs.moveToFirst()) {

                                if (tempCurs.getInt(versionIndex) > decision.version) {
                                    decision.key = tempCurs.getString(keyIndex);
                                    decision.value = tempCurs.getString(valueIndex);
                                    decision.version = tempCurs.getInt(versionIndex);
                                }
                            }
                        }

                        tempCurs.close();
                    }


                    // Step 4: process reply and repackage response

                    if (replyNum >= SimpleDynamoUtils.READ_CONFIRM_NODE_NUM) {

                        // UPDATE 3 device

                        Message u1 = new Message();
                        u1.msgType = Message.type.UPDATE;
                        u1.forwardPort = coordinator;
                        u1.key = decision.key;
                        u1.value = decision.value;
                        u1.version = decision.version;

                        Message u2 = new Message();
                        u2.msgType = Message.type.UPDATE;
                        u2.forwardPort = coordinator;
                        u2.key = decision.key;
                        u2.value = decision.value;
                        u2.version = decision.version;

                        Message u3 = new Message();
                        u3.msgType = Message.type.UPDATE;
                        u3.forwardPort = coordinator;
                        u3.key = decision.key;
                        u3.value = decision.value;
                        u3.version = decision.version;

                        sendLooper.mHandler.post(new SendThread(u1));
                        sendLooper.mHandler.post(new SendThread(u2));
                        sendLooper.mHandler.post(new SendThread(u3));


                        // repackage cursor to response

                        ((MatrixCursor) cur).addRow(new String[]{
                                decision.key,
                                decision.value
                        });

                        return cur;

                    } else {

                        // Fail to query
                        return null;

                    }
            }
        }
    }

    private class ProcessDelete implements Callable<Integer> {

        String key;
        String hashKey;

        public ProcessDelete(String selection) {
            key = (selection != null && selection.contains("\"")) ? selection.substring(1, selection.length() - 1) : selection;
            hashKey = (key != null) ? SimpleDynamoUtils.genHash(key) : "";
        }

        @Override
        public Integer call() {
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
                        int rows = 0;
                        rows = db.delete(DatabaseSchema.DatabaseEntry.TABLE_NAME, null, null);
                        i += rows;
                    }

                    for (int n = 0; n < membership.REMOTEAVD.size(); n++) {
                        int forward = membership.REMOTEAVD.get(n) * 2;
                        if (forward == localPort) continue;

                        Message delMsg = new Message();
                        delMsg.msgType = Message.type.DELETE;
                        delMsg.key = "\"@\"";
                        delMsg.forwardPort = forward;

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

                    int replyNum = 0;

                    if (localPort != coordinator) {

                        Message del = new Message();
                        del.forwardPort = coordinator;
                        del.msgType = Message.type.DELETE;
                        del.key = key;

                        // Step 2: sending requests to coordinator.
                        // sendLooper.mHandler.post(new SendThread(ins));
                        Log.d(TAG, "DELETE -- FORWARD:" + coordinator + "KEY:" + key);


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
                    // Step 4: process reply and repackage response
                    return (replyNum >= SimpleDynamoUtils.READ_CONFIRM_NODE_NUM) ? i : 0;
            }
        }
    }

    /**
     * Socket Server
     */
    public class Server implements Runnable {

        Context context;
        private String TAG = Server.class.getSimpleName();

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
                    clientSocket.setSoTimeout(500);

                    ObjectInputStream input = new ObjectInputStream(
                            new BufferedInputStream(clientSocket.getInputStream()));

                    Message msg = (Message) input.readObject();

                    Log.d(TAG, "Received Message: " + msg.msgType + ", From: " + msg.originPort);

                    String key;
                    String hashKey;
                    String value;
                    int version;
                    Message reply;

                    switch (msg.msgType) {

                        case INSERT:

                            key = msg.key;
                            value = msg.value;

                            ContentValues cv = new ContentValues();
                            cv.put(DatabaseSchema.DatabaseEntry.COLUMN_NAME_KEY, key);
                            cv.put(DatabaseSchema.DatabaseEntry.COLUMN_NAME_VALUE, value);

                            Uri u = localInsert(SimpleDynamoUtils.DATABASE_CONTENT_URL, cv);

                            Log.d(TAG, "INSERT " + key + " in " + localPort + ", send back ACK");


                            reply = new Message();
                            reply.msgType = Message.type.ACK;

                            // Create out stream
                            ObjectOutputStream iOut = new ObjectOutputStream(
                                    new BufferedOutputStream(clientSocket.getOutputStream()));

                            // Write message
                            iOut.writeObject(reply);
                            iOut.flush();

                            Log.d(TAG, "Reply sent");

                            break;

                        case DELETE:


                            // For Delete
                            // Situation 1:
                            // When received key == "@", then delete locally, reply the ACK with
                            // deleted rows and successor;
                            //
                            // Situation 2:
                            // When received key == particular, then delete and return the rows.

                            key = msg.key;

                            int rows = localDelete(SimpleDynamoUtils.DATABASE_CONTENT_URL, key, null);

                            reply = new Message();
                            reply.msgType = Message.type.ACK;
                            reply.version = rows;

                            // Create out stream
                            ObjectOutputStream dOut = new ObjectOutputStream(
                                    new BufferedOutputStream(clientSocket.getOutputStream()));

                            // Write message
                            dOut.writeObject(reply);
                            dOut.flush();

                            Log.d(TAG, "Reply finished");

                            break;

                        case QUERY:

                            key = msg.key;

                            reply = new Message();
                            reply.msgType = Message.type.ACK;

                            Cursor cur = localQuery(SimpleDynamoUtils.DATABASE_CONTENT_URL, null, key, null, null);

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

                            // Create out stream
                            ObjectOutputStream qOut = new ObjectOutputStream(
                                    new BufferedOutputStream(clientSocket.getOutputStream()));

                            // Write message
                            qOut.writeObject(reply);
                            qOut.flush();

                            Log.d(TAG, "Reply finished");

                            break;

                        case UPDATE:

                            key = msg.key;
                            hashKey = SimpleDynamoUtils.genHash(key);
                            value = msg.value;
                            version = msg.version;

                            ContentValues c = new ContentValues();
                            c.put(DatabaseSchema.DatabaseEntry.COLUMN_NAME_KEY, key);
                            c.put(DatabaseSchema.DatabaseEntry.COLUMN_NAME_HASHKEY, hashKey);
                            c.put(DatabaseSchema.DatabaseEntry.COLUMN_NAME_VALUE, value);
                            c.put(DatabaseSchema.DatabaseEntry.COLUMN_NAME_VERSION, version);

                            update(SimpleDynamoUtils.DATABASE_CONTENT_URL, c,
                                    DatabaseSchema.DatabaseEntry.COLUMN_NAME_HASHKEY,
                                    new String[]{hashKey});

                            break;

                        default:

                            input.close();
                            clientSocket.close();

                            break;

                    }


                } catch (NullPointerException | ClassNotFoundException | IOException e) {
                    Log.e(TAG, e.toString());
                    e.printStackTrace();
                }

            }
        }
    }
}
