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
    private static String localId;

    Context context; // application context
    DatabaseHelper dbHlp; // database helper
    SQLiteDatabase db; // database object
    private Object dbLock = new Object(); // lock for database

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
        int r = 0;
        r = requestCoordination(2, selection);
		return r;
	}

	@Override
	public String getType(Uri uri) {
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {

        Uri uriReturn  = null;
        uriReturn = requestCoordination(0, values);

		return uriReturn;
	}

	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub

        context = getContext();
        dbHlp = new DatabaseHelper(context);
        db = dbHlp.getWritableDatabase();

        localPort = getLocalPort();
        localId = SimpleDynamoUtils.genHash(String.valueOf(localPort/2));

        membership = new Membership();
        sendLooper = new LooperThread();
        processLooper = new LooperThread();
        requestExecPool = Executors.newCachedThreadPool();


        sendLooper.start();
        processLooper.start();

		return false;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {

        Cursor cur = null;
        cur = requestCoordination(1, selection);
		return cur;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {

		return 0;
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
                }
                return (R)Integer.valueOf(i);
            default:
                return null;
        }

    }

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

            // When item is not belonging to current node
            // forward the item to node which it belongs to
            // When item is belonging to current node
            // insert the item into local database firstly
            // then notify the next 2 successor to replicate.

            if (localPort != c[0]*2) {

                Message ins = new Message();
                ins.forwardPort = c[0]*2;
                ins.msgType = Message.type.INSERT;
                ins.key = key;
                ins.value = value;

                // Step 2: sending requests to coordinator.
                sendLooper.mHandler.post(new SendThread(ins));
                Log.d(TAG, "INSERT -- FORWARD:" + c[0]*2 + "KEY:" + key + " VERSION: UNKNOWN");


                // Step 3: waiting for response from coordinator


                // Step 4: process reply and repackage response
                return u;

            } else {

                int replyNum = 0;

                // Step 2: sending requests to coordinator.

                String hashKey = (key != null)? SimpleDynamoUtils.genHash(key) : "";
                int replicA = c[1]*2;
                int replicB = c[2]*2;

                // Increase the version
                int version = getKeyVersion(hashKey)+1;
                values.put(DatabaseSchema.DatabaseEntry.COLUMN_NAME_VERSION, version);

                // insert in local firstly
                u = localInsert(SimpleDynamoUtils.DATABASE_CONTENT_URL, values);
                replyNum = (u != null) ? replyNum++ : replyNum;
                Log.d(TAG, "INSERT -- IN:" + localPort + "KEY:" + key + " VERSION:" + version);


                // Replication
                Message repA = new Message();
                repA.msgType = Message.type.REPLICA;
                repA.forwardPort = replicA;
                repA.key = key;
                repA.value = value;
                repA.version = version;
                sendLooper.mHandler.post(new SendThread(repA));

                Log.d(TAG, "REPLICA -- TO:" + replicA + "KEY:" + key + " VERSION:" + version);



                // Replication
                Message repB = new Message();
                repA.msgType = Message.type.REPLICA;
                repB.forwardPort = replicB;
                repB.key = key;
                repB.value = value;
                repB.version = version;
                sendLooper.mHandler.post(new SendThread(repB));

                Log.d(TAG, "REPLICA -- TO:" + replicB + "KEY:" + key + " VERSION:" + version);



                // Step 3: waiting for response from coordinator
                // In there, wait another W-1 reply from replication
                // If repA success, replyNum ++
                // If repB success, replyNum ++


                // Step 4: process reply and repackage response
                return (replyNum >= SimpleDynamoUtils.WRITE_CONFIRM_NODE_NUM) ? u : null;

            }
        }
    }

    private class ProcessQuery implements Callable<Cursor> {
        // TODO Auto-generated method stub

        String key;
        String hashKey;

        public ProcessQuery(String selection) {
            key = (selection != null && selection.contains("\"")) ? selection.substring(1, selection.length()-1) : selection;
            hashKey = (key != null)? SimpleDynamoUtils.genHash(key) : "";
        }

        @Override
        public Cursor call() {
            Cursor cur = null;

            if (key.equals("@")) {

                Log.d(TAG, "QUERY @");

                cur = localQuery(SimpleDynamoUtils.DATABASE_CONTENT_URL, null, "@", null, null);

                Log.d(TAG, "QUERY @ finished");

                return cur;

            } else if (key.equals("*")) {

                Cursor cursor = null;
                cur = new MatrixCursor(new String[] {
                        DatabaseSchema.DatabaseEntry.COLUMN_NAME_KEY,
                        DatabaseSchema.DatabaseEntry.COLUMN_NAME_VALUE});

                Log.d(TAG, "QUERY *");

                cursor = localQuery(SimpleDynamoUtils.DATABASE_CONTENT_URL, null, "@", null, null);

                if (cursor != null) {
                    int keyIndex = cursor.getColumnIndex(DatabaseSchema.DatabaseEntry.COLUMN_NAME_KEY);
                    int valueIndex = cursor.getColumnIndex(DatabaseSchema.DatabaseEntry.COLUMN_NAME_VALUE);
                    if (keyIndex != -1 && valueIndex != -1) {
                        if (cursor.moveToFirst()) {
                            do {

                                ((MatrixCursor) cur).addRow(new String[]{
                                        cursor.getString(keyIndex), cursor.getString(valueIndex)});

                            } while (cursor.moveToNext());
                        }
                    }
                    cursor.close();
                }

                for (int n = 0; n < membership.REMOTEAVD.size(); n++) {
                    int forward = membership.REMOTEAVD.get(n)*2;
                    if (forward == localPort) continue;

                    Message delMsg = new Message();
                    delMsg.msgType = Message.type.QUERY;
                    delMsg.key = "\"@\"";
                    delMsg.forwardPort = forward;

                    // send to everyone and wait response and plus result
                }

                return cur;

            } else {

                // Step 1: Identify the node that holds the key.
                int[] c = membership.findPreferenceList(key);

                // When item is not belonging to current node
                // forward the item to node which it belongs to
                // When item is belonging to current node
                // insert the item into local database firstly
                // then notify the next 2 successor to replicate.

                if (localPort != c[0]*2) {

                    Message ins = new Message();
                    ins.forwardPort = c[0]*2;
                    ins.msgType = Message.type.DELETE;
                    ins.key = key;

                    // Step 2: sending requests to coordinator.
                    sendLooper.mHandler.post(new SendThread(ins));
                    Log.d(TAG, "QUERY -- FORWARD:" + c[0]*2 + "KEY:" + key + " VERSION: UNKNOWN");


                    // Step 3: waiting for response from coordinator


                    // Step 4: process reply and repackage response
                    return cur;

                } else {

                    int replyNum = 0;

                    // Step 2: sending requests to coordinator.

                    // Step 3: waiting for response from coordinator
                    // In there, wait another R-1 reply from replication
                    // If repA success, replyNum ++
                    // If repB success, replyNum ++


                    // Step 4: process reply and repackage response
                    return (replyNum >= SimpleDynamoUtils.READ_CONFIRM_NODE_NUM) ? cur : null;

                }
            }
        }
    }

    private class ProcessDelete implements Callable<Integer> {
        // TODO Auto-generated method stub

        String key;
        String hashKey;

        public ProcessDelete(String selection) {
            key = (selection != null && selection.contains("\"")) ? selection.substring(1, selection.length()-1) : selection;
            hashKey = (key != null)? SimpleDynamoUtils.genHash(key) : "";
        }

        @Override
        public Integer call() {
            int i = 0;

            if (key.equals("@")) {

                Log.d(TAG, "DELETE @");

                synchronized (dbLock) {
                    i = db.delete(DatabaseSchema.DatabaseEntry.TABLE_NAME, null, null);
                }

                Log.d(TAG, "DELETE @ finished");

                return i;

            } else if (key.equals("*")) {

                Log.d(TAG, "DELETE *");


                synchronized (dbLock) {
                    int rows = 0;
                    rows = db.delete(DatabaseSchema.DatabaseEntry.TABLE_NAME, null, null);
                    i += rows;
                }

                for (int n = 0; n < membership.REMOTEAVD.size(); n++) {
                    int forward = membership.REMOTEAVD.get(n)*2;
                    if (forward == localPort) continue;

                    Message delMsg = new Message();
                    delMsg.msgType = Message.type.DELETE;
                    delMsg.key = "\"@\"";
                    delMsg.forwardPort = forward;

                    // send to everyone and wait response and plus result
                }

                return i;

            } else {
                // Step 1: Identify the node that holds the key.
                int[] c = membership.findPreferenceList(key);

                // When item is not belonging to current node
                // forward the item to node which it belongs to
                // When item is belonging to current node
                // insert the item into local database firstly
                // then notify the next 2 successor to replicate.

                if (localPort != c[0]*2) {

                    Message ins = new Message();
                    ins.forwardPort = c[0]*2;
                    ins.msgType = Message.type.DELETE;
                    ins.key = key;

                    // Step 2: sending requests to coordinator.
                    sendLooper.mHandler.post(new SendThread(ins));
                    Log.d(TAG, "DELETE -- FORWARD:" + c[0]*2 + "KEY:" + key + " VERSION: UNKNOWN");


                    // Step 3: waiting for response from coordinator


                    // Step 4: process reply and repackage response
                    return i;

                } else {

                    int replyNum = 0;

                    // Step 2: sending requests to coordinator.

                    // Step 3: waiting for response from coordinator
                    // In there, wait another W-1 reply from replication
                    // If repA success, replyNum ++
                    // If repB success, replyNum ++


                    // Step 4: process reply and repackage response
                    return (replyNum >= SimpleDynamoUtils.READ_CONFIRM_NODE_NUM) ? i : null;

                }
            }
        }
    }


    /**
     * query the version of key from database, where the parameter is hash key of selection.
     *
     * @param hashKey
     * @return version, number of version of key
     */
    private int getKeyVersion(String hashKey) {
        Cursor cur = null;
        int version = 0;
        int versionIndex = 0;

        synchronized (dbLock) {
            cur = db.query(
                    DatabaseSchema.DatabaseEntry.TABLE_NAME,
                    new String[]{DatabaseSchema.DatabaseEntry.COLUMN_NAME_VERSION},
                    DatabaseSchema.DatabaseEntry.COLUMN_NAME_HASHKEY + "=?", new String[]{hashKey},
                    null, null, null);
        }

        if (cur != null) {
            versionIndex = cur.getColumnIndex(DatabaseSchema.DatabaseEntry.COLUMN_NAME_VERSION);
            if (versionIndex != -1) {
                if(cur.moveToFirst()) {
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
     * @param uri
     * @param values
     * @return uri
     */
    public Uri localInsert(Uri uri, ContentValues values) {
        // TODO: delete previous version

        long newRowId;
        synchronized (dbLock) {
            newRowId = db.insertOrThrow(DatabaseSchema.DatabaseEntry.TABLE_NAME, null, values);
        }

        String key = values.getAsString(DatabaseSchema.DatabaseEntry.COLUMN_NAME_KEY);
        String value = values.getAsString(DatabaseSchema.DatabaseEntry.COLUMN_NAME_VALUE);
        String version = values.getAsString(DatabaseSchema.DatabaseEntry.COLUMN_NAME_VERSION);
        Log.d(TAG, "Insert:("+newRowId+") Version: " + version + " KEY: " + key + " VALUE: " + value);

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
                cur = db.rawQuery("SELECT * from " + DatabaseSchema.DatabaseEntry.TABLE_NAME, null);
            }
            cur.setNotificationUri(context.getContentResolver(), uri);
        } else {
            String hashKey = SimpleDynamoUtils.genHash(selection);

            synchronized (dbLock) {
                cur = db.rawQuery("SELECT * from " + DatabaseSchema.DatabaseEntry.TABLE_NAME +
                        " WHERE " + DatabaseSchema.DatabaseEntry.COLUMN_NAME_HASHKEY +
                        "=?", new String[] {hashKey});
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
                i = delete(uri, DatabaseSchema.DatabaseEntry.COLUMN_NAME_HASHKEY, new String[] {hashKey});
            }
        }

        return i;
    }

}
