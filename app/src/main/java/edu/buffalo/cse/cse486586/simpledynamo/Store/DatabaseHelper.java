package edu.buffalo.cse.cse486586.simpledynamo.Store;

import android.content.Context;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.util.Log;

/**
 * SimpleDht
 * <p/>
 * Created by darrenxyli on 3/26/15.
 * Changed by darrenxyli on 3/26/15 6:47 PM.
 * Reference from: http://developer.android.com/training/basics/data-storage/databases.html
 */
public class DatabaseHelper extends SQLiteOpenHelper {

    public static final int DATABASE_VERSION = 1;
    public static final String DATABASE_NAME = "simpledynamo";

    private static final String TEXT_TYPE = " TEXT";
    private static final String COMMA_SEP = ",";
    private static final String SQL_CREATE_ENTRIES =
            "CREATE TABLE " + DatabaseSchema.DatabaseEntry.TABLE_NAME + " (" +
                    DatabaseSchema.DatabaseEntry.COLUMN_NAME_HASHKEY + " TEXT PRIMARY KEY," +
                    DatabaseSchema.DatabaseEntry.COLUMN_NAME_KEY + TEXT_TYPE + COMMA_SEP +
                    DatabaseSchema.DatabaseEntry.COLUMN_NAME_VALUE + TEXT_TYPE + ");";

    private static final String SQL_DELETE_ENTRIES =
            "DROP TABLE IF EXISTS " + DatabaseSchema.DatabaseEntry.TABLE_NAME;


    /**
     * DatabaseHelper
     *
     * @param context
     */
    public DatabaseHelper(Context context) {
        super(context, DATABASE_NAME, null, DATABASE_VERSION);
    }


    /**
     * on Create
     *
     * @param db
     */
    public void onCreate(SQLiteDatabase db) {
        db.execSQL(SQL_CREATE_ENTRIES);
        Log.d(DatabaseHelper.class.getName(), DatabaseSchema.DatabaseEntry.TABLE_NAME + "table created");
    }

    /**
     * Only used in upgrade db version
     *
     * @param db
     * @param oldVersion
     * @param newVersion
     */
    public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
        db.execSQL(SQL_DELETE_ENTRIES);
        onCreate(db);
        Log.d(DatabaseHelper.class.getName(), "upgraded from " + oldVersion + " to " + newVersion);
    }

    /**
     * Only used in downgrade db version
     *
     * @param db
     * @param oldVersion
     * @param newVersion
     */
    public void onDowngrade(SQLiteDatabase db, int oldVersion, int newVersion) {
        onUpgrade(db, oldVersion, newVersion);
        Log.d(DatabaseHelper.class.getName(), "degraded from " + oldVersion + " to " + newVersion);
    }
}
