package edu.buffalo.cse.cse486586.simpledynamo.Store;

import android.provider.BaseColumns;

/**
 * SimpleDynamo
 * <p/>
 * Created by darrenxyli on 4/9/15.
 * Changed by darrenxyli on 4/9/15 11:19 AM.
 */
public class DatabaseSchema {

    // To prevent someone from accidentally instantiating the contract class,
    // give it an empty constructor.
    public DatabaseSchema() {
    }

    /* Inner class that defines the table contents */
    public static abstract class DatabaseEntry implements BaseColumns {
        public static final String TABLE_NAME = "messages";
        public static final String COLUMN_NAME_KEY = "key";
        public static final String COLUMN_NAME_VALUE = "value";
        public static final String COLUMN_NAME_HASHKEY = "hashkey";
        public static final String COLUMN_NAME_VERSION = "version";
    }
}