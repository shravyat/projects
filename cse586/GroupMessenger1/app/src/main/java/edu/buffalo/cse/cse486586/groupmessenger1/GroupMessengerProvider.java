package edu.buffalo.cse.cse486586.groupmessenger1;

import android.content.ContentProvider;
import android.content.ContentUris;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.net.Uri;
import android.util.Log;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.database.sqlite.SQLiteQueryBuilder;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.database.sqlite.SQLiteQueryBuilder;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.database.sqlite.SQLiteQueryBuilder;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.database.sqlite.SQLiteQueryBuilder;

import java.sql.SQLException;

/**
 * GroupMessengerProvider is a key-value table. Once again, please note that we do not implement
 * full support for SQL as a usual ContentProvider does. We re-purpose ContentProvider's interface
 * to use it as a key-value table.
 *
 * Please read:
 *
 * http://developer.android.com/guide/topics/providers/content-providers.html
 * http://developer.android.com/reference/android/content/ContentProvider.html
 *
 * before you start to get yourself familiarized with ContentProvider.
 *
 * There are two methods you need to implement---insert() and query(). Others are optional and
 * will not be tested.
 *
 * @author stevko
 *
 */
public class GroupMessengerProvider extends ContentProvider {

    private SQLiteDatabase DB, DBW;
    static final String DB_NAME = "GroupText";
    static final String DB_Table = "Table1";
    static final int DATABASE_VERSION = 1;

    static final String key = "key";
    static final String value = "value";

    static final String CREATE_TABLE = "CREATE TABLE " + DB_Table + "( key TEXT NOT NULL, " + "value TEXT NOT NULL);";

    private static class DatabaseHelper extends SQLiteOpenHelper
    {
        DatabaseHelper(Context context)
        {
            super(context, DB_NAME, null, DATABASE_VERSION);
        }

        public void onCreate(SQLiteDatabase DB)
        {
            DB.execSQL(CREATE_TABLE);
        }
        public void onUpgrade(SQLiteDatabase DB, int oldVersion, int newVersion)
        {
            DB.execSQL("DROP TABLE IF EXISTS " +  DB_Table);
            onCreate(DB);
        }
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // You do not need to implement this.
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // You do not need to implement this.
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values)
    {
        long rowID = DB.insert(DB_Table, null, values);

        Uri uri_new = null;
        if (rowID > 0)
        {
            // referred from http://developer.android.com/guide/topics/providers/content-provider-basics.html#Inserting;
            // http://developer.android.com/training/basics/data-storage/databases.html
            uri_new = ContentUris.withAppendedId(uri, rowID);
            getContext().getContentResolver().notifyChange(uri_new, null);
            Log.v("inserted into database", values.toString());
            return uri_new;
        }

        try
        {
            throw new SQLException("Failed to add a record into " + uri);
        }
        catch (SQLException e)
        {
            e.printStackTrace();
        }

        /*
         * TODO: You need to implement this method. Note that values will have two columns (a key
         * column and a value column) and one row that contains the actual (key, value) pair to be
         * inserted.
         * 
         * For actual storage, you can use any option. If you know how to use SQL, then you can use
         * SQLite. But this is not a requirement. You can use other storage options, such as the
         * internal storage option that we used in PA1. If you want to use that option, please
         * take a look at the code for PA1.
         */

        return uri_new;
    }

    @Override
    public boolean onCreate() {
        // If you need to perform any one-time initialization task, please do it here.
        Context context = getContext();
        DatabaseHelper dbHelper = new DatabaseHelper(context);

        DB = dbHelper.getWritableDatabase();

        if(DB==null)
            return false;
        else
            return true;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // You do not need to implement this.
        return 0;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        Cursor cursor_query;

        cursor_query = DB.query(DB_Table, projection, "key='"+selection+"'", selectionArgs, null, null, null, null);
        // referred from http://developer.android.com/reference/android/database/sqlite/SQLiteDatabase.html#query(boolean, java.lang.String, java.lang.String[], java.lang.String, java.lang.String[], java.lang.String, java.lang.String, java.lang.String, java.lang.String)
        /*
         * TODO: You need to implement this method. Note that you need to return a Cursor object
         * with the right format. If the formatting is not correct, then it is not going to work.
         *
         * If you use SQLite, whatever is returned from SQLite is a Cursor object. However, you
         * still need to be careful because the formatting might still be incorrect.
         *
         * If you use a file storage option, then it is your job to build a Cursor * object. I
         * recommend building a MatrixCursor described at:
         * http://developer.android.com/reference/android/database/MatrixCursor.html
         */
        Log.v("query", selection);
        return cursor_query;
    }
}

