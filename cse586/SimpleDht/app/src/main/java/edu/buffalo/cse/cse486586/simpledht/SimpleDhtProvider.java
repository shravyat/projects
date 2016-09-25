package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.concurrent.ExecutionException;

import android.content.ContentProvider;
import android.content.ContentResolver;
import android.content.ContentUris;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDhtProvider extends ContentProvider {

    private SQLiteDatabase DB;
    static final String DB_Name = "DB_DHT";
    static final String DB_Table = "table_DHT";
    static final int DATABASE_VERSION = 1;

    static final String key = "key";
    static final String value = "value";

    static final String TAG = SimpleDhtActivity.class.getSimpleName();
    static final String REMOTE_PORT0 = "11108";
    static final String REMOTE_PORT1 = "11112";
    static final String REMOTE_PORT2 = "11116";
    static final String REMOTE_PORT3 = "11120";
    static final String REMOTE_PORT4 = "11124";
    static final String[] REMOTE_PORTS = {REMOTE_PORT0, REMOTE_PORT1, REMOTE_PORT2, REMOTE_PORT3, REMOTE_PORT4};
    static final int SERVER_PORT = 10000;

    static final int[] avd_ports = {5554, 5556, 5558, 5560, 5562};
    static final ArrayList<String> hashed_avd_ports = new ArrayList<String>(); // used in Oncreate to get the hashed port values of avd_ports
    static String AVD_inst = "";
    static ArrayList<String> avd_joints = new ArrayList<String>(); //in client
    static final ArrayList<String> hashed_nodes = new ArrayList<String>(); //hashed nodes & then sorted for node joints
    static String successor = "";
    static String predecessor ="";
    static String suc = "";
    static String pre = "";
    static String avd_inst_hashed = "";

    static ArrayList<String> alive_avds_hashed = new ArrayList<String>();
    static ArrayList<String> alive_avds = new ArrayList<String>();

    static final String CREATE_TABLE = "CREATE TABLE " + DB_Table + "(key TEXT NOT NULL, value TEXT NOT NULL);";

    private ContentResolver mContentResolver;
    static Uri mUri;

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub

        Context context = getContext();
        TelephonyManager tel = (TelephonyManager) context.getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        AVD_inst = myPort;

        mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");

        //Printing the AVD_inst hashed value
        try {
            avd_inst_hashed = genHash(Integer.toString((Integer.parseInt(AVD_inst))/2));
        } catch (NoSuchAlgorithmException e) { e.printStackTrace(); }
        Log.v("Hashed value-local port", avd_inst_hashed);

        //Getting the hashed values of avd_ports
        for(int i = 0; i<5; i++) {
            String avd_hashed = "";
            try {
                avd_hashed = genHash(Integer.toString(avd_ports[i]/2));
            } catch (NoSuchAlgorithmException e) { e.printStackTrace();}
            hashed_avd_ports.add(i, avd_hashed);
        }

        try {
            ServerSocket serverSocket= new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);

            if (AVD_inst.equals(REMOTE_PORT0)) {
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, null, AVD_inst);
            }
        } catch (IOException e) { Log.e(TAG, "Can't create a ServerSocket"); }

        DatabaseHelper dbHelper = new DatabaseHelper(context);
        DB = dbHelper.getWritableDatabase();
        if (DB == null){ return false; }
        else {return true;}
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub
        long rowID=0;
        Uri uri_new = null;

        //GET KEY VALUE from ContentValues
        String key1 = values.get(key).toString();
        String value1 = values.get(value).toString();
        String insert_request = key1+";"+value1;

        //Get the hashed value of key
        String key1_hashed = "";
        try { key1_hashed = genHash(key1);
        } catch (NoSuchAlgorithmException e) { e.printStackTrace(); }
        //Log.v("Value of hashed key",key1_hashed);

        //If there is a single AVD alive
        if(successor.equals("")) {
            rowID = DB.insert(DB_Table, null, values);
            Log.v("inserted into database", values.toString());
        }
        else{
            //smallest avd
            if (alive_avds.get(0).equals(AVD_inst)){

                if (key1_hashed.compareTo(avd_inst_hashed)<0){
                    rowID = DB.insert(DB_Table, null, values);
                    Log.v("inserted into database", values.toString());
                }
                else if (key1_hashed.compareTo(predecessor)>0){
                    rowID = DB.insert(DB_Table, null, values);
                    Log.v("inserted into database", values.toString());
                }
                else{
                    //Log.v("Sending to next", "");
                    new ClientTask_insert().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, insert_request, AVD_inst);
                }
            }
            //all other avds
            else{
                if (key1_hashed.compareTo(avd_inst_hashed)<0 && key1_hashed.compareTo(predecessor)>0){
                    rowID = DB.insert(DB_Table, null, values);
                    Log.v("inserted into database", values.toString());
                }
                else{
//                    Log.v("Sending to next", "");
                    new ClientTask_insert().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, insert_request, AVD_inst);
                }
            }

        }

        return uri_new;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs, String sortOrder) {
        // TODO Auto-generated method stub

        Cursor cursor_query=null;
        String key1 = selection;

        String key1_hashed="";
        try {
            key1_hashed = genHash(selection);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        if(successor.isEmpty()){
            Log.v("One AVD","Case");
            if (key1.equals("*")) {
                cursor_query = DB.rawQuery("select * from " + DB_Table, null);
            }
            else if (key1.equals("@")) {
                cursor_query = DB.rawQuery("select * from " + DB_Table, null);
            }
            else {
                cursor_query = DB.query(DB_Table, projection, "key='" + key1 + "'", selectionArgs, null, null, null, null);
                Log.v("query", selection);
            }
            return cursor_query;
        }
        else
        {
            Log.v("All AVDS","Case");
            if (key1.equals("@")) {
                cursor_query = DB.rawQuery("select * from " + DB_Table, null);
                return cursor_query;
            }
            else if(key1.equals("*"))
            {

                Log.v("INSIDE QUERY","ALL");

                try {
                    cursor_query = new ClientTask_QueryAll().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR).get();
                    return cursor_query;
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    return null;
                } catch (ExecutionException e) {
                    e.printStackTrace();
                    return null;
                }
            }
            else {
                Log.v("Inside Query","");
                //smallest avd
                if (alive_avds.get(0).equals(AVD_inst)) {

                    Log.v("Smallest","Case");
                    if (key1_hashed.compareTo(avd_inst_hashed) < 0) {
                        cursor_query = DB.query(DB_Table, projection, "key='" + key1 + "'", selectionArgs, null, null, null, null);
                        Log.v("query", selection);
                        return cursor_query;
                    } else if (key1_hashed.compareTo(predecessor) > 0) {
                        cursor_query = DB.query(DB_Table, projection, "key='" + key1 + "'", selectionArgs, null, null, null, null);
                        Log.v("query", selection);
                        return cursor_query;
                    } else {
                        Log.v("Sending to next", "");
                        try {
                            cursor_query = new ClientTask_Query().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, key1, AVD_inst).get();
                            return cursor_query;
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                            return null;
                        } catch (ExecutionException e) {
                            e.printStackTrace();
                            return null;
                        }
                    }
                }
                //all other avds
                else {
                    Log.v("Other","Case");
                    if (key1_hashed.compareTo(avd_inst_hashed) < 0 && key1_hashed.compareTo(predecessor) > 0) {
                        cursor_query = DB.query(DB_Table, projection, "key='" + key1 + "'", selectionArgs, null, null, null, null);
                        Log.v("query", selection);
                        return cursor_query;
                    } else {
                        Log.v("Sending to next", "");
                        try {
                            cursor_query = new ClientTask_Query().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, key1, AVD_inst).get();
                            return cursor_query;
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                            return  null;
                        } catch (ExecutionException e) {
                            e.printStackTrace();
                            return null;
                        }
                    }
                }
            }

        }
        //return cursor_query;
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub

        String key1 = selection;
        int rowID;
        if (key1.equals("*")) {
            rowID = DB.delete(DB_Table, null, null);
        } else if (key1.equals("@")) {
            rowID = DB.delete(DB_Table, null, null);
        } else {
            rowID = DB.delete(DB_Table, "key='"+key1+"'", null);
        }

        if (rowID > 0) {
            Log.v("No of rows deleted", Integer.toString(rowID));
            return rowID;
        } else {
            Log.v("No rows deleted", "ERROR");
            return 0;
        }
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {
        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            try {
                while (true) {
                    String text;
                    Socket cs = serverSocket.accept();
                    InputStreamReader sin = new InputStreamReader(cs.getInputStream());
                    BufferedReader sin1 = new BufferedReader(sin);

                    PrintWriter sout = new PrintWriter(cs.getOutputStream(), true);

                    text = sin1.readLine();
                    //Log.v("Msg-ping received", text);

                    String delims = "[;]";
                    String[] tokens = text.split(delims);

                    if (tokens[0].equals("ping")) {
                        sout.println(AVD_inst);
                        sout.flush();
                    }
                    else if (tokens[0].equals("JOINS")){
                        String temp = sin1.readLine();
                        String[] temp1 = temp.split(delims);

                        for (int i = 1;i<tokens.length;i++){
                            alive_avds_hashed.add(tokens[i]);
                            alive_avds.add(temp1[i]);
                        }

                        // Setting successor and predecessor
                        for(int i=0;i<alive_avds_hashed.size();i++) {
                            if(alive_avds_hashed.get(i).equals(avd_inst_hashed)) {
                                if(i==0) {
                                    successor = alive_avds_hashed.get(i+1);
                                    predecessor = alive_avds_hashed.get(alive_avds_hashed.size()-1);
                                    suc = alive_avds.get(i+1);
                                    pre = alive_avds.get(alive_avds.size()-1);
                                }
                                else if(i==alive_avds_hashed.size()-1) {
                                    successor = alive_avds_hashed.get(0);
                                    predecessor = alive_avds_hashed.get(i-1);
                                    suc = alive_avds.get(0);
                                    pre = alive_avds.get(i-1);
                                }
                                else {
                                    successor = alive_avds_hashed.get(i + 1);
                                    predecessor = alive_avds_hashed.get(i - 1);
                                    suc = alive_avds.get(i+1);
                                    pre = alive_avds.get(i-1);
                                }
                            }
                        }
                        Log.v("Successor hashed: ",successor);
                        Log.v("Predecessor hashed:",predecessor);
                        Log.v("Successor :", suc);
                        Log.v("Predecessor :", pre);
                    }
                    else if (tokens[0].equals("Insert")) {

                        String key2 = tokens[1];
                        String value2 = tokens[2];
                        Uri new_uri = null;
                        ContentValues values1 = new ContentValues();

                        values1.put(key, key2);
                        values1.put(value, value2);
                        insert(new_uri, values1);
                    }
                    else if(tokens[0].equals("Query")) {
                        String key_check = tokens[1];
                        String key_got = "";
                        String value_got = "";
                        String values_to_send = "";

                        Cursor Query_Cursor = query(mUri, null, key_check, null, null);
                        if (Query_Cursor != null) {
                            //get elements from cursor
                            if (Query_Cursor.moveToFirst()) {
                                key_got = Query_Cursor.getString(Query_Cursor.getColumnIndex(key));
                                value_got = Query_Cursor.getString(Query_Cursor.getColumnIndex(value));
                                values_to_send =  key_got + ";" + value_got;
                                sout.println(values_to_send);
                                Log.v("SERVER QUERY", "SEND QUERY BACK  "+ values_to_send);
                            }
                        }
                    }
                    else if(tokens[0].equals("QueryAll")) {
                        String key_check = tokens[1];
                        String key_got = "";
                        String value_got = "";
                        String values_to_send = "";

                        Cursor Query_Cursor = query(mUri, null, key_check, null, null);
                        if (Query_Cursor != null) {
                            //get elements from cursor
                            if (Query_Cursor.moveToFirst()) {
                                while(!Query_Cursor.isAfterLast()) {
                                    key_got = Query_Cursor.getString(Query_Cursor.getColumnIndex(key));
                                    value_got = Query_Cursor.getString(Query_Cursor.getColumnIndex(value));
                                    values_to_send =  values_to_send + "," + key_got + ";" + value_got;

                                    Query_Cursor.moveToNext();
                                }
                                values_to_send = values_to_send.substring(1);
                                sout.println(values_to_send);
                                Log.v("SERVER QUERY", "SEND QUERY BACK  " + values_to_send);
                            }
                        }
                    }
                    cs.close();
                }
            }catch (IOException e) { e.printStackTrace();}
            return null;
        }
    }

    private class ClientTask extends AsyncTask<String, Void, Void> {
        @Override
        protected Void doInBackground(String... msgs) {

            try {
                //Log.v("AVD0","GOING TO SLEEP");
                Thread.sleep(6000);
                //Log.v("AVD0", "AWAKE");
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            for (int i = 0; i < 5; i++) {
                try {
                    String remotePort = REMOTE_PORTS[i];
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(remotePort));
                    String msg_ping = "ping";
                    //Log.v("Sending PING", msg_ping);
                    PrintWriter cout_ping = new PrintWriter(socket.getOutputStream(), true);
                    cout_ping.println(msg_ping);
                    cout_ping.flush();

                    BufferedReader cin1 = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    String msg_avd_no = cin1.readLine();
                    if(msg_avd_no!=null) {
                        avd_joints.add(msg_avd_no);
                        //Log.v("Received AVD NO",msg_avd_no);
                    }
                    else
                        Log.v("Did not receive","from"+remotePort);
                } catch (IOException e) {e.printStackTrace();}
            }


            //Finding the hashed nodes
            Log.v("No of pings received",Integer.toString(avd_joints.size()));
            //Divide the avd ports by 2 and then hash
            for (int a = 0; a < avd_joints.size(); a++) {
                String key = avd_joints.get(a);
                String hash = "";
                try {
                    hash = genHash(Integer.toString((Integer.parseInt(key)/2)));
                    //Log.v("HASH :", hash);
                } catch (NoSuchAlgorithmException e) {e.printStackTrace();}
                hashed_nodes.add(a, hash);
            }


            // Sorting hashed values of nodes
            for (int k = 1; k < hashed_nodes.size(); k++) {
                for (int k1 = k; k1 > 0; k1--) {
                    String temp = "";
                    String temp1 = "";
                    if (hashed_nodes.get(k1-1).compareTo(hashed_nodes.get(k1))>0) {
                        temp = hashed_nodes.get(k1 - 1);
                        hashed_nodes.set(k1 - 1, hashed_nodes.get(k1));
                        hashed_nodes.set(k1, temp);

                        temp1 = avd_joints.get(k1-1);
                        avd_joints.set(k1-1, avd_joints.get(k1));
                        avd_joints.set(k1, temp1);
                    }
                }
            }
            //Log.v("Sorted","hashed values nodes");

            String hashed_nodes_to_send= "JOINS";
            String avd_joints_send = "JOINS";
            for (int b=0; b<hashed_nodes.size();b++) {
                //Log.v("hashed node", hashed_nodes.get(b));

                hashed_nodes_to_send = hashed_nodes_to_send + ";"+hashed_nodes.get(b);
                avd_joints_send = avd_joints_send +";"+avd_joints.get(b);
            }

            //hashed_nodes_to_send = hashed_nodes_to_send.substring(1);

            //send list of hashed nodes to all
            for (int i = 0;i<hashed_nodes.size();i++){
                try {
                    String remotePort = REMOTE_PORTS[i];
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(remotePort));
                    PrintWriter cout_ping = new PrintWriter(socket.getOutputStream(), true);

                    cout_ping.println(hashed_nodes_to_send);
                    cout_ping.flush();

                    cout_ping.println(avd_joints_send);
                    cout_ping.flush();

                    socket.close();

                } catch (IOException e) {e.printStackTrace();}
            }


            return null;
        }
    }

    private class ClientTask_insert extends AsyncTask<String, Void, Void> {
        @Override
        protected Void doInBackground(String... msgs) {
            try {
                String remotePort = suc;
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(remotePort));
                String msg_sending = "Insert"+";"+msgs[0];
                //Log.v("Sending insert request", msg_sending);
                PrintWriter cout_ping = new PrintWriter(socket.getOutputStream(), true);

                cout_ping.println(msg_sending);
                cout_ping.flush();
                socket.close();
            }catch (IOException e) {e.printStackTrace();}
            return null;
        }
    }

    private class ClientTask_Query extends AsyncTask<String, Void, Cursor> {
        @Override
        protected Cursor doInBackground(String... msgs) {

            String[] values1 = {key,value};
            String[] values = new String[2];
            MatrixCursor matrixCursor = new MatrixCursor(values1);
            String values_got = "";

            try {
                String remotePort = suc;
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(remotePort));

                String msg_sending = "Query"+";"+msgs[0]+";"+msgs[1];
                Log.v("Sending Query request", msg_sending);

                PrintWriter cout_ping = new PrintWriter(socket.getOutputStream(), true);
                BufferedReader cin1 = new BufferedReader(new InputStreamReader(socket.getInputStream()));

                cout_ping.println(msg_sending);
                cout_ping.flush();

                values_got = cin1.readLine();
                if(!values_got.isEmpty()) {
                    String delims = "[;]";
                    values = values_got.split(delims);
                    Log.v("Client query", values[0]+ " " +values[1]);
                    matrixCursor.addRow(new Object[]{values[0], values[1]});

                }
                socket.close();
            }catch (IOException e) {e.printStackTrace();}
            Cursor cursor = matrixCursor;
            return cursor;
        }

        @Override
        protected void onPostExecute(Cursor cursor) {
            super.onPostExecute(cursor);
        }
    }

    private class ClientTask_QueryAll extends AsyncTask<String, Void, Cursor> {
        @Override
        protected Cursor doInBackground(String... msgs) {

            String[] values1 = {key,value};
            Cursor cursor=null;
            MatrixCursor matrixCursor = new MatrixCursor(values1);
            String values_got = "";
            for (int i = 0;i<alive_avds.size();i++) {
                try {
                    String remotePort = suc;
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(alive_avds.get(i)));

                    String msg_sending = "QueryAll" + ";" + "@";
                    Log.v("Sending QueryAll", msg_sending);

                    PrintWriter cout_ping = new PrintWriter(socket.getOutputStream(), true);
                    BufferedReader cin1 = new BufferedReader(new InputStreamReader(socket.getInputStream()));

                    cout_ping.println(msg_sending);
                    cout_ping.flush();

                    values_got = cin1.readLine();

                    if (values_got != null) {
                        String delims = "[,]";
                        String delims1 = "[;]";
                        String[] values = values_got.split(delims);
                        for (int j = 0; j < values.length; j++) {
                            String[] key_value = values[j].split(delims1);
                            Log.v("Client query", key_value[0] + " " + key_value[1]);
                            matrixCursor.addRow(new Object[]{key_value[0], key_value[1]});
                        }
                    }
                    socket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                cursor = matrixCursor;
            }
            return cursor;
        }

        @Override
        protected void onPostExecute(Cursor cursor) {
            super.onPostExecute(cursor);
        }
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    private static class DatabaseHelper extends SQLiteOpenHelper {
        DatabaseHelper(Context context) {
            super(context, DB_Name, null, DATABASE_VERSION);
        }

        public void onCreate(SQLiteDatabase DB) {
            DB.execSQL(CREATE_TABLE);
        }

        public void onUpgrade(SQLiteDatabase DB, int oldVersion, int newVersion) {
            DB.execSQL("DROP TABLE IF EXISTS " + DB_Table);
            onCreate(DB);
        }
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

}


