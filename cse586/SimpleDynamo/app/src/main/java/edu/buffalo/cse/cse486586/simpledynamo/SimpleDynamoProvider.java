package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.concurrent.ExecutionException;

import android.bluetooth.BluetoothSocket;
import android.content.ContentProvider;
import android.content.ContentResolver;
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

public class SimpleDynamoProvider extends ContentProvider {

    private SQLiteDatabase DB;
    static final String DB_Name = "DB_Dynamo";
    static final String DB_Table = "table_Dynamo";
    static final int DATABASE_VERSION = 1;

    static final String key = "key";
    static final String value = "value";

    static final String TAG = SimpleDynamoActivity.class.getSimpleName();
    static final String REMOTE_PORT0 = "11108";
    static final String REMOTE_PORT1 = "11112";
    static final String REMOTE_PORT2 = "11116";
    static final String REMOTE_PORT3 = "11120";
    static final String REMOTE_PORT4 = "11124";
    static final String[] REMOTE_PORTS = {REMOTE_PORT0, REMOTE_PORT1, REMOTE_PORT2, REMOTE_PORT3, REMOTE_PORT4};
    static final int SERVER_PORT = 10000;

    static final int[] avd_ports = {5554, 5556, 5558, 5560, 5562};
    static String AVD_inst = "";
    static String avd_inst_hashed = "";

    static final ArrayList<String> hashed_avd_joints = new ArrayList<String>(); //hashed nodes & then sorted for node joints in client
    static ArrayList<String> avd_joints = new ArrayList<String>(); //in client

    static String successor = "";
    static String predecessor ="";
    static String suc = "";
    static String pre = "";

    static String failed_avd=null;
    volatile boolean wait_flag = false;

    static final String CREATE_TABLE = "CREATE TABLE " + DB_Table + "(key TEXT NOT NULL PRIMARY KEY, value TEXT NOT NULL);";

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

        mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");

        //Printing the AVD_inst hashed value
        try {
            avd_inst_hashed = genHash(Integer.toString((Integer.parseInt(AVD_inst)) / 2));
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

//		Log.v("Hashed value-local port", avd_inst_hashed);

        //Getting the hashed values of avd_ports
        for (int i = 0; i < 5; i++) {
            String avd_hashed = "";
            try {
                avd_hashed = genHash(Integer.toString(avd_ports[i]));
                avd_joints.add(Integer.toString(2 * avd_ports[i]));

            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
            hashed_avd_joints.add(i, avd_hashed);
        }

        for (int k = 1; k < hashed_avd_joints.size(); k++) {
            for (int k1 = k; k1 > 0; k1--) {
                String temp = "";
                String temp1 = "";
                if (hashed_avd_joints.get(k1 - 1).compareTo(hashed_avd_joints.get(k1)) > 0) {
                    temp = hashed_avd_joints.get(k1 - 1);
                    hashed_avd_joints.set(k1 - 1, hashed_avd_joints.get(k1));
                    hashed_avd_joints.set(k1, temp);

                    temp1 = avd_joints.get(k1 - 1);
                    avd_joints.set(k1 - 1, avd_joints.get(k1));
                    avd_joints.set(k1, temp1);
                }
            }
        }

//		//Printing the joints and the hashed values
//		Log.v("Sorted", "hashed values nodes");
//		for (int k = 0; k < hashed_avd_joints.size(); k++) {
//			Log.v("AVD", avd_joints.get(k) + "-" + hashed_avd_joints.get(k));
//		}

        //Setting the successor and predecessor
        for (int i = 0; i < hashed_avd_joints.size(); i++) {
            if (hashed_avd_joints.get(i).equals(avd_inst_hashed)) {
                if (i == 0) {
                    successor = hashed_avd_joints.get(i + 1);
                    predecessor = hashed_avd_joints.get(hashed_avd_joints.size() - 1);
                    suc = avd_joints.get(i + 1);
                    pre = avd_joints.get(avd_joints.size() - 1);
                } else if (i == hashed_avd_joints.size() - 1) {
                    successor = hashed_avd_joints.get(0);
                    predecessor = hashed_avd_joints.get(i - 1);
                    suc = avd_joints.get(0);
                    pre = avd_joints.get(i - 1);
                } else {
                    successor = hashed_avd_joints.get(i + 1);
                    predecessor = hashed_avd_joints.get(i - 1);
                    suc = avd_joints.get(i + 1);
                    pre = avd_joints.get(i - 1);
                }
            }
        }
//		Log.v("Successor hashed: ", successor);
//		Log.v("Predecessor hashed:", predecessor);
//		Log.v("Successor :", suc);
//		Log.v("Predecessor :", pre);

        String key_check = "@";

        DatabaseHelper dbHelper = new DatabaseHelper(context);
        DB = dbHelper.getWritableDatabase();

        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);

            new ClientTask_recovery().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR);

        } catch (IOException e) { Log.e(TAG, "Can't create a ServerSocket"); }


        if (DB == null){ return false; }
        else {return true;}

    }

    public int find_position(String avd_hashed) {
        int position = -1;
        if((avd_hashed.compareTo(hashed_avd_joints.get(0))<0) ||(avd_hashed.compareTo(hashed_avd_joints.get(4))>0)) {
            position = 0;
        }
        else {
            for (int i = 1; i < 5; i++) {
                if ((avd_hashed.compareTo(hashed_avd_joints.get(i)) < 0) && (avd_hashed.compareTo(hashed_avd_joints.get(i - 1)) > 0)) {
                    position = i;
                }
            }
        }
//		if(position == -1)
//			Log.v("Position","not found");
//		else
//			Log.v("Found Position", Integer.toString(position));

        return position;
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
        }
        catch (NoSuchAlgorithmException e) { e.printStackTrace(); }
//		Log.v("Value of hashed key",key1_hashed);
//		Log.v("Finding Position", "");

        int position = find_position(key1_hashed);

        String avd_no = avd_joints.get(position);
        String replica1 = null;
        String replica2 = null;

        if(position == 3)
        {
            replica1 = avd_joints.get(4);
            replica2 = avd_joints.get(0);
        }
        else if(position == 4)
        {
            replica1 = avd_joints.get(0);
            replica2 = avd_joints.get(1);
        }
        else if(position < 3)
        {
            replica1 = avd_joints.get(position+1);
            replica2 = avd_joints.get(position+2);
        }
        else
        {
            Log.v("Something went wrong","in insert position");
        }
        if(avd_no.equals(AVD_inst))
        {
            DB.replace(DB_Table, null, values);
            Log.v("inserted into database", values.toString());

            new ClientTask_insert().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, insert_request, "Replica1;"+replica1);
            new ClientTask_insert().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, insert_request, "Replica2;" + replica2);
        }
        else if(replica1.equals(AVD_inst))
        {
            new ClientTask_insert().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, insert_request, "Insert;"+avd_no);

            DB.replace(DB_Table, null, values);
            Log.v("Replicated in database", values.toString());

            new ClientTask_insert().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, insert_request, "Replica2;" + replica2);

        }
        else if(replica2.equals(AVD_inst))
        {
            new ClientTask_insert().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, insert_request, "Insert;"+avd_no);
            new ClientTask_insert().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, insert_request, "Replica1;" + replica1);

            DB.replace(DB_Table, null, values);
            Log.v("Replicate in database", values.toString());
        }
        else if (avd_no != null && replica1!= null && replica2 !=null)
        {
            new ClientTask_insert().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, insert_request, "Insert;"+avd_no);
            new ClientTask_insert().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, insert_request, "Replica1;"+replica1);
            new ClientTask_insert().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, insert_request, "Replica2;"+replica2);
        }
        else {
            Log.v("Something went wrong","finding position");
        }
        return uri_new;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs, String sortOrder) {
        // TODO Auto-generated method stub
        Cursor cursor_query=null;
        String key1 = selection;

//		Log.v("All AVDS","Query");
        if (key1.equals("@")) {
            cursor_query = DB.rawQuery("select * from " + DB_Table, null);
            return cursor_query;
        }

        else if(key1.equals("*"))
        {
//			Log.v("INSIDE QUERY","ALL");
            try {
                cursor_query = new ClientTask_Query().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "query_all").get();
                return cursor_query;
            } catch (InterruptedException e) {
                e.printStackTrace();
                return null;
            } catch (ExecutionException e) {
                e.printStackTrace();
                return null;
            }
        }

        else
        {
//			Log.v("Inside Query",AVD_inst);

			Log.v("query", selection);
            cursor_query = DB.query(DB_Table, projection, "key='" + key1 + "'", selectionArgs, null, null, null, null);

            if(cursor_query.getCount()!=0)
            {
                return cursor_query;
            }
            else {
                String key_hashed=null;
//				Log.v("Finding Position", "Query");
                try {
                    key_hashed = genHash(key1);
                    int position = find_position(key_hashed);
                    String avd_no = avd_joints.get(position);
                    String replica1 = null;
                    String replica2 = null;
                    if(position == 3)
                    {
                        replica1 = avd_joints.get(4);
                        replica2 = avd_joints.get(0);
                    }
                    else if(position == 4)
                    {
                        replica1 = avd_joints.get(4);
                        replica2 = avd_joints.get(0);
                    }
                    else if(position < 3)
                    {
                        replica1 = avd_joints.get(position+1);
                        replica2 = avd_joints.get(position+2);
                    }

                    String query_avd = avd_no+";"+replica1+";"+replica2;

                    cursor_query = new ClientTask_Query().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, key1, query_avd).get();
                    return cursor_query;
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                    return null;
                }
                catch (ExecutionException e) {
                    e.printStackTrace();
                    return null;
                }
                catch (NoSuchAlgorithmException e) {
                    e.printStackTrace();
                    return null;
                }
            }

        }
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
//					Log.v("Msg-ping received", text);

                    String delims = "[;]";
                    String[] tokens = text.split(delims);

                    if (tokens[0].equals("ask")) {
                        if(failed_avd!=null) {
//							Log.v("sending failed_avd :", failed_avd);
                            sout.println(failed_avd);
                            sout.flush();
                        } else {
                            sout.println("none");
                            sout.flush();
                        }
                    }

                    else if (tokens[0].equals("Insert")) {
//						Log.v("Failed avd is", failed_avd);
//                        if(wait_flag==true)
//                        {
//                            Thread.sleep(400);
//                            Log.v("Thread sleep insert", "over");
//                        }

                        String key2 = tokens[1];
                        String value2 = tokens[2];
                        Uri new_uri = null;
                        ContentValues values1 = new ContentValues();

                        values1.put(key, key2);
                        values1.put(value, value2);
//						insert_replica(new_uri, values1);

                        Log.v("Inserted into DB", values1.toString());
                        DB.replace(DB_Table, null, values1);


//						Log.v("Insert", "success");
                        sout.println("Insert Success");
                        sout.flush();
                    }

                    else if (tokens[0].equals("Replica")) {

                        String key2 = tokens[1];
                        String value2 = tokens[2];
                        Uri new_uri = null;
                        ContentValues values1 = new ContentValues();

                        values1.put(key, key2);
                        values1.put(value, value2);
//						insert_replica(new_uri, values1);
                        Log.v("Replicated into DB", values1.toString());
                        DB.replace(DB_Table, null, values1);

//						Log.v("Replica", "success");
                        sout.println("Replica Success");
                        sout.flush();
                    }

                    else if (tokens[0].equals("Query"))
                    {
//						Log.v("Failed avd is", failed_avd);
                        if(wait_flag==true)
                        {
                            Thread.sleep(400);
                            Log.v("Thread sleep Query", "over");
                        }
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

//								Log.v("SERVER QUERY", "SEND QUERY BACK  "+ values_to_send);
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
//								Log.v("SERVER QUERY", "SEND QUERY BACK  " + values_to_send);
                            }
                        }
                    }

                    else if(tokens[0].equals("recovery_pre"))
                    {
//						Log.v("Inside","recovery_pre");
//						Log.v("requesting", tokens[1]);

                        String key_check = tokens[1];
                        String key_got = "";
                        String value_got = "";
                        String values_to_send = "";

                        Cursor Query_Cursor = query(mUri, null, key_check, null, null);
                        if (Query_Cursor != null)
                        {
                            //get elements from cursor
                            if (Query_Cursor.moveToFirst())
                            {
                                while (!Query_Cursor.isAfterLast())
                                {
                                    key_got = Query_Cursor.getString(Query_Cursor.getColumnIndex(key));
                                    value_got = Query_Cursor.getString(Query_Cursor.getColumnIndex(value));
                                    values_to_send = values_to_send + "," + key_got + ";" + value_got;

                                    Query_Cursor.moveToNext();
                                }
                                values_to_send = values_to_send.substring(1);
                                sout.println(values_to_send);
//								Log.v("SERVER Recovery", values_to_send);
                            }
                        }

                    }

                    else if(tokens[0].equals("recovery_suc"))
                    {
//						Log.v("Inside","recovery_suc");
//						Log.v("requesting", tokens[1]);

                        String key_check = tokens[1];

                        String key_got = "";
                        String value_got = "";
                        String values_to_send = "";

                        Cursor Query_Cursor = query(mUri, null, key_check, null, null);

                        if (Query_Cursor != null)
                        {
                            //get elements from cursor
                            if (Query_Cursor.moveToFirst())
                            {
                                while (!Query_Cursor.isAfterLast())
                                {
                                    key_got = Query_Cursor.getString(Query_Cursor.getColumnIndex(key));
                                    value_got = Query_Cursor.getString(Query_Cursor.getColumnIndex(value));
                                    values_to_send = values_to_send + "," + key_got + ";" + value_got;

                                    Query_Cursor.moveToNext();
                                }

                                //Log.v("data_send",values_to_send);

                                values_to_send = values_to_send.substring(1);
                                sout.println(values_to_send);
//								Log.v("SERVER Recovery", values_to_send);
                            }
                        }
                    }
                    else if(tokens[0].equals("failed_none"))
                    {
                        Log.v("Failed avd", "setting to null");
                        failed_avd = "none";
                    }
                    cs.close();
                }
            }catch (IOException e) { e.printStackTrace();} catch (InterruptedException e) {
                e.printStackTrace();
            }
            return null;
        }
    }

    private class ClientTask_recovery extends AsyncTask<String, Void, Void> {
        @Override
        protected Void doInBackground(String... msgs) {

            String avd_failed = "none";

            String remotePort_1 = pre;
            String remotePort_2 = suc;

            String delims = "[,]";
            String delims1 = "[;]";

            String data_received_pre = null;

            try {
                for (int i = 0; i < 5; i++) {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(avd_joints.get(i)));
                    String msg_ask = "ask";
//					Log.v("Sending request", "for failed_avd");

                    PrintWriter cout_failed = new PrintWriter(socket.getOutputStream(), true);
                    BufferedReader cin_failed = new BufferedReader(new InputStreamReader(socket.getInputStream()));

                    cout_failed.println(msg_ask);
                    cout_failed.flush();

                    avd_failed = cin_failed.readLine();
                    if (avd_failed != null) {
                        if (!avd_failed.equals("none")) {
                            Log.v("recovery node", avd_failed);
                            break;
                        }
                    }
                    socket.close();
                }
            } catch (IOException e) { e.printStackTrace(); }

            if (avd_failed != null) {
                if (!avd_failed.equals("none")) {
                    try {
//					Log.v("Getting data from pre", pre);

                        String msg_sending = "recovery_pre" + ";" + "@";
//					Log.v("Send recovery pre", msg_sending + ";" + pre);

                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(remotePort_1));

                        PrintWriter cout_recovery_pre = new PrintWriter(socket.getOutputStream(), true);
                        BufferedReader cin_recovery_suc = new BufferedReader(new InputStreamReader(socket.getInputStream()));

                        cout_recovery_pre.println(msg_sending);
                        cout_recovery_pre.flush();

                        data_received_pre = cin_recovery_suc.readLine();

                        socket.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }

                if ((data_received_pre != null) || avd_failed.equals(AVD_inst)) {
                    Log.v("Recovery", "start");
                    wait_flag = true;
//				Log.v("Deleting","db");

                    delete(mUri, "@", null);
//				Log.v("Finding", "position");

                    int position = -1;

                    for (int i = 0; i < 5; i++) {
                        if (avd_joints.get(i).equals(AVD_inst)) {
                            position = i;
                            break;
                        }
                    }
                    String pre1 = null;

                    if (position == 0) {
                        pre1 = avd_joints.get(3);
                    } else if (position == 1) {
                        pre1 = avd_joints.get(4);
                    } else if (position == 2) {
                        pre1 = avd_joints.get(0);
                    } else if (position > 2) {
                        pre1 = avd_joints.get(position - 2);
                    } else {
                        Log.v("Something went", "wrong in recovery position");
                    }

                    String[] values1 = data_received_pre.split(delims);

                    for (int j = 0; j < values1.length; j++) {
                        ContentValues values_insert = new ContentValues();

                        String key_value1[] = values1[j].split(delims1);

                        String key_hashed = null;
                        try {
                            key_hashed = genHash(key_value1[0]);
                        } catch (NoSuchAlgorithmException e) {
                            e.printStackTrace();
                        }

                        int position_1 = find_position(key_hashed);

                        String avd_1 = avd_joints.get(position_1);

                        if (avd_1.equals(AVD_inst) || avd_1.equals(pre) || avd_1.equals(pre1)) {

                            values_insert.put(key, key_value1[0]);
                            values_insert.put(value, key_value1[1]);

                            Log.v("Insert in DB recovery", values_insert.toString());
                            DB.insertWithOnConflict(DB_Table, null, values_insert, SQLiteDatabase.CONFLICT_IGNORE);

                        } else {
                            Log.v("Condition not satisfied", key_value1[0]);
                        }
                    }
//					Log.v("Keys Inserted from pre", Integer.toString(count1));

                    try {
//					Log.v("Getting data from suc", suc);

                        String msg_sending = "recovery_suc" + ";" + "@";
//					Log.v("Send recovery pre", msg_sending + ";" + suc);

                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(remotePort_2));

                        PrintWriter cout_recovery_suc = new PrintWriter(socket.getOutputStream(), true);
                        BufferedReader cin_recovery_suc = new BufferedReader(new InputStreamReader(socket.getInputStream()));

                        cout_recovery_suc.println(msg_sending);
                        cout_recovery_suc.flush();

                        String data_received_suc = cin_recovery_suc.readLine();

                        if (data_received_suc != null) {
//						Log.v("Insert pre in recovery", data_received_suc);

                            String[] values = data_received_suc.split(delims);
//						Log.v("Keys obtained", Integer.toString(values.length));
//						int count = 0;

                            for (int j = 0; j < values.length; j++) {
                                ContentValues values_insert = new ContentValues();

                                String key_value[] = values[j].split(delims1);

                                String key_hashed = genHash(key_value[0]);

                                int position_0 = find_position(key_hashed);
                                String avd_0 = avd_joints.get(position_0);

                                if (avd_0.equals(AVD_inst)) {
                                    values_insert.put(key, key_value[0]);
                                    values_insert.put(value, key_value[1]);
//								insert_replica(new_uri, values_insert);

                                    Log.v("Insert in DB recovery", values_insert.toString());
                                    DB.insertWithOnConflict(DB_Table, null, values_insert, SQLiteDatabase.CONFLICT_IGNORE);

//								count++;
                                } else {
                                    Log.v("Condition not satisfied", key_value[0]);
                                }
                            }
//						Log.v("Keys Inserted from suc", Integer.toString(count));
                        }
                        socket.close();
                    } catch (IOException e) { e.printStackTrace(); }
                    catch (NoSuchAlgorithmException e) { e.printStackTrace();}

                    Log.v("Recovery", "complete");
                    wait_flag = false;

                    try {
                        for (int i = 0; i < 5; i++) {

                            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(avd_joints.get(i)));
                            String failed_none = "failed_none";
//						Log.v("Sending failed_none", "none");

                            PrintWriter cout_failed = new PrintWriter(socket.getOutputStream(), true);
                            cout_failed.println(failed_none);
                            cout_failed.flush();
                            socket.close();
                        }
                    } catch (IOException e) { e.printStackTrace(); }
                }
            }
            else { Log.v("No recovery","Needed"); }
            return null;
        }
    }

    private class ClientTask_insert extends AsyncTask<String, Void, Void> {
        @Override
        protected Void doInBackground(String... msgs) {
            try {
                boolean flag = false;
                String ack;

                String delims = "[;]";
                String[] msgs_1 = msgs[1].split(delims);


                if(msgs_1[0].equals("Insert"))
                {
                    String remotePort = msgs_1[1];
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(remotePort));

                    BufferedReader cin_insert = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    PrintWriter cout_insert = new PrintWriter(socket.getOutputStream(), true);

                    String msg_sending = "Insert" + ";" + msgs[0];
//					Log.v("Sending insert request", msg_sending);

                    cout_insert.println(msg_sending);
                    cout_insert.flush();

                    ack = cin_insert.readLine();
                    socket.close();
                    if(ack==null)
                    {
//						Log.v("Insert failed", remotePort);
                        failed_avd = remotePort;
                        flag=true;
                    }
                }
                else if(msgs_1[0].equals("Replica1"))
                {
                    String remotePort = msgs_1[1];
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(remotePort));

                    BufferedReader cin_insert = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    PrintWriter cout_insert = new PrintWriter(socket.getOutputStream(), true);

                    String msg_sending = "Replica" + ";" + msgs[0];
//					Log.v("Send Replica1 request", msg_sending);

                    cout_insert.println(msg_sending);
                    cout_insert.flush();

                    ack = cin_insert.readLine();
                    socket.close();
                    if(ack==null)
                    {
//						Log.v("Replica1 failed", remotePort);
                        failed_avd = remotePort;
                        flag=true;
                    }
                }
                else if(msgs_1[0].equals("Replica2"))
                {
                    String remotePort = msgs_1[1];
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(remotePort));

                    BufferedReader cin_insert = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    PrintWriter cout_insert = new PrintWriter(socket.getOutputStream(), true);

                    String msg_sending = "Replica" + ";" + msgs[0];
//					Log.v("Send Replica2 request", msg_sending);

                    cout_insert.println(msg_sending);
                    cout_insert.flush();

                    ack = cin_insert.readLine();
                    socket.close();
                    if(ack==null)
                    {
//						Log.v("Replica2 failed", remotePort);
                        failed_avd = remotePort;
                        flag=true;
                    }
                }
                else
                {
                    Log.v("Something went wrong", "in insert");
                }
            }
            catch (IOException e) {e.printStackTrace();}
            return null;
        }
    }

    private class ClientTask_Query extends AsyncTask<String, Void, Cursor> {
        @Override
        protected Cursor doInBackground(String... msgs) {

            String[] values1 = {key,value};
            String values_got = "";
            MatrixCursor matrixCursor = new MatrixCursor(values1);

            try {
                if(msgs[0].equals("query_all"))
                {
                    for (int i = 0;i<avd_joints.size();i++)
                    {
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(avd_joints.get(i)));
                        String msg_sending = "QueryAll" + ";" + "@";
//						Log.v("Sending QueryAll", msg_sending);

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
//								Log.v("Client query", key_value[0] + " " + key_value[1]);
                                matrixCursor.addRow(new Object[]{key_value[0], key_value[1]});
                            }
                        }
                        socket.close();
                    }
                }
                else {
                    String delims = "[;]";

                    String[] avd_no = msgs[1].split(delims);
                    for (int i = 0; i < 3; ) {

                        String remotePort = avd_no[i];
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(remotePort));

                        String msg_sending = "Query" + ";" + msgs[0];
						Log.v("Sending Query request", msg_sending+";"+remotePort);

                        PrintWriter cout_query = new PrintWriter(socket.getOutputStream(), true);
                        BufferedReader cin_query = new BufferedReader(new InputStreamReader(socket.getInputStream()));

                        cout_query.println(msg_sending);
                        cout_query.flush();

                        values_got = cin_query.readLine();
                        if (!(values_got == null)) {
                            String[] values = values_got.split(delims);
//							Log.v("Client query", values[0] + " " + values[1]);
                            matrixCursor.addRow(new Object[]{values[0], values[1]});
                            break;
                        }
                        else {
//							Log.v("Query", "failed");
                            failed_avd = remotePort;
                            i++;
                        }
                        socket.close();
                    }
                }
            }
            catch (IOException e) {e.printStackTrace();}

            Cursor cursor = matrixCursor;
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

    public Uri insert_replica(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub

        long rowID=0;
        Uri uri_new = uri;

        Log.v("Inside Replica",AVD_inst);
        Log.v("Replicated into DB", values.toString());
        rowID = DB.replace(DB_Table, null, values);

        return uri_new;
    }


}
