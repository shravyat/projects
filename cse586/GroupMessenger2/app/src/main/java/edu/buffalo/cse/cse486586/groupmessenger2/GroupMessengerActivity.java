package edu.buffalo.cse.cse486586.groupmessenger2;

import android.app.Activity;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.Bundle;
import android.telephony.TelephonyManager;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.Menu;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.TextView;

import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.text.Collator;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;

import edu.buffalo.cse.cse486586.groupmessenger2.OnPTestClickListener;
import edu.buffalo.cse.cse486586.groupmessenger2.R;

import java.util.Arrays;
//@author SHRAVYA

public class GroupMessengerActivity extends Activity {
    static final String TAG = GroupMessengerActivity.class.getSimpleName();
    static final String REMOTE_PORT0 = "11108";
    static final String REMOTE_PORT1 = "11112";
    static final String REMOTE_PORT2 = "11116";
    static final String REMOTE_PORT3 = "11120";
    static final String REMOTE_PORT4 = "11124";
    static final String[] REMOTE_PORTS = {REMOTE_PORT0, REMOTE_PORT1, REMOTE_PORT2, REMOTE_PORT3, REMOTE_PORT4};
    static final int SERVER_PORT = 10000;

    private static final String KEY_FIELD = "key";
    private static final String VALUE_FIELD = "value";
    static int counter = 0; //counter
    static int seq_no = 0;  //s
    static int key_counter = 0;
    static String AVD_inst = "";

    static LinkedList<ArrayList<String>> buffer = new LinkedList<ArrayList<String>>();

    //referred from OnPTestClickListener.java
    private final Uri newUri = buildUri("content", "edu.buffalo.cse.cse486586.groupmessenger2.provider");

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_group_messenger);

        TelephonyManager tel = (TelephonyManager) this.getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        AVD_inst = myPort;
        try {
            ServerSocket serverSocket;
            serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket");
        }

        final EditText editText = (EditText) findViewById(R.id.editText1);
        final Button button = (Button) findViewById(R.id.button4);  // http://developer.android.com/reference/android/widget/Button.html
        button.setOnClickListener(new View.OnClickListener() {
            public void onClick(View v) {
                String msg = editText.getText().toString() + "\n";
                editText.setText(""); // This is one way to reset the input box.
                TextView localTextView = (TextView) findViewById(R.id.textView1);
                localTextView.append("\t" + msg); // This is one way to display a string
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, myPort);
            }
        });

        TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());
        findViewById(R.id.button1).setOnClickListener(new OnPTestClickListener(tv, getContentResolver()));
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {
        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            try {
                while (true) {
                    String text;
                    Socket cs = serverSocket.accept();

                    //On B-deliver of message <m,mid,j> from pj
                    InputStreamReader sin = new InputStreamReader(cs.getInputStream());
                    BufferedReader sin1 = new BufferedReader(sin);
                    text = sin1.readLine();
                    Log.v("packet received1-sever", text);

                    String delims = "[;]";
                    String[] tokens = text.split(delims);

                    if (tokens.length == 3) {
                        seq_no += 1;
                        String msg_sendseq = AVD_inst + ";" + seq_no + ";" + text;

                        String msg_into_buffer = "u" + ";" + msg_sendseq; //u;avd_inst;seq_no;sender_avd;counter;msg
                        String[] tokens1 = msg_into_buffer.split(delims);
                        ArrayList<String> list_buffer = new ArrayList<String>();

                        for (int i = 0; i < 6; i++) {
                            list_buffer.add(tokens1[i]);
                        }
                        buffer.add(list_buffer);
                        Log.v("Msg added to buffer", msg_into_buffer);

                        //<undeliverable,j,si,i,mid,m> in hold-back queue;
                        //Send <j,si,i,mid,m> to pj;
                        PrintWriter sout = new PrintWriter(cs.getOutputStream(), true);
                        sout.println(msg_sendseq);
                        sout.flush();
                    }


                    else if (tokens.length == 6) {
                        //Put <m, mid, j, si, i, undeliverable>
                        // 0.d;1.avd_inst;2.seq_no;3.sender_avd;4.counter;5.msg
                        Log.v("Last received-server", text);
                        for (int i = 0; i < buffer.size(); i++) {
                            ArrayList<String> list = new ArrayList<String>();
                            list = buffer.get(i);
                            if (list.get(5).equals(tokens[5])) {
//                                if (Integer.parseInt(list.get(2)) < Integer.parseInt(tokens[2])) {
                                    list.set(0, tokens[0]);
                                    list.set(1, tokens[1]);
                                    list.set(2, tokens[2]);
//                                } else {
//                                    list.set(0, tokens[0]);
//                                }
                            }
                        }
                        // Sorting the buffer
                        if (buffer.size() > 1) {
                            for (int k = 1; k < buffer.size(); k++) {
                                for (int k1 = k; k1 > 0; k1--) {
                                    ArrayList<String> list1 = new ArrayList<String>();
                                    if (Integer.parseInt(buffer.get(k1 - 1).get(2)) > Integer.parseInt(buffer.get(k1).get(2))) {
                                        list1 = buffer.get(k1 - 1);
                                        buffer.set(k1 - 1, buffer.get(k1));
                                        buffer.set(k1, list1);
                                    } else if (Integer.parseInt(buffer.get(k1 - 1).get(2)) == Integer.parseInt(buffer.get(k1).get(2))) {
                                        if (Integer.parseInt(buffer.get(k1 - 1).get(3)) > Integer.parseInt(buffer.get(k1).get(3))) {
                                            list1 = buffer.get(k1 - 1);
                                            buffer.set(k1 - 1, buffer.get(k1));
                                            buffer.set(k1, list1);
                                        }
//                                        else if (Integer.parseInt(buffer.get(k1 - 1).get(3)) == Integer.parseInt(buffer.get(k1).get(1))) {
//                                            if (Integer.parseInt(buffer.get(k1 - 1).get(3)) > Integer.parseInt(buffer.get(k1).get(3))) {
//                                                list1 = buffer.get(k1 - 1);
//                                                buffer.set(k1 - 1, buffer.get(k1));
//                                                buffer.set(k1, list1);
//                                            }
//                                        }
                                    }
                                }
                            }
                        }

                        boolean flag = true;
                        for (int i = 0; i < buffer.size(); i++) {
                            ArrayList<String> list2;
                            list2 = buffer.get(i);
                            if (list2.get(0).equals("u")) {
                                flag = false;
                                break;
                            }
                        }

                        if (flag == true) {
                            for (int i = 0; i < buffer.size(); i++) {
                                ArrayList<String> list3 = new ArrayList<String>();

                                for (int a = 0; a < buffer.get(i).size(); a++) {
                                    list3.add(buffer.get(i).get(a));
                                }

                                String message = list3.get(5).toString();
                                seq_no = Integer.parseInt(list3.get(2)) + 1;

                                String msg2 = list3.get(2) + ";" + message + ";" + buffer.size();
                                Log.v("DB Insert", msg2);

                                publishProgress(message);

                                ContentValues newcv = new ContentValues();
                                newcv.put(KEY_FIELD, Integer.toString(key_counter));
                                newcv.put(VALUE_FIELD, message);
                                getContentResolver().insert(newUri, newcv);
                                key_counter++;
                            }
                            buffer.removeAll(buffer);
                        }
                    }

                    else if (tokens.length == 1) {
                        String msg_ack = "ack";
                        if (tokens[0].equals("ping")) {
                            Log.v("Ping Received", tokens[0]);
                            PrintWriter sout_ack = new PrintWriter(cs.getOutputStream(), true);
                            sout_ack.println(msg_ack);
                            Log.v("SEND ACK","ACK");
                            sout_ack.flush();
                        } else if(tokens[0].equals(REMOTE_PORT0)||
                                tokens[0].equals(REMOTE_PORT1)||
                                tokens[0].equals(REMOTE_PORT2)||
                                tokens[0].equals(REMOTE_PORT3)||
                                tokens[0].equals(REMOTE_PORT4)) {

                            for (int a = 0; a < buffer.size(); a++) {
                                if (buffer.get(a).get(3).equals(tokens[0])) //&& buffer.get(a).get(0).equals("u") && buffer.size()>0) {
                                {
                                    buffer.remove(a);
                                    Log.v("Removed avd msgs", tokens[0]);
                                }
                            }
                        }
                    }
                    cs.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            return null;
        }


        protected void onProgressUpdate(String... strings) {
            String strReceived = strings[0].trim();
            TextView remoteTextView = (TextView) findViewById(R.id.textView1);
            remoteTextView.append(strReceived + "\t\n");

//        String filename = "SimpleMessengerOutput";
//        String string = strReceived + "\n";
//        FileOutputStream outputStream;
//
//            try
//            {
//                outputStream = openFileOutput(filename, Context.MODE_PRIVATE);
//                outputStream.write(string.getBytes());
//                outputStream.close();
//            } catch (Exception e) {
//                Log.e(TAG, "File write failed");
//            }
        }
    }







    private class ClientTask extends AsyncTask<String, Void, Void> {


        @Override
        protected Void doInBackground(String... msgs) {
            int count = 0;
            String msg_avd="none";
            int sender_avd = 0;
            counter += 1;
            ArrayList<String> rec_seq = new ArrayList<String>();
            String msg_final_seq = "";


            // Ping to check if avd is alive
            for (int i = 0; i < 5; i++) {
                //0.avd_inst;1.seq_no;2.sender_avd;3.counter;4.msg
                try {
                    String remotePort = REMOTE_PORTS[i];
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(remotePort));
                    String msg_ping = "ping";
                    Log.v("Sending PING", msg_ping);
                    PrintWriter cout_ping = new PrintWriter(socket.getOutputStream(), true);
                    cout_ping.println(msg_ping);
                    cout_ping.flush();

                    InputStreamReader cin = new InputStreamReader(socket.getInputStream());
                    BufferedReader cin1 = new BufferedReader(cin);
                    String msg_ack = cin1.readLine();

                    if (msg_ack== null) {
                        Log.v("MSG ACK","A");
                        msg_avd = remotePort;
                    }

                    socket.close();
                } catch (IOException e) {
                    Log.e(TAG, "ClientTask socket IOException");
                }
            }

            if(!msg_avd.equals("none")) {
                for (int i = 0; i < 5; i++) {
                    try {
                        String remotePort = REMOTE_PORTS[i];
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(remotePort));

                        Log.v("Sending failed avd", msg_avd);
                        PrintWriter cout_ping = new PrintWriter(socket.getOutputStream(), true);
                        cout_ping.println(msg_avd);
                        cout_ping.flush();

                        socket.close();

                    } catch (IOException e) {
                        Log.e(TAG, "ClientTask socket IOException");
                    }
                }
            }



            for (int i = 0; i < 5; i++) {
                try {
                    String remotePort = REMOTE_PORTS[i];
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(remotePort));
                    socket.setSoTimeout(1000);

                    // B-multicast(<m,counter,i>);
                    sender_avd = Integer.parseInt(AVD_inst);
                    Log.v("Message got in client", msgs[0]);

                    String msgToSeq = sender_avd + ";" + counter + ";" + msgs[0];
                    Log.v("Sending packet1-client", msgToSeq);

                    PrintWriter cout = new PrintWriter(socket.getOutputStream(), true);
                    cout.println(msgToSeq);
                    cout.flush();
                    //On receive(<mid,sj>) from pj
                    InputStreamReader cin = new InputStreamReader(socket.getInputStream());
                    BufferedReader cin1 = new BufferedReader(cin);
                    String msg_seq_rec = cin1.readLine();

                    //Log.v("Received Packet2-client", msg_seq_rec);

                    if(msg_seq_rec!=null) {
                        //Add <sj,j> to list of suggested sequence numbers for message mid;
                        String delims = "[;]";
                        String[] tokens = msg_seq_rec.split(delims);
                        if (rec_seq.isEmpty()) {
                            count++;
                            for (int j = 0; j < 5; j++)
                                rec_seq.add(tokens[j]);
                        }
                        else {
                            count++;
                            if (Integer.parseInt(rec_seq.get(1))<=Integer.parseInt(tokens[1])) {
                                if (Integer.parseInt(rec_seq.get(1)) == Integer.parseInt(tokens[1])) {
                                    if (Integer.parseInt(rec_seq.get(0)) > Integer.parseInt(tokens[0])) {
                                        for (int j = 0; j < 5; j++)
                                            rec_seq.set(j, tokens[j]);
                                    }
                                }
                                else if(Integer.parseInt(rec_seq.get(1))<Integer.parseInt(tokens[1]))
                                {
                                    for (int j = 0; j < 5; j++)
                                        rec_seq.set(j, tokens[j]);
                                }
                            }
                        }
                    }
                    else if(msg_seq_rec==null){
                        count++;
                        Log.v("Readline gives","Null");
                    }
                    socket.close();
                }
                catch (SocketTimeoutException e)
                {e.printStackTrace();
                count++;}
                catch (NullPointerException e)
                {e.printStackTrace();}
                catch (IOException e)
                {e.printStackTrace();}
            }

            if (count == 5) {
                if (Integer.parseInt(rec_seq.get(1)) < Integer.parseInt(rec_seq.get(3))) {
                    rec_seq.set(1, rec_seq.get(3));
                    rec_seq.set(0, rec_seq.get(2));
                }

                msg_final_seq = "d";
                for (int k = 0; k < 5; k++)
                    msg_final_seq = msg_final_seq + ";" + rec_seq.get(k);

                Log.v("Last at client", msg_final_seq);
            }




            //Broadcasting final msg to all avds
            for (int i = 0; i < 5; i++) {
                //0.avd_inst;1.seq_no;2.sender_avd;3.counter;4.msg
                try {
                    String remotePort = REMOTE_PORTS[i];
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(remotePort));
                    Log.v("Sending final msg", msg_final_seq);
                    PrintWriter cout = new PrintWriter(socket.getOutputStream(), true);
                    cout.println(msg_final_seq);
                    cout.flush();

                    socket.close();
                } catch (IOException e)
                {Log.e(TAG, "ClientTask socket IOException");}
            }
            return null;
        }
    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.activity_group_messenger, menu);
        return true;
    }
}