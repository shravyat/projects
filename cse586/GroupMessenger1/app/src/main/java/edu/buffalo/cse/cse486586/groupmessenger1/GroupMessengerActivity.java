package edu.buffalo.cse.cse486586.groupmessenger1;

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
import java.net.UnknownHostException;

public class GroupMessengerActivity extends Activity {
    static final String TAG = GroupMessengerActivity.class.getSimpleName();
    static final String REMOTE_PORT0 = "11108";
    static final String REMOTE_PORT1 = "11112";
    static final String REMOTE_PORT2 = "11116";
    static final String REMOTE_PORT3 = "11120";
    static final String REMOTE_PORT4 = "11124";
    static final String[] REMOTE_PORTS = {REMOTE_PORT0,REMOTE_PORT1,REMOTE_PORT2,REMOTE_PORT3,REMOTE_PORT4};
    static final int SERVER_PORT = 10000;

    private static final String KEY_FIELD = "key";
    private static final String VALUE_FIELD = "value";
    static int counter = 0;

    //referred from OnPTestClickListener.java
    private final Uri newUri = buildUri("content", "edu.buffalo.cse.cse486586.groupmessenger1.provider");

    private Uri buildUri(String scheme, String authority)
    {
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

        try
        {
            ServerSocket serverSocket;
            serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        }
        catch (IOException e)
        {

            Log.e(TAG, "Can't create a ServerSocket");
        }

        final EditText editText = (EditText) findViewById(R.id.editText1);
        final Button button = (Button) findViewById(R.id.button4);  // http://developer.android.com/reference/android/widget/Button.html
        button.setOnClickListener(new View.OnClickListener()
        {
            public void onClick(View v)
            {
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

    private class ServerTask extends AsyncTask<ServerSocket, String, Void>
    {
        @Override
        protected Void doInBackground(ServerSocket... sockets)
        {
            ServerSocket serverSocket = sockets[0];
            try
            {
                while(true)
                {
                    String text;                                //referred from http://www.oracle.com/technetwork/java/socket-140484.html
                    Socket cs = serverSocket.accept();
                    InputStreamReader cin = new InputStreamReader(cs.getInputStream());
                    BufferedReader sin = new BufferedReader(cin);
                    text = sin.readLine();
                    publishProgress(text);

                    ContentValues newcv = new ContentValues();

                    newcv.put(KEY_FIELD, Integer.toString(counter));
                    newcv.put(VALUE_FIELD, text.toString());

                    getContentResolver().insert(newUri, newcv);
                    counter++;
                    cs.close();
                }
            }
            catch(IOException e)
            {
                e.printStackTrace();
            }
            return null;
        }

        protected void onProgressUpdate(String...strings) {

            String strReceived = strings[0].trim();
            TextView remoteTextView = (TextView) findViewById(R.id.textView1);
            remoteTextView.append(strReceived + "\t\n");

            String filename = "SimpleMessengerOutput";
            String string = strReceived + "\n";
            FileOutputStream outputStream;

            try
            {
                outputStream = openFileOutput(filename, Context.MODE_PRIVATE);
                outputStream.write(string.getBytes());
                outputStream.close();
            } catch (Exception e)
            {
                Log.e(TAG, "File write failed");
            }
            return;
        }
    }


    private class ClientTask extends AsyncTask<String, Void, Void>
    {
        @Override
        protected Void doInBackground(String... msgs)
        {
            try {
                for(int i=0;i<5;i++)
                {
                    String remotePort = REMOTE_PORTS[i];
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(remotePort));
                    String msgToSend = msgs[0];
                    PrintWriter cout = new PrintWriter(socket.getOutputStream(), true);  //referred from http://www.oracle.com/technetwork/java/socket-140484.html
                    cout.println(msgToSend);
                    cout.flush();
                    socket.close();
                }
            }
            catch (UnknownHostException e)
            {
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch (IOException e)
            {
                Log.e(TAG, "ClientTask socket IOException");
            }
            return null;
        }
    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu)
    {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.activity_group_messenger, menu);
        return true;
    }
}
