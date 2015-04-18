package edu.buffalo.cse.cse486586.simpledynamo.Client;

import android.util.Log;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;

import edu.buffalo.cse.cse486586.simpledynamo.Common.Message;
import edu.buffalo.cse.cse486586.simpledynamo.Utils.SimpleDynamoUtils;

/**
 * SimpleDynamo
 * <p/>
 * Created by darrenxyli on 4/9/15.
 * Changed by darrenxyli on 4/9/15 2:48 PM.
 */
public class SendThread implements Runnable {

    Message msg;
    private String TAG = SendThread.class.getSimpleName();

    public SendThread(Message m) {
        this.msg = m;
    }

    @Override
    public void run() {
        try {

            // Create socket
            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), msg.forwardPort);

            // Create out stream
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());

            // Write message
            out.writeUTF(SimpleDynamoUtils.toJSON(msg));
            out.flush();

            Log.d(TAG, "Send from Client Thread");

        } catch (UnknownHostException e) {
            Log.e(TAG, e.toString());
        } catch (IOException e) {
            Log.e(TAG, e.toString());
        }
    }
}