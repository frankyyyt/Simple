package edu.buffalo.cse.cse486586.simpledynamo.Client;

import android.util.Log;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;

import edu.buffalo.cse.cse486586.simpledynamo.Common.Message;

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
            ObjectOutputStream out = new ObjectOutputStream(
                    new BufferedOutputStream(socket.getOutputStream()));

            // Write message
            out.writeObject(msg);
            out.flush();

            Log.d(TAG, "Send from Client Thread");

            // Try not kill the socket in here
            // Close socket
            // socket.close();


        } catch (UnknownHostException e) {
            Log.e(TAG, e.toString());
        } catch (IOException e) {
            Log.e(TAG, e.toString());
        }
    }
}