package edu.buffalo.cse.cse486586.simpledynamo.Common;

import android.os.Handler;
import android.os.Looper;

/**
 * SimpleDynamo
 * <p/>
 * Created by darrenxyli on 4/9/15.
 * Changed by darrenxyli on 4/9/15 1:13 PM.
 */
public class LooperThread extends Thread {
    public Handler mHandler;

    @Override
    public void run() {
        Looper.prepare();

        mHandler = new Handler();

        Looper.loop();
    }

    /**
     * Simply post the quit runnable to the thread's event loop
     */
    public void quit() {
        mHandler.post(new QuitLooper());
    }

    /**
     * Inner Runnable which can be posted to the handler
     */
    class QuitLooper implements Runnable {
        @Override
        public void run() {
            // Causes the loop() method to terminate without processing any more messages
            // in the message queue. Any attempt to post messages to the queue after the
            // looper is asked to quit will fail. For example, the sendMessage(Message)
            // method will return false. Using this method may be unsafe because some
            // messages may not be delivered before the looper terminates. Consider using
            // quitSafely() instead to ensure that all pending work is completed in an orderly manner.
            // Looper.myLooper().quit();


            // Quits the looper safely.
            // Causes the loop() method to terminate as soon as all remaining messages
            // in the message queue that are already due to be delivered have been handled.
            // However pending delayed messages with due times in the future will not be
            // delivered before the loop terminates. Any attempt to post messages to
            // the queue after the looper is asked to quit will fail.
            // For example, the sendMessage(Message) method will return false.
            Looper.myLooper().quitSafely();

        }
    }
}