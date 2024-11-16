package cis5550.webserver;

import cis5550.tools.Logger;

import java.util.concurrent.BlockingQueue;

public class ServerWorkerThread extends Thread {
    private static final Logger LOGGER = Logger.getLogger(ServerWorkerThread.class);

    private final BlockingQueue<Runnable> theTaskQueue;
    private volatile boolean theIsStopped = false;

    public ServerWorkerThread(BlockingQueue<Runnable> aTaskQueue) {
        theTaskQueue = aTaskQueue;
    }

    @Override
    public void run() {
        while (!theIsStopped) {
            try {
                Runnable myTask = theTaskQueue.take();
                myTask.run();
            } catch (InterruptedException e) {
                LOGGER.error("Thread interrupted", e);
                Thread.currentThread().interrupt();
            }
        }
    }

    public synchronized void stopThread() {
        theIsStopped = true;
        this.interrupt();
    }

    public synchronized boolean isStopped() {
        return theIsStopped;
    }
}
