package cis5550.generic;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;

public class Worker {
    public static final long PING_INTERVAL = 5000;

    public static void startPingThread(String aId, int aPort, String aCoordinatorIPPort) {
        Thread myPingThread = new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(PING_INTERVAL);
                    String myPingURL = "http://" + aCoordinatorIPPort + "/ping?id=" + aId + "&port=" + aPort;
                    URL myURL = new URL(myPingURL);
                    myURL.getContent();

                } catch (InterruptedException | IOException e) {
                    e.printStackTrace();
                }
            }
        });
        myPingThread.start();
    }
}
