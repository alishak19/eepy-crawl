package cis5550.kvs;

import cis5550.kvs.datamodels.IPPort;
import cis5550.tools.Logger;
import cis5550.webserver.Request;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.List;

public class WorkerReplicationManager {
    public static final Logger LOGGER = Logger.getLogger(WorkerReplicationManager.class);
    public static final int NUM_REPLICAS = 2;
    public static final int PING_INTERVAL = 5000;

    private final SortedMap<String, IPPort> theReplicaWorkerAddresses;
    private final IPPort theCoordinatorAddr;
    private final String theName;

    public WorkerReplicationManager(String aName, IPPort aCoordinatorAddr) {
        theName = aName;
        theReplicaWorkerAddresses = new ConcurrentSkipListMap<>();
        theCoordinatorAddr = aCoordinatorAddr;

        startReplicationThread();
    }

    public void startReplicationThread() {
        Thread myReplicationThread = new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(PING_INTERVAL);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                SortedMap<String, IPPort> myAllWorkers = getWorkersFromCoordinator();
                setMyReplicas(myAllWorkers);
            }
        });
        myReplicationThread.start();
    }

    private SortedMap<String, IPPort> getWorkersFromCoordinator() {
        SortedMap<String, IPPort> myAllWorkers = new ConcurrentSkipListMap<>();
        try {
            URL myURL = URI.create("http://" + theCoordinatorAddr.ip() + ":" + theCoordinatorAddr.port() + "/workers")
                    .toURL();

            HttpURLConnection myConnection = (HttpURLConnection) myURL.openConnection();
            myConnection.setRequestMethod("GET");
            myConnection.connect();
            BufferedReader myReader = new BufferedReader(new InputStreamReader(myConnection.getInputStream()));
            while (myReader.ready()) {
                String myLine = myReader.readLine();
                if (myLine == null) {
                    break;
                }
                String[] myParts = myLine.split(",");
                if (myParts.length != 2) {
                    continue;
                }
                String myID = myParts[0];
                IPPort myAddr = IPPort.fromString(myParts[1]);
                myAllWorkers.put(myID, myAddr);
            }
        } catch (IOException e) {
            LOGGER.info("Failed to get workers from coordinator");
        }

        return myAllWorkers;
    }

    private void setMyReplicas(SortedMap<String, IPPort> aAllWorkers) {
        theReplicaWorkerAddresses.clear();
        int myIndex = 0;
        List<String> myWorkerKeys = new ArrayList<>(aAllWorkers.keySet());
        Collections.reverse(myWorkerKeys);

        for (String myID : myWorkerKeys) {
            if (myIndex >= NUM_REPLICAS) {
                break;
            }
            if (myID.compareTo(theName) < 0) {
                theReplicaWorkerAddresses.put(myID, aAllWorkers.get(myID));
                myIndex++;
            }
        }
        for (String myID : myWorkerKeys) {
            if (myIndex >= NUM_REPLICAS) {
                break;
            }
            if (!myID.equals(theName)) {
                theReplicaWorkerAddresses.put(myID, aAllWorkers.get(myID));
                myIndex++;
            }
        }
    }

    public void forwardPuts(Request aRequest) {
        for (IPPort myAddr : theReplicaWorkerAddresses.values()) {
            try {
                URL myURL = URI.create("http://" + myAddr.ip() + ":" + myAddr.port() + aRequest.url() + "?"
                                + buildQueryParams(aRequest))
                        .toURL();
                HttpURLConnection myConnection = (HttpURLConnection) myURL.openConnection();
                myConnection.setRequestMethod("PUT");
                myConnection.setDoOutput(true);
                myConnection.getOutputStream().write(aRequest.bodyAsBytes());
                myConnection.connect();
                int myResponseCode = myConnection.getResponseCode();
            } catch (IOException e) {
                LOGGER.error("Failed to forward put to replica", e);
            }
        }
    }

    private String buildQueryParams(Request aRequest) {
        StringBuilder myStringBuilder = new StringBuilder();
        for (String myKey : aRequest.queryParams()) {
            myStringBuilder
                    .append(myKey)
                    .append("=")
                    .append(aRequest.queryParams(myKey))
                    .append("&");
        }
        myStringBuilder.append("replica=true");
        return myStringBuilder.toString();
    }
}
