package cis5550.generic;

import cis5550.kvs.datamodels.IPPort;

import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Vector;

public class WorkerTable<K extends Comparable<K>> {
    private static final long MAX_DURATION_MILLIS = 15000;
    private final TreeMap<K, IPPort> theWorkerMap;
    private final TreeMap<K, Long> theLastPing;

    public WorkerTable() {
        theWorkerMap = new TreeMap<>();
        theLastPing = new TreeMap<>();
    }

    public void addOrUpdate(K aKey, String aIP, int aPort) {
        theLastPing.put(aKey, System.currentTimeMillis());
        theWorkerMap.put(aKey, new IPPort(aIP, aPort));
    }

    public String getWorkers() {
        StringBuilder myStringBuilder = new StringBuilder();
        removeExpired();
        myStringBuilder.append(theWorkerMap.size()).append("\n");
        for (K myId : theWorkerMap.keySet()) {
            IPPort myIPPort = theWorkerMap.get(myId);
            myStringBuilder
                    .append(myId)
                    .append(",")
                    .append(myIPPort.ip())
                    .append(":")
                    .append(myIPPort.port())
                    .append("\n");
        }
        return myStringBuilder.toString();
    }

    public Vector<String> getWorkersList() {
        Vector<String> myVector = new Vector<>();
        removeExpired();
        for (K myId : theWorkerMap.keySet()) {
            IPPort myIPPort = theWorkerMap.get(myId);
            myVector.add(myIPPort.ip() + ":" + myIPPort.port());
        }
        return myVector;
    }

    public String buildWorkerTable() {
        StringBuilder myStringBuilder = new StringBuilder();
        removeExpired();

        myStringBuilder.append("<table border=\"1\">");
        myStringBuilder.append("<tr><th>Worker ID</th><th>IP Address</th><th>Port</th></tr>");
        for (K myId : theWorkerMap.keySet()) {
            IPPort myIPPort = theWorkerMap.get(myId);
            myStringBuilder
                    .append("<tr><td><a href=\"http://")
                    .append(myIPPort.ip())
                    .append(":")
                    .append(myIPPort.port())
                    .append("\">")
                    .append(myId)
                    .append("</a></td><td>")
                    .append(myIPPort.ip())
                    .append("</td><td>")
                    .append(myIPPort.port())
                    .append("</td></tr>");
        }
        myStringBuilder.append("</table>");

        return myStringBuilder.toString();
    }

    private void removeExpired() {
        long myCurrentTime = System.currentTimeMillis();
        Set<K> myKeysToRemove = new TreeSet<>();
        theLastPing.forEach((myId, myTime) -> {
            if (myCurrentTime - myTime > MAX_DURATION_MILLIS) {
                theWorkerMap.remove(myId);
                myKeysToRemove.add(myId);
            }
        });
        myKeysToRemove.forEach(theLastPing::remove);
    }
}
