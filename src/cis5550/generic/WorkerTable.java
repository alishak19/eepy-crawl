package cis5550.generic;

import cis5550.kvs.datamodels.IPPort;

import java.util.Set;
import java.util.TreeSet;
import java.util.Vector;
import java.util.concurrent.ConcurrentSkipListMap;

public class WorkerTable<K extends Comparable<K>> {
    private static final long MAX_DURATION_MILLIS = 150000;
    private final ConcurrentSkipListMap<K, IPPort> theWorkerMap;
    private final ConcurrentSkipListMap<K, Long> theLastPing;

    public WorkerTable() {
        theWorkerMap = new ConcurrentSkipListMap<>();
        theLastPing = new ConcurrentSkipListMap<>();
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

        myStringBuilder.append("""
        <style>
            table {
                border-collapse: collapse;
                width: 100%;
                margin: 20px 0;
                font-size: 16px;
                text-align: left;
            }
            th, td {
                border: 1px solid #ddd;
                padding: 8px;
            }
            th {
                background-color: #f4f4f4;
                font-weight: bold;
            }
            tr:hover {
                background-color: #f1f1f1;
            }
        </style>
        <table>
            <tr>
                <th>Worker ID</th>
                <th>IP Address</th>
                <th>Port</th>
            </tr>
    """);

        for (K myId : theWorkerMap.keySet()) {
            IPPort myIPPort = theWorkerMap.get(myId);
            myStringBuilder.append("<tr>")
                    .append("<td><a href=\"http://")
                    .append(myIPPort.ip())
                    .append(":")
                    .append(myIPPort.port())
                    .append("\">")
                    .append(myId)
                    .append("</a></td>")
                    .append("<td>")
                    .append(myIPPort.ip())
                    .append("</td>")
                    .append("<td>")
                    .append(myIPPort.port())
                    .append("</td>")
                    .append("</tr>");
        }

        myStringBuilder.append("</table>");
        return myStringBuilder.toString();
    }

    private synchronized void removeExpired() {
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
