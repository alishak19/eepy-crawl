package cis5550.flame;

import cis5550.kvs.KVSClient;
import cis5550.tools.*;

import java.io.IOException;
import java.io.Serializable;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.ConcurrentLinkedDeque;

import static cis5550.flame.Coordinator.getFlameWorkers;

public class FlameContextImpl implements FlameContext, Serializable {
    public static final Logger LOGGER = Logger.getLogger(FlameContextImpl.class);

    public static final String COLUMN_NAME = "value";

    private final StringBuilder theOutputStringBuilder;
    private final String theJarName;
    private final String theKVSCoordinatorIpPort;

    private int theNextJobId = 1;

    public FlameContextImpl(String aJarName, String aKVSCoordinatorIpPort) {
        theJarName = aJarName;
        theKVSCoordinatorIpPort = aKVSCoordinatorIpPort;
        theOutputStringBuilder = new StringBuilder();
    }

    @Override
    public KVSClient getKVS() {
        return new KVSClient(theKVSCoordinatorIpPort);
    }

    @Override
    public void output(String s) {
        theOutputStringBuilder.append(s);
    }

    @Override
    public FlameRDD parallelize(List<String> list) throws Exception {
        String myJobId = getNewTableName();
        for (int i = 0; i < list.size(); i++) {
            getKVS().put(myJobId, Hasher.hash(String.valueOf(i)), COLUMN_NAME, list.get(i));
        }

        return new FlameRDDImpl(myJobId, getKVS(), this);
    }

    @Override
    public FlameRDD fromTable(String tableName, RowToString lambda) throws Exception {
        String myOutputTable =
                invokeOperation(tableName, FlameOperation.FROM_TABLE, Serializer.objectToByteArray(lambda));
        if (myOutputTable == null) {
            throw new Exception("Failed to invoke fromTable operation");
        }
        return new FlameRDDImpl(myOutputTable, getKVS(), this);
    }

    public String getOutput() {
        if (theOutputStringBuilder.isEmpty()) {
            return "No output";
        }
        return theOutputStringBuilder.toString();
    }

    public String invokeOperation(
            String aInputTable, FlameOperation aFlameOperation, byte[] aLambda, String aZeroElement) {
        String myOutputTable = getNewTableName();

        ConcurrentLinkedDeque<HTTP.Response> myResponses = new ConcurrentLinkedDeque<>();
        List<Thread> myThreads = new LinkedList<>();

        Vector<Partitioner.Partition> myPartitions = generatePartitions();

        for (Partitioner.Partition myPartition : myPartitions) {
            Thread myThread = sendOperationToWorker(
                    aFlameOperation,
                    aLambda,
                    aInputTable,
                    myOutputTable,
                    myPartition,
                    aZeroElement,
                    myResponses);
            myThreads.add(myThread);
            myThread.start();
        }

        for (Thread myThread : myThreads) {
            try {
                myThread.join();
            } catch (InterruptedException e) {
                LOGGER.error("Failed to join thread", e);
            }
        }

        if (myResponses.size() != myThreads.size()) {
            LOGGER.error("Failed to send operation to all workers");
            return null;
        }

        for (HTTP.Response myResponse : myResponses) {
            if (myResponse == null || myResponse.statusCode() != 200) {
                LOGGER.error("Operation failed on at least one worker with status code: " + (myResponse == null ? "response is null" : myResponse.statusCode()));
                return null;
            }
        }

        return myOutputTable;
    }

    public String invokeOperation(String aInputTable, FlameOperation aFlameOperation, byte[] aLambda) {
        return invokeOperation(aInputTable, aFlameOperation, aLambda, null);
    }

    public String invokeFold(String aInputTable, FlamePairRDD.TwoStringsToString aLambda, String aZeroElement) {
        ConcurrentLinkedDeque<HTTP.Response> myResponses = new ConcurrentLinkedDeque<>();
        List<Thread> myThreads = new LinkedList<>();

        Vector<Partitioner.Partition> myPartitions = generatePartitions();

        for (Partitioner.Partition myPartition : myPartitions) {
            Thread myThread = sendOperationToWorker(
                    FlameOperation.FOLD,
                    Serializer.objectToByteArray(aLambda),
                    aInputTable,
                    "",
                    myPartition,
                    aZeroElement,
                    myResponses);
            myThreads.add(myThread);
            myThread.start();
        }

        for (Thread myThread : myThreads) {
            try {
                myThread.join();
            } catch (InterruptedException e) {
                LOGGER.error("Failed to join thread", e);
            }
        }

        if (myResponses.size() != myThreads.size()) {
            LOGGER.error("Failed to send operation to all workers");
            return null;
        }

        List<String> myWorkerResults = new LinkedList<>();

        for (HTTP.Response myResponse : myResponses) {
            if (myResponse == null || myResponse.statusCode() != 200) {
                LOGGER.error("Operation failed on at least one worker");
                return null;
            }

            myWorkerResults.add(new String(myResponse.body()));
        }

        return myWorkerResults.stream().reduce(aZeroElement, aLambda::op);
    }

    private Thread sendOperationToWorker(
            FlameOperation aFlameOperation,
            byte[] aLambda,
            String aInputTable,
            String aOutputTable,
            Partitioner.Partition aPartition,
            String aZeroElement,
            ConcurrentLinkedDeque<HTTP.Response> myResponses) {
        return new Thread(() -> {
            try {
                StringBuilder myWorkerQuery = new StringBuilder();
                myWorkerQuery
                        .append("http://")
                        .append(aPartition.assignedFlameWorker)
                        .append(aFlameOperation.getPath());
                myWorkerQuery.append("?");
                myWorkerQuery.append("inputTable=").append(URLEncoder.encode(aInputTable, StandardCharsets.UTF_8));
                myWorkerQuery.append("&");
                myWorkerQuery.append("outputTable=").append(URLEncoder.encode(aOutputTable, StandardCharsets.UTF_8));
                myWorkerQuery.append("&");
                myWorkerQuery
                        .append("kvsCoordinator=")
                        .append(URLEncoder.encode(getKVS().getCoordinator(), StandardCharsets.UTF_8));
                if (aPartition.fromKey != null) {
                    myWorkerQuery.append("&");
                    myWorkerQuery
                            .append("fromKey=")
                            .append(URLEncoder.encode(aPartition.fromKey, StandardCharsets.UTF_8));
                }
                if (aPartition.toKeyExclusive != null) {
                    myWorkerQuery.append("&");
                    myWorkerQuery
                            .append("toKeyExclusive=")
                            .append(URLEncoder.encode(aPartition.toKeyExclusive, StandardCharsets.UTF_8));
                }
                if (aZeroElement != null) {
                    myWorkerQuery.append("&");
                    myWorkerQuery
                            .append("zeroElement=")
                            .append(URLEncoder.encode(aZeroElement, StandardCharsets.UTF_8));
                }
                myResponses.add(HTTP.doRequest("POST", myWorkerQuery.toString(), aLambda));
            } catch (IOException e) {
                LOGGER.error("Failed to send operation to worker", e);
            }
        });
    }

    private Vector<Partitioner.Partition> generatePartitions() {
        Partitioner myPartitioner = new Partitioner();

        List<String> myKVSWorkerIDs = new LinkedList<>();
        List<String> myKVSWorkerAddresses = new LinkedList<>();

        try {
            int myNumKVSWorkers = getKVS().numWorkers();
            for (int i = 0; i < myNumKVSWorkers; i++) {
                myKVSWorkerIDs.add(getKVS().getWorkerID(i));
                myKVSWorkerAddresses.add(getKVS().getWorkerAddress(i));
            }
        } catch (IOException e) {
            LOGGER.error("Failed to get KVS workers", e);
        }

        for (int i = 0; i < myKVSWorkerIDs.size(); i++) {
            myPartitioner.addKVSWorker(
                    myKVSWorkerAddresses.get(i),
                    myKVSWorkerIDs.get(i),
                    i < myKVSWorkerIDs.size() - 1 ? myKVSWorkerIDs.get(i + 1) : null);

            if (i == myKVSWorkerIDs.size() - 1) {
                myPartitioner.addKVSWorker(myKVSWorkerAddresses.get(i), null, myKVSWorkerIDs.get(0));
            }
        }

        Vector<String> myFlameWorkers = getFlameWorkers();
        myFlameWorkers.forEach(myPartitioner::addFlameWorker);

        return myPartitioner.assignPartitions();
    }

    private String getNewTableName() {
        return "flame_" + theNextJobId++ + "_" + theJarName + "_" + System.currentTimeMillis();
    }
}
