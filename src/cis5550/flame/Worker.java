package cis5550.flame;

import java.util.*;
import java.io.*;

import static cis5550.flame.FlameContextImpl.COLUMN_NAME;
import static cis5550.utils.HTTPStatus.*;
import static cis5550.webserver.Server.*;
import cis5550.tools.Hasher;
import cis5550.tools.Logger;
import cis5550.kvs.*;
import cis5550.webserver.Request;

class Worker extends cis5550.generic.Worker {
    public static Logger LOGGER = Logger.getLogger(Worker.class);

    public static void main(String args[]) {
        if (args.length != 2) {
            System.err.println("Syntax: Worker <port> <coordinatorIP:port>");
            System.exit(1);
        }

        LOGGER.info("Flame worker starting on port " + args[0]);

        int port = Integer.parseInt(args[0]);
        String server = args[1];
        startPingThread(String.valueOf(port), port, server);
        final File myJAR = new File("__worker" + port + "-current.jar");

        port(port);

        post("/useJAR", (request, response) -> {
            FileOutputStream fos = new FileOutputStream(myJAR);
            fos.write(request.bodyAsBytes());
            fos.close();
            return "OK";
        });

        before((req, res) -> {
            LOGGER.debug("Received request: " + req.requestMethod() + " " + req.url());
        });

        post(FlameOperation.FLATMAP.getPath(), (request, response) -> {
            OperationParameters myParams = getAndValidateParams(request, myJAR);

            LOGGER.debug("Received flatMap request, parsed parameters");

            if (myParams == null) {
                setResponseStatus(response, BAD_REQUEST);
                return "Bad request";
            }

            KVSClient myKVS = new KVSClient(myParams.kvsCoordinator());
            Iterator<Row> myRows;

            LOGGER.debug("Getting KVS client and scanning rows");

            try {
                myRows = myKVS.scan(myParams.inputTable(), myParams.fromKey(), myParams.toKeyExclusive());
            } catch (IOException e) {
                LOGGER.debug("Failed to scan rows", e);
                setResponseStatus(response, INTERNAL_SERVER_ERROR);
                return "Internal error";
            }

            LOGGER.debug("Scanned rows");

            FlameRDD.StringToIterable myLambda = (FlameRDD.StringToIterable) myParams.lambda();

            int myI = 0;
            while (myRows.hasNext()) {
                Row myRow = myRows.next();
                String myValue = myRow.get(COLUMN_NAME);
                Iterable<String> myResults = myLambda.op(myValue);

                if (myResults != null) {
                    for (String myResult : myResults) {
                        myKVS.put(myParams.outputTable(), createUniqueRowKey(myRow.key(), myI), COLUMN_NAME, myResult);
                        myI++;
                    }
                }
            }

            setResponseStatus(response, OK);
            return "OK";
        });

        post(FlameOperation.MAP_TO_PAIR.getPath(), (request, response) -> {
            OperationParameters myParams = getAndValidateParams(request, myJAR);

            if (myParams == null) {
                setResponseStatus(response, BAD_REQUEST);
                return "Bad request";
            }

            KVSClient myKVS = new KVSClient(myParams.kvsCoordinator());
            Iterator<Row> myRows;

            try {
                myRows = myKVS.scan(myParams.inputTable(), myParams.fromKey(), myParams.toKeyExclusive());
            } catch (IOException e) {
                LOGGER.debug("Failed to scan rows", e);
                setResponseStatus(response, INTERNAL_SERVER_ERROR);
                return "Internal error";
            }

            FlameRDD.StringToPair myLambda = (FlameRDD.StringToPair) myParams.lambda();

            while (myRows.hasNext()) {
                Row myRow = myRows.next();
                String myValue = myRow.get(COLUMN_NAME);
                FlamePair myResult = myLambda.op(myValue);

                if (myResult != null) {
                    myKVS.put(myParams.outputTable(), myResult._1(), myRow.key(), myResult._2());
                }
            }

            setResponseStatus(response, OK);
            return "OK";
        });

        post(FlameOperation.FOLD_BY_KEY.getPath(), (request, response) -> {
            OperationParameters myParams = getAndValidateParams(request, myJAR);

            if (myParams == null || myParams.zeroElement() == null) {
                setResponseStatus(response, BAD_REQUEST);
                return "Bad request";
            }

            KVSClient myKVS = new KVSClient(myParams.kvsCoordinator());
            Iterator<Row> myRows;

            try {
                myRows = myKVS.scan(myParams.inputTable(), myParams.fromKey(), myParams.toKeyExclusive());
            } catch (IOException e) {
                LOGGER.debug("Failed to scan rows", e);
                setResponseStatus(response, INTERNAL_SERVER_ERROR);
                return "Internal error";
            }

            FlamePairRDD.TwoStringsToString myLambda = (FlamePairRDD.TwoStringsToString) myParams.lambda();

            while (myRows.hasNext()) {
                Row myRow = myRows.next();
                String myAccumulatedValue = myParams.zeroElement();

                for (String myColumn : myRow.columns()) {
                    String myValue = myRow.get(myColumn);
                    myAccumulatedValue = myLambda.op(myAccumulatedValue, myValue);
                }

                if (myAccumulatedValue != null) {
                    myKVS.put(myParams.outputTable(), myRow.key(), COLUMN_NAME, myAccumulatedValue);
                }
            }

            setResponseStatus(response, OK);
            return "OK";
        });

        post(FlameOperation.GROUP_BY.getPath(), (request, response) -> {
            OperationParameters myParams = getAndValidateParams(request, myJAR);

            if (myParams == null) {
                setResponseStatus(response, BAD_REQUEST);
                return "Bad request";
            }

            KVSClient myKVS = new KVSClient(myParams.kvsCoordinator());
            Iterator<Row> myRows;

            try {
                myRows = myKVS.scan(myParams.inputTable(), myParams.fromKey(), myParams.toKeyExclusive());
            } catch (IOException e) {
                LOGGER.debug("Failed to scan rows", e);
                setResponseStatus(response, INTERNAL_SERVER_ERROR);
                return "Internal error";
            }

            FlameRDD.StringToString myLambda = (FlameRDD.StringToString) myParams.lambda();

            while (myRows.hasNext()) {
                Row myRow = myRows.next();
                String myValue = myRow.get(COLUMN_NAME);
                String myKey = myLambda.op(myValue);

                if (myKey != null) {
                    myKVS.put(myParams.outputTable(), myKey, myRow.key(), myValue);
                }
            }

            setResponseStatus(response, OK);
            return "OK";
        });

        post(FlameOperation.SAMPLE.getPath(), (request, response) -> {
            OperationParameters myParams = getAndValidateParams(request, myJAR);

            if (myParams == null) {
                setResponseStatus(response, BAD_REQUEST);
                return "Bad request";
            }

            KVSClient myKVS = new KVSClient(myParams.kvsCoordinator());
            Iterator<Row> myRows;
            double myF = (double) myParams.lambda();

            try {
                myRows = myKVS.scan(myParams.inputTable(), myParams.fromKey(), myParams.toKeyExclusive());
            } catch (IOException e) {
                LOGGER.debug("Failed to scan rows", e);
                setResponseStatus(response, INTERNAL_SERVER_ERROR);
                return "Internal error";
            }

            while (myRows.hasNext()) {
                Row myRow = myRows.next();
                if (Math.random() < myF) {
                    myKVS.put(myParams.outputTable(), myRow.key(), COLUMN_NAME, myRow.get(COLUMN_NAME));
                }
            }

            setResponseStatus(response, OK);
            return "OK";
        });

        post(FlameOperation.INTERSECTION.getPath(), (request, response) -> {
            OperationParameters myParams = getAndValidateParams(request, myJAR);

            if (myParams == null) {
                setResponseStatus(response, BAD_REQUEST);
                return "Bad request";
            }

            KVSClient myKVS = new KVSClient(myParams.kvsCoordinator());
            Iterator<Row> myRows;
            Iterator<Row> myOtherRows;

            try {
                myRows = myKVS.scan(myParams.inputTable(), myParams.fromKey(), myParams.toKeyExclusive());
                myOtherRows = myKVS.scan((String) myParams.lambda(), myParams.fromKey(), myParams.toKeyExclusive());
            } catch (IOException e) {
                LOGGER.debug("Failed to scan rows", e);
                setResponseStatus(response, INTERNAL_SERVER_ERROR);
                return "Internal error";
            }

            Set<String> mySet = new HashSet<>();

            while (myOtherRows.hasNext()) {
                Row myRow = myOtherRows.next();
                mySet.add(myRow.get(COLUMN_NAME));
            }

            while (myRows.hasNext()) {
                Row myRow = myRows.next();
                if (mySet.contains(myRow.get(COLUMN_NAME))) {
                    myKVS.put(myParams.outputTable(), myRow.key(), COLUMN_NAME, myRow.get(COLUMN_NAME));
                }
            }

            setResponseStatus(response, OK);
            return "OK";
        });

        post(FlameOperation.DISTINCT.getPath(), (request, response) -> {
            OperationParameters myParams = getAndValidateParams(request, myJAR);

            if (myParams == null) {
                setResponseStatus(response, BAD_REQUEST);
                return "Bad request";
            }

            KVSClient myKVS = new KVSClient(myParams.kvsCoordinator());
            Iterator<Row> myRows;

            try {
                myRows = myKVS.scan(myParams.inputTable(), myParams.fromKey(), myParams.toKeyExclusive());
            } catch (IOException e) {
                LOGGER.debug("Failed to scan rows", e);
                setResponseStatus(response, INTERNAL_SERVER_ERROR);
                return "Internal error";
            }

            Set<String> mySet = new HashSet<>();

            while (myRows.hasNext()) {
                Row myRow = myRows.next();
                if (!mySet.contains(myRow.get(COLUMN_NAME))) {
                    mySet.add(myRow.get(COLUMN_NAME));
                    myKVS.put(myParams.outputTable(), myRow.get(COLUMN_NAME), COLUMN_NAME, myRow.get(COLUMN_NAME));
                }
            }

            setResponseStatus(response, OK);
            return "OK";
        });

        post(FlameOperation.FROM_TABLE.getPath(), (request, response) -> {
            OperationParameters myParams = getAndValidateParams(request, myJAR);

            if (myParams == null) {
                setResponseStatus(response, BAD_REQUEST);
                return "Bad request";
            }

            KVSClient myKVS = new KVSClient(myParams.kvsCoordinator());
            Iterator<Row> myRows;

            try {
                myRows = myKVS.scan(myParams.inputTable(), myParams.fromKey(), myParams.toKeyExclusive());
            } catch (IOException e) {
                LOGGER.debug("Failed to scan rows", e);
                setResponseStatus(response, INTERNAL_SERVER_ERROR);
                return "Internal error";
            }

            FlameContext.RowToString myLambda = (FlameContext.RowToString) myParams.lambda();

            Map<String, String> myRowValueMap = new HashMap<>();
            while (myRows.hasNext()) {
                Row myRow = myRows.next();
                String myRowKey = myRow.key();
                String myValue = myLambda.op(myRow);
                if (myValue != null) {
                    myRowValueMap.put(myRowKey, myValue);
                }
            }

            myKVS.batchPut(myParams.outputTable(), COLUMN_NAME, myRowValueMap);

            /* Keeping this for if the batches are buggy - delete after more testing ! */
//            while (myRows.hasNext()) {
//                Row myRow = myRows.next();
//                String myValue = myLambda.op(myRow);
//                if (myValue != null) {
//                    myKVS.put(myParams.outputTable(), myRow.key(), COLUMN_NAME, myValue);
//                }
//            }

            setResponseStatus(response, OK);
            return "OK";
        });

        post(FlameOperation.PAIR_FROM_TABLE.getPath(), (request, response) -> {
            OperationParameters myParams = getAndValidateParams(request, myJAR);

            if (myParams == null) {
                setResponseStatus(response, BAD_REQUEST);
                return "Bad request";
            }

            KVSClient myKVS = new KVSClient(myParams.kvsCoordinator());
            Iterator<Row> myRows;

            try {
                myRows = myKVS.scan(myParams.inputTable(), myParams.fromKey(), myParams.toKeyExclusive());
            } catch (IOException e) {
                LOGGER.debug("Failed to scan rows", e);
                setResponseStatus(response, INTERNAL_SERVER_ERROR);
                return "Internal error";
            }

            FlameContext.RowToPair myLambda = (FlameContext.RowToPair) myParams.lambda();

            while (myRows.hasNext()) {
                Row myRow = myRows.next();
                FlamePair myValue = myLambda.op(myRow);
                if (myValue != null) {
                    myKVS.put(myParams.outputTable(), myValue._1(), myRow.key(), myValue._2());
                }
            }

            setResponseStatus(response, OK);
            return "OK";
        });

        post(FlameOperation.FLATMAP_TO_PAIR.getPath(), (request, response) -> {
            OperationParameters myParams = getAndValidateParams(request, myJAR);

            if (myParams == null) {
                setResponseStatus(response, BAD_REQUEST);
                return "Bad request";
            }

            KVSClient myKVS = new KVSClient(myParams.kvsCoordinator());
            Iterator<Row> myRows;

            try {
                myRows = myKVS.scan(myParams.inputTable(), myParams.fromKey(), myParams.toKeyExclusive());
            } catch (IOException e) {
                LOGGER.debug("Failed to scan rows", e);
                setResponseStatus(response, INTERNAL_SERVER_ERROR);
                return "Internal error";
            }

            FlameRDD.StringToPairIterable myLambda = (FlameRDD.StringToPairIterable) myParams.lambda();

            int myI = 0;
            while (myRows.hasNext()) {
                Row myRow = myRows.next();
                String myValue = myRow.get(COLUMN_NAME);
                Iterable<FlamePair> myResults = myLambda.op(myValue);

                if (myResults != null) {
                    for (FlamePair myResult : myResults) {
                        myKVS.put(myParams.outputTable(), myResult._1(), createUniqueRowKey(myRow.key(), myI), myResult._2());
                        myI++;
                    }
                }
            }

            setResponseStatus(response, OK);
            return "OK";
        });

        post(FlameOperation.PAIR_FLATMAP.getPath(), (request, response) -> {
            OperationParameters myParams = getAndValidateParams(request, myJAR);


            if (myParams == null) {
                setResponseStatus(response, BAD_REQUEST);
                return "Bad request";
            }


            KVSClient myKVS = new KVSClient(myParams.kvsCoordinator());
            Iterator<Row> myRows;

            try {
                myRows = myKVS.scan(myParams.inputTable(), myParams.fromKey(), myParams.toKeyExclusive());
            } catch (IOException e) {
                LOGGER.debug("Failed to scan rows", e);
                setResponseStatus(response, INTERNAL_SERVER_ERROR);
                return "Internal error";
            }

            FlamePairRDD.PairToStringIterable myLambda = (FlamePairRDD.PairToStringIterable) myParams.lambda();

            int myI = 0;
            while (myRows.hasNext()) {
                Row myRow = myRows.next();
                for (String myColumn : myRow.columns()) {
                    FlamePair myPair = new FlamePair(myRow.key(), myRow.get(myColumn));
                    Iterable<String> myResults = myLambda.op(myPair);

                    if (myResults != null) {
                        for (String myResult : myResults) {
                            myKVS.put(myParams.outputTable(), createUniqueRowKey(myRow.key(), myI), COLUMN_NAME, myResult);
                            myI++;
                        }
                    }
                }
            }

            setResponseStatus(response, OK);
            return "OK";
        });

        post(FlameOperation.PAIR_FLATMAP_TO_PAIR.getPath(), (request, response) -> {
            OperationParameters myParams = getAndValidateParams(request, myJAR);

            if (myParams == null) {
                setResponseStatus(response, BAD_REQUEST);
                return "Bad request";
            }

            KVSClient myKVS = new KVSClient(myParams.kvsCoordinator());
            Iterator<Row> myRows;

            try {
                myRows = myKVS.scan(myParams.inputTable(), myParams.fromKey(), myParams.toKeyExclusive());
            } catch (IOException e) {
                LOGGER.debug("Failed to scan rows", e);
                setResponseStatus(response, INTERNAL_SERVER_ERROR);
                return "Internal error";
            }

            FlamePairRDD.PairToPairIterable myLambda = (FlamePairRDD.PairToPairIterable) myParams.lambda();

            int myI = 0;
            while (myRows.hasNext()) {
                Row myRow = myRows.next();
                for (String myColumn : myRow.columns()) {
                    FlamePair myPair = new FlamePair(myRow.key(), myRow.get(myColumn));
                    Iterable<FlamePair> myResults = myLambda.op(myPair);

                    if (myResults != null) {
                        for (FlamePair myResult : myResults) {
                            myKVS.put(
                                    myParams.outputTable(),
                                    myResult._1(),
                                    createUniqueRowKey(myRow.key(), myI),
                                    myResult._2());
                            myI++;
                        }
                    }
                }
            }

            setResponseStatus(response, OK);
            return "OK";
        });

        post(FlameOperation.JOIN.getPath(), (request, response) -> {
            OperationParameters myParams = getAndValidateParams(request, myJAR);

            if (myParams == null) {
                setResponseStatus(response, BAD_REQUEST);
                return "Bad request";
            }

            KVSClient myKVS = new KVSClient(myParams.kvsCoordinator());
            String myOtherTable = (String) myParams.lambda();
            Iterator<Row> myRows;

            try {
                myRows = myKVS.scan(myParams.inputTable(), myParams.fromKey(), myParams.toKeyExclusive());
            } catch (IOException e) {
                LOGGER.debug("Failed to scan rows", e);
                setResponseStatus(response, INTERNAL_SERVER_ERROR);
                return "Internal error";
            }

            while (myRows.hasNext()) {
                Row myRow = myRows.next();
                Row myOtherRow = myKVS.getRow(myOtherTable, myRow.key());

                if (myOtherRow != null) {
                    for (String myColumn : myRow.columns()) {
                        for (String myOtherColumn : myOtherRow.columns()) {
                            myKVS.put(
                                    myParams.outputTable(),
                                    myRow.key(),
                                    Hasher.hash(myColumn + "!" + myOtherColumn),
                                    myRow.get(myColumn) + "," + myOtherRow.get(myOtherColumn));
                        }
                    }
                }
            }

            setResponseStatus(response, OK);
            return "OK";
        });

        post(FlameOperation.FOLD.getPath(), (request, response) -> {
            OperationParameters myParams = getAndValidateFoldParams(request, myJAR);

            if (myParams == null) {
                setResponseStatus(response, BAD_REQUEST);
                return "Bad request";
            }

            KVSClient myKVS = new KVSClient(myParams.kvsCoordinator());
            Iterator<Row> myRows;

            try {
                myRows = myKVS.scan(myParams.inputTable(), myParams.fromKey(), myParams.toKeyExclusive());
            } catch (IOException e) {
                LOGGER.debug("Failed to scan rows", e);
                setResponseStatus(response, INTERNAL_SERVER_ERROR);
                return "Internal error";
            }

            FlamePairRDD.TwoStringsToString myLambda = (FlamePairRDD.TwoStringsToString) myParams.lambda();
            String myAccumulatedValue = myParams.zeroElement();

            while (myRows.hasNext()) {
                Row myRow = myRows.next();
                String myValue = myRow.get(COLUMN_NAME);
                myAccumulatedValue = myLambda.op(myAccumulatedValue, myValue);
            }

            setResponseStatus(response, OK);
            return myAccumulatedValue;
        });

        post(FlameOperation.FILTER.getPath(), (request, response) -> {
            OperationParameters myParams = getAndValidateParams(request, myJAR);

            if (myParams == null) {
                setResponseStatus(response, BAD_REQUEST);
                return "Bad request";
            }

            KVSClient myKVS = new KVSClient(myParams.kvsCoordinator());
            Iterator<Row> myRows;
            FlameRDD.StringToBoolean myLambda = (FlameRDD.StringToBoolean) myParams.lambda();

            try {
                myRows = myKVS.scan(myParams.inputTable(), myParams.fromKey(), myParams.toKeyExclusive());
            } catch (IOException e) {
                LOGGER.debug("Failed to scan rows", e);
                setResponseStatus(response, INTERNAL_SERVER_ERROR);
                return "Internal error";
            }

            while (myRows.hasNext()) {
                Row myRow = myRows.next();
                String myValue = myRow.get(COLUMN_NAME);
                if (myLambda.op(myValue)) {
                    myKVS.put(myParams.outputTable(), myRow.key(), COLUMN_NAME, myValue);
                }
            }

            setResponseStatus(response, OK);
            return "OK";
        });

        post(FlameOperation.MAP_PARTITIONS.getPath(), (request, response) -> {
            OperationParameters myParams = getAndValidateParams(request, myJAR);

            if (myParams == null) {
                setResponseStatus(response, BAD_REQUEST);
                return "Bad request";
            }

            KVSClient myKVS = new KVSClient(myParams.kvsCoordinator());
            Iterator<Row> myRows;
            FlameRDD.IteratorToIterator myLambda = (FlameRDD.IteratorToIterator) myParams.lambda();

            try {
                myRows = myKVS.scan(myParams.inputTable(), myParams.fromKey(), myParams.toKeyExclusive());
            } catch (IOException e) {
                LOGGER.debug("Failed to scan rows", e);
                setResponseStatus(response, INTERNAL_SERVER_ERROR);
                return "Internal error";
            }

            List<String> myValues = new LinkedList<>();
            while (myRows.hasNext()) {
                Row myRow = myRows.next();
                myValues.add(myRow.get(COLUMN_NAME));
            }

            Iterator<String> myResults = myLambda.op(myValues.iterator());

            int myI = 0;
            while (myResults.hasNext()) {
                myKVS.put(
                        myParams.outputTable(), createUniqueRowKey(myParams.fromKey(), myI), COLUMN_NAME, myResults.next());
                myI++;
            }

            setResponseStatus(response, OK);
            return "OK";
        });

        post(FlameOperation.COGROUP.getPath(), (request, response) -> {
            LOGGER.error("Cogroup is NOT fully supported and is prone to bugs.");
            OperationParameters myParams = getAndValidateParams(request, myJAR);

            if (myParams == null) {
                setResponseStatus(response, BAD_REQUEST);
                return "Bad request";
            }

            KVSClient myKVS = new KVSClient(myParams.kvsCoordinator());
            String myOtherTable = (String) myParams.lambda();
            Iterator<Row> myRows;

            try {
                myRows = myKVS.scan(myParams.inputTable(), myParams.fromKey(), myParams.toKeyExclusive());
            } catch (IOException e) {
                LOGGER.debug("Failed to scan rows", e);
                setResponseStatus(response, INTERNAL_SERVER_ERROR);
                return "Internal error";
            }

            while (myRows.hasNext()) {
                Row myRow = myRows.next();
                Row myOtherRow = myKVS.getRow(myOtherTable, myRow.key());

                List<String> myRowValues = new LinkedList<>();
                List<String> myOtherRowValues = new LinkedList<>();

                for (String myColumn : myRow.columns()) {
                    myRowValues.add(myRow.get(myColumn));
                }

                if (myOtherRow != null) {
                    for (String myColumn : myOtherRow.columns()) {
                        myOtherRowValues.add(myOtherRow.get(myColumn));
                    }
                }
                myKVS.put(
                        myParams.outputTable(),
                        myRow.key(),
                        COLUMN_NAME,
                        "[" + String.join(",", myRowValues) + "],[" + String.join(",", myOtherRowValues) + "]");
            }

            setResponseStatus(response, OK);
            return "OK";
        });

        after((req, res) -> {
            LOGGER.debug("Completed request: " + req.requestMethod() + " " + req.url());
        });
    }

    private static OperationParameters getAndValidateParams(Request aRequest, File aJar) {
        OperationParameters myParams = OperationParameters.fromRequest(aRequest, aJar);

        if (myParams.kvsCoordinator() == null) {
            return null;
        }

        if (myParams.inputTable() == null || myParams.outputTable() == null) {
            return null;
        }

        return myParams;
    }

    private static OperationParameters getAndValidateFoldParams(Request aRequest, File aJar) {
        OperationParameters myParams = OperationParameters.fromRequest(aRequest, aJar);

        if (myParams.kvsCoordinator() == null) {
            return null;
        }

        if (myParams.inputTable() == null || myParams.zeroElement() == null || myParams.lambda() == null) {
            return null;
        }

        return myParams;
    }

    private static String createUniqueRowKey(String aOriginalRowKey, int aI) {
        return Hasher.hash(aOriginalRowKey+ "!" + aI);
    }
}
