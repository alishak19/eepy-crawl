package cis5550.flame;

import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Serializer;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class FlamePairRDDImpl implements FlamePairRDD {
    private final KVSClient theKVSClient;
    private final FlameContextImpl theFlameContext;

    private String theTableName;

    public FlamePairRDDImpl(String aTableName, KVSClient aKVSClient, FlameContextImpl aFlameContext) {
        theTableName = aTableName;
        theKVSClient = aKVSClient;
        theFlameContext = aFlameContext;
    }

    @Override
    public List<FlamePair> collect() throws Exception {
        Iterator<Row> myRows = theKVSClient.scan(theTableName);

        List<FlamePair> myResults = new LinkedList<>();

        while (myRows.hasNext()) {
            Row myRow = myRows.next();
            for (String myColumn : myRow.columns()) {
                myResults.add(new FlamePair(myRow.key(), myRow.get(myColumn)));
            }
        }

        return myResults;
    }

    @Override
    public FlamePairRDD foldByKey(String zeroElement, TwoStringsToString lambda) throws Exception {
        String myOutputTable = theFlameContext.invokeOperation(
                theTableName,
                FlameOperation.FOLD_BY_KEY,
                Serializer.objectToByteArray(lambda),
                zeroElement);
        if (myOutputTable == null) {
            throw new Exception("Failed to invoke foldByKey operation");
        }
        return new FlamePairRDDImpl(myOutputTable, theKVSClient, theFlameContext);
    }

    @Override
    public void saveAsTable(String tableNameArg) throws Exception {
        if (!theKVSClient.rename(theTableName, tableNameArg)) {
            throw new Exception("Failed to rename table");
        }
        theTableName = tableNameArg;
    }

    @Override
    public FlameRDD flatMap(PairToStringIterable lambda) throws Exception {
        String myOutputTable = theFlameContext.invokeOperation(
                theTableName,
                FlameOperation.PAIR_FLATMAP,
                Serializer.objectToByteArray(lambda));
        if (myOutputTable == null) {
            throw new Exception("Failed to invoke flatMap operation");
        }
        return new FlameRDDImpl(myOutputTable, theKVSClient, theFlameContext);
    }

    @Override
    public void destroy() throws Exception {
        theKVSClient.delete(theTableName);
    }

    @Override
    public FlamePairRDD flatMapToPair(PairToPairIterable lambda) throws Exception {
        String myOutputTable = theFlameContext.invokeOperation(
                theTableName,
                FlameOperation.PAIR_FLATMAP_TO_PAIR,
                Serializer.objectToByteArray(lambda));
        if (myOutputTable == null) {
            throw new Exception("Failed to invoke flatMapToPair operation");
        }
        return new FlamePairRDDImpl(myOutputTable, theKVSClient, theFlameContext);
    }

    @Override
    public void flatMapToPairTable(PairToPairIterable lambda, String tableName, String aColumn) throws Exception {
        String myOutputTable = theFlameContext.invokeOperationWithOutputTable(
                theTableName,
                FlameOperation.PAIR_FLATMAP_TO_PAIR,
                Serializer.objectToByteArray(lambda),
                tableName,
                aColumn);
        if (myOutputTable == null) {
            throw new Exception("Failed to invoke flatMapToPairTable operation");
        }
    }

    @Override
    public FlamePairRDD join(FlamePairRDD other) throws Exception {
        if (!(other instanceof FlamePairRDDImpl)) {
            throw new Exception("Unsupported RDD type");
        }
        FlamePairRDDImpl myOtherRDD = (FlamePairRDDImpl) other;
        String myOutputTable = theFlameContext.invokeOperation(
                theTableName,
                FlameOperation.JOIN,
                Serializer.objectToByteArray(myOtherRDD.theTableName));
        if (myOutputTable == null) {
            throw new Exception("Failed to invoke join operation");
        }
        return new FlamePairRDDImpl(myOutputTable, theKVSClient, theFlameContext);
    }

    @Override
    public FlamePairRDD cogroup(FlamePairRDD other) throws Exception {
        if (!(other instanceof FlamePairRDDImpl)) {
            throw new Exception("Unsupported RDD type");
        }
        FlamePairRDDImpl myOtherRDD = (FlamePairRDDImpl) other;
        String myOutputTable = theFlameContext.invokeOperation(
                theTableName,
                FlameOperation.COGROUP,
                Serializer.objectToByteArray(myOtherRDD.theTableName));
        if (myOutputTable == null) {
            throw new Exception("Failed to invoke cogroup operation");
        }
        return new FlamePairRDDImpl(myOutputTable, theKVSClient, theFlameContext);
    }
}
