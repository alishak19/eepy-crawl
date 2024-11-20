package cis5550.flame;

import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Serializer;

import java.util.*;

import static cis5550.flame.FlameContextImpl.COLUMN_NAME;
import static cis5550.flame.FlameContextImpl.LOGGER;

public class FlameRDDImpl implements FlameRDD {
    private final KVSClient theKVSClient;
    private final FlameContextImpl theFlameContext;

    private String theTableName;

    public FlameRDDImpl(String aTableName, KVSClient aKVSClient, FlameContextImpl aFlameContext) {
        theTableName = aTableName;
        theKVSClient = aKVSClient;
        theFlameContext = aFlameContext;
    }

    @Override
    public List<String> collect() throws Exception {
        Iterator<Row> myRows = theKVSClient.scan(theTableName);

        List<String> myResults = new LinkedList<>();

        while (myRows.hasNext()) {
            Row myRow = myRows.next();
            myResults.add(myRow.get(COLUMN_NAME));
        }

        return myResults;
    }

    @Override
    public FlameRDD flatMap(StringToIterable lambda) throws Exception {
        String myOutputTable = theFlameContext.invokeOperation(
                theTableName, FlameOperation.FLATMAP, Serializer.objectToByteArray(lambda));
        if (myOutputTable == null) {
            throw new Exception("Failed to invoke flatMap operation");
        }
        return new FlameRDDImpl(myOutputTable, theKVSClient, theFlameContext);
    }

    @Override
    public FlamePairRDD flatMapToPair(StringToPairIterable lambda) throws Exception {
        String myOutputTable = theFlameContext.invokeOperation(
                theTableName, FlameOperation.FLATMAP_TO_PAIR, Serializer.objectToByteArray(lambda));
        if (myOutputTable == null) {
            throw new Exception("Failed to invoke flatMapToPair operation");
        }
        return new FlamePairRDDImpl(myOutputTable, theKVSClient, theFlameContext);
    }

    @Override
    public FlamePairRDD mapToPair(StringToPair lambda) throws Exception {
        String myOutputTable = theFlameContext.invokeOperation(
                theTableName, FlameOperation.MAP_TO_PAIR, Serializer.objectToByteArray(lambda));
        if (myOutputTable == null) {
            throw new Exception("Failed to invoke mapToPair operation");
        }
        return new FlamePairRDDImpl(myOutputTable, theKVSClient, theFlameContext);
    }

    @Override
    public FlameRDD intersection(FlameRDD r) throws Exception {
        if (r instanceof FlameRDDImpl) {
            FlameRDDImpl myOtherRDD = (FlameRDDImpl) r;
            FlameRDDImpl myDistinctThis = this.distinct();
            FlameRDDImpl myDistinctOther = myOtherRDD.distinct();

            String myOutputTable = theFlameContext.invokeOperation(
                    myDistinctThis.theTableName,
                    FlameOperation.INTERSECTION,
                    Serializer.objectToByteArray(myDistinctOther.theTableName));
            if (myOutputTable == null) {
                throw new Exception("Failed to invoke intersection operation");
            }
            return new FlameRDDImpl(myOutputTable, theKVSClient, theFlameContext);
        } else {
            throw new Exception("Unsupported RDD type");
        }
    }

    @Override
    public FlameRDD sample(double f) throws Exception {
        String myOutputTable =
                theFlameContext.invokeOperation(theTableName, FlameOperation.SAMPLE, Serializer.objectToByteArray(f));
        if (myOutputTable == null) {
            throw new Exception("Failed to invoke sample operation");
        }
        return new FlameRDDImpl(myOutputTable, theKVSClient, theFlameContext);
    }

    @Override
    public FlamePairRDD groupBy(StringToString lambda) throws Exception {
        String myOutputTable = theFlameContext.invokeOperation(
                theTableName, FlameOperation.GROUP_BY, Serializer.objectToByteArray(lambda));
        if (myOutputTable == null) {
            throw new Exception("Failed to invoke groupBy operation");
        }

        FlamePairRDDImpl myFlamePairRDD = new FlamePairRDDImpl(myOutputTable, theKVSClient, theFlameContext);
        return myFlamePairRDD.foldByKey("", (a, b) -> a.isEmpty() ? b : a + "," + b);
    }

    @Override
    public FlameRDD filter(StringToBoolean lambda) throws Exception {
        String myOutputTable = theFlameContext.invokeOperation(
                theTableName, FlameOperation.FILTER, Serializer.objectToByteArray(lambda));
        if (myOutputTable == null) {
            throw new Exception("Failed to invoke filter operation");
        }
        return new FlameRDDImpl(myOutputTable, theKVSClient, theFlameContext);
    }

    @Override
    public FlameRDD mapPartitions(IteratorToIterator lambda) throws Exception {
        String myOutputTable = theFlameContext.invokeOperation(
                theTableName, FlameOperation.MAP_PARTITIONS, Serializer.objectToByteArray(lambda));
        if (myOutputTable == null) {
            throw new Exception("Failed to invoke mapPartitions operation");
        }
        return new FlameRDDImpl(myOutputTable, theKVSClient, theFlameContext);
    }

    @Override
    public int count() throws Exception {
        return theKVSClient.count(theTableName);
    }

    @Override
    public void saveAsTable(String tableNameArg) throws Exception {
        if (!theKVSClient.rename(theTableName, tableNameArg)) {
            throw new Exception("Failed to rename table");
        }
        theTableName = tableNameArg;
    }

    /**
     * Returns a new RDD that contains only the unique elements of the original RDD. Internally, the row key for each
     * value will be the same as the value. This ensures that when we perform intersect, we can simply scan the table
     * for the range of keys that we are interested in.
     */
    @Override
    public FlameRDDImpl distinct() throws Exception {
        String myOutputTable = theFlameContext.invokeOperation(theTableName, FlameOperation.DISTINCT, new byte[]{});
        if (myOutputTable == null) {
            throw new Exception("Failed to invoke distinct operation");
        }
        return new FlameRDDImpl(myOutputTable, theKVSClient, theFlameContext);
    }

    @Override
    public void destroy() throws Exception {
        theKVSClient.delete(theTableName);
    }

    @Override
    public Vector<String> take(int num) throws Exception {
        Iterator<Row> myRows = theKVSClient.scan(theTableName);
        Vector<String> myResults = new Vector<>();
        while (myRows.hasNext() && myResults.size() < num) {
            myResults.add(myRows.next().get(COLUMN_NAME));
        }
        return myResults;
    }

    @Override
    public String fold(String zeroElement, FlamePairRDD.TwoStringsToString lambda) throws Exception {
        return theFlameContext.invokeFold(theTableName, lambda, zeroElement);
    }
}
