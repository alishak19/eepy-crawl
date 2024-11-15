package cis5550.kvs.datastore;

import cis5550.kvs.Row;
import cis5550.kvs.datamodels.OpStatus;

import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Stream;

public class DatastoreContainer implements Datastore {
    public static final String PERSISTENT_PREFIX = "pt-";

    private final Datastore theInMemoryDatastore;
    private final Datastore thePersistentDatastore;

    public DatastoreContainer(String aDirectory) {
        theInMemoryDatastore = new InMemoryDatastore();
        thePersistentDatastore = new PersistentDatastore(aDirectory);
    }

    @Override
    public int put(String aTable, String aKey, String aColumn, byte[] aValue) {
        if (isPersistent(aTable)) {
            return thePersistentDatastore.put(aTable, aKey, aColumn, aValue);
        } else {
            return theInMemoryDatastore.put(aTable, aKey, aColumn, aValue);
        }
    }

    @Override
    public Row get(String aTable, String aKey) {
        if (isPersistent(aTable)) {
            return thePersistentDatastore.get(aTable, aKey);
        } else {
            return theInMemoryDatastore.get(aTable, aKey);
        }
    }

    @Override
    public Row get(String aTable, String aKey, int aVersion) {
        if (isPersistent(aTable)) {
            return thePersistentDatastore.get(aTable, aKey, aVersion);
        } else {
            return theInMemoryDatastore.get(aTable, aKey, aVersion);
        }
    }

    @Override
    public int getVersion(String aTable, String aKey) {
        if (isPersistent(aTable)) {
            return thePersistentDatastore.getVersion(aTable, aKey);
        } else {
            return theInMemoryDatastore.getVersion(aTable, aKey);
        }
    }

    @Override
    public Map<String, Integer> getTables() {
        Map<String, Integer> myAllTables = theInMemoryDatastore.getTables();
        myAllTables.putAll(thePersistentDatastore.getTables());
        return myAllTables;
    }

    @Override
    public SortedMap<String, Row> getRows(String aTable, String aFromRow, int aNumRows) {
        if (isPersistent(aTable)) {
            return thePersistentDatastore.getRows(aTable, aFromRow, aNumRows);
        } else {
            return theInMemoryDatastore.getRows(aTable, aFromRow, aNumRows);
        }
    }

    @Override
    public Stream<Row> getRowDataStream(String aTable, String aStartRow, String aEndRowExclusive) {
        if (isPersistent(aTable)) {
            return thePersistentDatastore.getRowDataStream(aTable, aStartRow, aEndRowExclusive);
        } else {
            return theInMemoryDatastore.getRowDataStream(aTable, aStartRow, aEndRowExclusive);
        }
    }

    @Override
    public OpStatus delete(String aTable) {
        if (isPersistent(aTable)) {
            return thePersistentDatastore.delete(aTable);
        } else {
            return theInMemoryDatastore.delete(aTable);
        }
    }

    @Override
    public OpStatus rename(String aTable, String aNewName) {
        if (isPersistent(aTable)) {
            if (!isPersistent(aNewName)) {
                return OpStatus.WRONG_NAME_FORMAT;
            }
            return thePersistentDatastore.rename(aTable, aNewName);
        } else {
            ConcurrentMap<String, Row> myTable = theInMemoryDatastore.getMap(aTable);
            OpStatus myResult = thePersistentDatastore.fromMap(aNewName, myTable);
            if (myResult == OpStatus.SUCCESS) {
                theInMemoryDatastore.delete(aTable);
            }
            return myResult;
        }
    }

    @Override
    public int count(String aTable) {
        return isPersistent(aTable) ? thePersistentDatastore.count(aTable) : theInMemoryDatastore.count(aTable);
    }

    @Override
    public OpStatus fromMap(String aTableName, ConcurrentMap<String, Row> aTable) {
        if (isPersistent(aTableName)) {
            return thePersistentDatastore.fromMap(aTableName, aTable);
        } else {
            return theInMemoryDatastore.fromMap(aTableName, aTable);
        }
    }

    @Override
    public ConcurrentMap<String, Row> getMap(String aTableName) {
        return isPersistent(aTableName) ? thePersistentDatastore.getMap(aTableName) : theInMemoryDatastore.getMap(aTableName);
    }

    private boolean isPersistent(String aTable) {
        return aTable.startsWith(PERSISTENT_PREFIX);
    }
}
