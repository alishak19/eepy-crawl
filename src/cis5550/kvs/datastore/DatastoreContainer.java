package cis5550.kvs.datastore;

import cis5550.kvs.Row;
import cis5550.kvs.datamodels.OpStatus;
import cis5550.tools.Logger;

import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Stream;

public class DatastoreContainer implements Datastore {
    public static final Logger LOGGER = Logger.getLogger(DatastoreContainer.class);

    EnumMap<DatastoreType, Datastore> theDatastores = new EnumMap<>(DatastoreType.class);

    public DatastoreContainer(String aDirectory) {
        theDatastores.put(DatastoreType.IN_MEMORY, new InMemoryDatastore());
        theDatastores.put(DatastoreType.PERSISTENT, new PersistentDatastore(aDirectory));
        theDatastores.put(DatastoreType.APPEND_ONLY, new AppendOnlyDatastore(aDirectory));
    }

    @Override
    public int put(String aTable, String aKey, String aColumn, byte[] aValue) {
        LOGGER.debug("Putting into table: " + aTable + " key: " + aKey + " column: " + aColumn);
        return theDatastores.get(DatastoreType.fromName(aTable)).put(aTable, aKey, aColumn, aValue);
    }

    @Override
    public int putRow(String aTable, String aKey, Row aRow) {
        LOGGER.debug("Putting row into table: " + aTable + " key: " + aKey);
        return theDatastores.get(DatastoreType.fromName(aTable)).putRow(aTable, aKey, aRow);
    }

    @Override
    public int append(String aTable, String aKey, String aColumn, byte[] aValue, String aDelimiter) {
        LOGGER.debug("Appending to table: " + aTable + " key: " + aKey + " column: " + aColumn);
        return theDatastores.get(DatastoreType.fromName(aTable)).append(aTable, aKey, aColumn, aValue, aDelimiter);
    }

    @Override
    public Row get(String aTable, String aKey) {
        LOGGER.debug("Getting from table: " + aTable + " key: " + aKey);
        return theDatastores.get(DatastoreType.fromName(aTable)).get(aTable, aKey);
    }

    @Override
    public Row get(String aTable, String aKey, int aVersion) {
        LOGGER.debug("Getting from table: " + aTable + " key: " + aKey + " version: " + aVersion);
        return theDatastores.get(DatastoreType.fromName(aTable)).get(aTable, aKey, aVersion);
    }

    @Override
    public int getVersion(String aTable, String aKey) {
        LOGGER.debug("Getting version from table: " + aTable + " key: " + aKey);
        return theDatastores.get(DatastoreType.fromName(aTable)).getVersion(aTable, aKey);
    }

    @Override
    public Map<String, Integer> getTables() {
        LOGGER.debug("Getting tables");
        Map<String, Integer> myAllTables = new HashMap<>();
        for (DatastoreType myType : DatastoreType.values()) {
            myAllTables.putAll(theDatastores.get(myType).getTables());
        }
        return myAllTables;
    }

    @Override
    public SortedMap<String, Row> getRows(String aTable, String aFromRow, int aNumRows) {
        LOGGER.debug("Getting rows from table: " + aTable + " from row: " + aFromRow + " num rows: " + aNumRows);
        return theDatastores.get(DatastoreType.fromName(aTable)).getRows(aTable, aFromRow, aNumRows);
    }

    @Override
    public Stream<Row> getRowDataStream(String aTable, String aStartRow, String aEndRowExclusive) {
        LOGGER.debug("Getting row data stream from table: " + aTable + " start row: " + aStartRow + " end row: " + aEndRowExclusive);
        return theDatastores.get(DatastoreType.fromName(aTable)).getRowDataStream(aTable, aStartRow, aEndRowExclusive);
    }

    @Override
    public OpStatus delete(String aTable) {
        LOGGER.debug("Deleting table: " + aTable);
        return theDatastores.get(DatastoreType.fromName(aTable)).delete(aTable);
    }

    @Override
    public OpStatus rename(String aTable, String aNewName) {
        LOGGER.debug("Renaming table: " + aTable + " to: " + aNewName);
        switch (DatastoreType.fromName(aTable)) {
            case PERSISTENT -> {
                if (DatastoreType.fromName(aNewName) != DatastoreType.PERSISTENT) {
                    return OpStatus.WRONG_NAME_FORMAT;
                }
                return theDatastores.get(DatastoreType.PERSISTENT).rename(aTable, aNewName);
            }
            case IN_MEMORY -> {
                ConcurrentMap<String, Row> myTable = theDatastores.get(DatastoreType.IN_MEMORY).getMap(aTable);
                OpStatus myResult = theDatastores.get(DatastoreType.PERSISTENT).fromMap(aNewName, myTable);
                if (myResult == OpStatus.SUCCESS) {
                    theDatastores.get(DatastoreType.IN_MEMORY).delete(aTable);
                }
                return myResult;
            }
            case APPEND_ONLY -> {
                LOGGER.error("Renaming append only table not supported");
                return OpStatus.SERVER_ERROR;
            }
            default -> {
                LOGGER.error("Unknown table type");
                return OpStatus.SERVER_ERROR;
            }
        }
    }

    @Override
    public int count(String aTable) {
        LOGGER.debug("Counting table: " + aTable);
        return theDatastores.get(DatastoreType.fromName(aTable)).count(aTable);
    }

    @Override
    public OpStatus fromMap(String aTableName, ConcurrentMap<String, Row> aTable) {
        LOGGER.debug("Loading table: " + aTableName);
        return theDatastores.get(DatastoreType.fromName(aTableName)).fromMap(aTableName, aTable);
    }

    @Override
    public ConcurrentMap<String, Row> getMap(String aTableName) {
        LOGGER.debug("Getting map for table: " + aTableName);
        return theDatastores.get(DatastoreType.fromName(aTableName)).getMap(aTableName);
    }
}
