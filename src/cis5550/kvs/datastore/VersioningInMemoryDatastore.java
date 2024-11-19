package cis5550.kvs.datastore;

import cis5550.kvs.datamodels.OpStatus;
import cis5550.kvs.Row;
import cis5550.tools.Logger;

import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Stream;

public class VersioningInMemoryDatastore implements Datastore {
    public static final Logger LOGGER = Logger.getLogger(InMemoryDatastore.class);

    private final ConcurrentMap<String, ConcurrentMap<String, ConcurrentLinkedDeque<Row>>> theMemoryData;

    public VersioningInMemoryDatastore() {
        theMemoryData = new ConcurrentHashMap<>();
    }

    @Override
    public int put(String aTable, String aKey, String aColumn, byte[] aValue) {
        theMemoryData.putIfAbsent(aTable, new ConcurrentSkipListMap<>());
        theMemoryData.get(aTable).putIfAbsent(aKey, new ConcurrentLinkedDeque<>());
        Row myPreviousRow = theMemoryData.get(aTable).get(aKey).peekLast();
        Row myNewRow;
        if (myPreviousRow == null) {
            myNewRow = new Row(aKey);
            myNewRow.put(aColumn, aValue);
        } else {
            myNewRow = myPreviousRow.clone();
            myNewRow.put(aColumn, aValue);
        }
        theMemoryData.get(aTable).get(aKey).add(myNewRow);
        return theMemoryData.get(aTable).get(myNewRow.key()).size();
    }

    @Override
    public int putRow(String aTable, String aKey, Row aRow) {
        theMemoryData.putIfAbsent(aTable, new ConcurrentSkipListMap<>());
        theMemoryData.get(aTable).putIfAbsent(aKey, new ConcurrentLinkedDeque<>());
        theMemoryData.get(aTable).get(aKey).add(aRow);
        return theMemoryData.get(aTable).get(aKey).size();
    }

    @Override
    public Row get(String aTable, String aKey) {
        return get(aTable, aKey, -1);
    }

    @Override
    public Row get(String aTable, String aKey, int aVersion) {

        if (!theMemoryData.containsKey(aTable) || !theMemoryData.get(aTable).containsKey(aKey)) {
            return null;
        }
        if (aVersion == -1) {
            return theMemoryData.get(aTable).get(aKey).peekLast();
        }
        int myI = 1;
        for (Row myRow : theMemoryData.get(aTable).get(aKey)) {
            if (myI == aVersion) {
                return myRow;
            }
            myI++;
        }
        return null;
    }

    @Override
    public int getVersion(String aTable, String aKey) {
        return theMemoryData.get(aTable).get(aKey).size();
    }

    @Override
    public Map<String, Integer> getTables() {
        return theMemoryData.entrySet().stream()
                .collect(
                        ConcurrentHashMap::new,
                        (aMap, aEntry) ->
                                aMap.put(aEntry.getKey(), aEntry.getValue().size()),
                        ConcurrentHashMap::putAll);
    }

    @Override
    public SortedMap<String, Row> getRows(String aTable, String aFromRow, int aNumRows) {
        if (!theMemoryData.containsKey(aTable)) {
            return null;
        }
        return theMemoryData.get(aTable).entrySet().stream()
                .filter(aEntry -> aFromRow == null || aEntry.getKey().compareTo(aFromRow) >= 0)
                .limit(aNumRows + 1)
                .collect(
                        ConcurrentSkipListMap::new,
                        (aMap, aEntry) ->
                                aMap.put(aEntry.getKey(), aEntry.getValue().peekLast()),
                        ConcurrentSkipListMap::putAll);
    }

    @Override
    public Stream<Row> getRowDataStream(String aTable, String aStartRow, String aEndRowExclusive) {
        if (!theMemoryData.containsKey(aTable)) {
            return null;
        }
        return theMemoryData.get(aTable).entrySet().stream()
                .filter(aEntry -> {
                    if (aStartRow != null && aEntry.getKey().compareTo(aStartRow) < 0) {
                        return false;
                    }
                    if (aEndRowExclusive != null && aEntry.getKey().compareTo(aEndRowExclusive) >= 0) {
                        return false;
                    }
                    return true;
                })
                .map(aEntry -> aEntry.getValue().peekLast());
    }

    @Override
    public OpStatus delete(String aTable) {
        if (!theMemoryData.containsKey(aTable)) {
            return OpStatus.TABLE_NOT_FOUND;
        }
        return theMemoryData.remove(aTable, theMemoryData.get(aTable)) ? OpStatus.SUCCESS : OpStatus.SERVER_ERROR;
    }

    @Override
    public OpStatus rename(String aTable, String aNewName) {
        if (!theMemoryData.containsKey(aTable)) {
            return OpStatus.TABLE_NOT_FOUND;
        }
        if (theMemoryData.containsKey(aNewName)) {
            return OpStatus.TABLE_ALREADY_EXISTS;
        }
        theMemoryData.put(aNewName, theMemoryData.get(aTable));
        return delete(aTable);
    }

    @Override
    public int count(String aTable) {
        if (!theMemoryData.containsKey(aTable)) {
            return -1;
        }
        return theMemoryData.get(aTable).size();
    }

    @Override
    public OpStatus fromMap(String aTableName, ConcurrentMap<String, Row> aTable) {
        if (theMemoryData.containsKey(aTableName)) {
            return OpStatus.TABLE_ALREADY_EXISTS;
        }
        ConcurrentMap<String, ConcurrentLinkedDeque<Row>> myTable = new ConcurrentSkipListMap<>();
        aTable.forEach((aKey, aRow) -> {
            ConcurrentLinkedDeque<Row> myRowDeque = new ConcurrentLinkedDeque<>();
            myRowDeque.add(aRow);
            myTable.put(aKey, myRowDeque);
        });
        theMemoryData.put(aTableName, myTable);
        return OpStatus.SUCCESS;
    }

    @Override
    public ConcurrentMap<String, Row> getMap(String aTableName) {
        ConcurrentMap<String, Row> myResult = new ConcurrentHashMap<>();
        if (!theMemoryData.containsKey(aTableName)) {
            return myResult;
        }
        theMemoryData.get(aTableName).forEach((aKey, aRowDeque) -> myResult.put(aKey, aRowDeque.peekLast()));
        return myResult;
    }
}
