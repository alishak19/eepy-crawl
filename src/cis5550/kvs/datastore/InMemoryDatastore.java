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

public class InMemoryDatastore implements Datastore {
    public static final Logger LOGGER = Logger.getLogger(InMemoryDatastore.class);

    private final ConcurrentMap<String, ConcurrentMap<String, Row>> theMemoryData;

    public InMemoryDatastore() {
        theMemoryData = new ConcurrentHashMap<>();
    }

    @Override
    public int put(String aTable, String aKey, String aColumn, byte[] aValue) {
        theMemoryData.putIfAbsent(aTable, new ConcurrentHashMap<>());
        if (!theMemoryData.get(aTable).containsKey(aKey)) {
            Row myNewRow = new Row(aKey);
            myNewRow.put(aColumn, aValue);
            theMemoryData.get(aTable).put(aKey, myNewRow);
        } else {
            theMemoryData.get(aTable).get(aKey).put(aColumn, aValue);
        }
        return 0;
    }

    @Override
    public int putRow(String aTable, String aKey, Row aRow) {
        theMemoryData.putIfAbsent(aTable, new ConcurrentHashMap<>());
        theMemoryData.get(aTable).put(aKey, aRow);
        return 0;
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
        return theMemoryData.get(aTable).get(aKey);
    }

    @Override
    public int getVersion(String aTable, String aKey) {
        return 0;
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
                                aMap.put(aEntry.getKey(), aEntry.getValue()),
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
                .map(aEntry -> aEntry.getValue());
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
        theMemoryData.put(aTableName, aTable);
        return OpStatus.SUCCESS;
    }

    @Override
    public ConcurrentMap<String, Row> getMap(String aTableName) {
        if (!theMemoryData.containsKey(aTableName)) {
            return new ConcurrentHashMap<>();
        }
        return theMemoryData.get(aTableName);
    }
}
