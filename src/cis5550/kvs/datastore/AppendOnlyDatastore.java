package cis5550.kvs.datastore;

import cis5550.kvs.Row;
import cis5550.kvs.datamodels.OpStatus;

import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Stream;

public class AppendOnlyDatastore implements Datastore {

    public AppendOnlyDatastore(String aDirectory) {
    }

    @Override
    public int put(String aTable, String aKey, String aColumn, byte[] aValue) {
        return 0;
    }

    @Override
    public int putRow(String aTable, String aKey, Row aRow) {
        return 0;
    }

    @Override
    public int append(String aTable, String aKey, String aColumn, byte[] aValue, String aDelimiter) {
        return 0;
    }

    @Override
    public Row get(String aTable, String aKey) {
        return null;
    }

    @Override
    public Row get(String aTable, String aKey, int aVersion) {
        return null;
    }

    @Override
    public int getVersion(String aTable, String aKey) {
        return 0;
    }

    @Override
    public Map<String, Integer> getTables() {
        return Map.of();
    }

    @Override
    public SortedMap<String, Row> getRows(String aTable, String aFromRow, int aNumRows) {
        return null;
    }

    @Override
    public Stream<Row> getRowDataStream(String aTable, String aStartRow, String aEndRowExclusive) {
        return Stream.empty();
    }

    @Override
    public OpStatus delete(String aTable) {
        return null;
    }

    @Override
    public OpStatus rename(String aTable, String aNewName) {
        return null;
    }

    @Override
    public int count(String aTable) {
        return 0;
    }

    @Override
    public OpStatus fromMap(String aTableName, ConcurrentMap<String, Row> aTable) {
        return null;
    }

    @Override
    public ConcurrentMap<String, Row> getMap(String aTableName) {
        return null;
    }
}
