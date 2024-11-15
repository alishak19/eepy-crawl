package cis5550.kvs.datastore;

import cis5550.kvs.datamodels.OpStatus;
import cis5550.kvs.Row;

import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Stream;

public interface Datastore {
    int put(String aTable, String aKey, String aColumn, byte[] aValue);
    Row get(String aTable, String aKey);
    Row get(String aTable, String aKey, int aVersion);
    int getVersion(String aTable, String aKey);
    Map<String, Integer> getTables();
    SortedMap<String, Row> getRows(String aTable, String aFromRow, int aNumRows);
    Stream<Row> getRowDataStream(String aTable, String aStartRow, String aEndRowExclusive);
    OpStatus delete(String aTable);
    OpStatus rename(String aTable, String aNewName);
    int count(String aTable);
    OpStatus fromMap(String aTableName, ConcurrentMap<String, Row> aTable);
    ConcurrentMap<String, Row> getMap(String aTableName);
}
