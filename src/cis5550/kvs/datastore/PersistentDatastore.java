package cis5550.kvs.datastore;

import cis5550.kvs.datamodels.OpStatus;
import cis5550.kvs.Row;
import cis5550.tools.Logger;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Stream;

import static cis5550.kvs.KeyEncoder.encode;
import static cis5550.utils.FileIOUtils.readRowFromFile;
import static cis5550.utils.FileIOUtils.writeRowToFile;

public class PersistentDatastore implements Datastore {
    public static final Logger LOGGER = Logger.getLogger(PersistentDatastore.class);
    public static final String SUBDIRECTORY_PREFIX = "__";
    public static final int MAX_KEY_LENGTH = 6;
    public static final int KEY_SUBSTRING_LENGTH = 2;

    private final String theDataDirectory;

    public PersistentDatastore(String aDirectory) {
        theDataDirectory = aDirectory;
    }

    @Override
    public int put(String aTable, String aKey, String aColumn, byte[] aValue) {
        String myFileName = getRowFileName(aTable, aKey);

        File myFile = new File(myFileName);
        Row myRow = new Row(aKey);
        if (!myFile.getParentFile().exists()) {
            if (!myFile.getParentFile().mkdirs()) {
                LOGGER.error("Failed to create directory");
                return -1;
            }
        } else if (myFile.exists()) {
            try {
                myRow = readRowFromFile(myFileName);
            } catch (Exception e) {
                LOGGER.error("Failed to read from file", e);
                return -1;
            }
        }

        myRow.put(aColumn, aValue);

        return writeRowToFile(myFileName, myRow) ? 0 : -1;
    }

    @Override
    public int putRow(String aTable, String aKey, Row aRow) {
        String myFileName = getRowFileName(aTable, aKey);

        File myFile = new File(myFileName);
        if (!myFile.getParentFile().exists()) {
            if (!myFile.getParentFile().mkdirs()) {
                LOGGER.error("Failed to create directory");
                return -1;
            }
        }

        return writeRowToFile(myFileName, aRow) ? 0 : -1;

    }

    @Override
    public int append(String aTable, String aKey, String aColumn, byte[] aValue, String aDelimiter) {
        String myFileName = getRowFileName(aTable, aKey);

        File myFile = new File(myFileName);
        Row myRow = new Row(aKey);
        if (!myFile.getParentFile().exists()) {
            if (!myFile.getParentFile().mkdirs()) {
                LOGGER.error("Failed to create directory");
                return -1;
            }
        } else if (myFile.exists()) {
            try {
                myRow = readRowFromFile(myFileName);
            } catch (Exception e) {
                LOGGER.error("Failed to read from file", e);
                return -1;
            }
        }

        if (myRow.get(aColumn) == null) {
            myRow.put(aColumn, aValue);
        } else {
            myRow.put(aColumn, myRow.get(aColumn) + aDelimiter + new String(aValue));
        }

        return writeRowToFile(myFileName, myRow) ? 0 : -1;
    }

    @Override
    public Row get(String aTable, String aKey) {
        String myFileName = getRowFileName(aTable, aKey);

        File myFile = new File(myFileName);
        if (!myFile.exists()) {
            return null;
        }

        try {
            return readRowFromFile(myFileName);
        } catch (Exception e) {
            LOGGER.error("Failed to read from file", e);
            return null;
        }
    }

    @Override
    public Row get(String aTable, String aKey, int aVersion) {
        return get(aTable, aKey);
    }

    @Override
    public int getVersion(String aTable, String aKey) {
        return 0;
    }

    @Override
    public Map<String, Integer> getTables() {
        File myDataDirectory = new File(theDataDirectory);
        File[] myTables = myDataDirectory.listFiles();

        if (myTables == null) {
            return Map.of();
        }

        Map<String, Integer> myResult = new HashMap<>();

        for (File myTable : myTables) {
            if (myTable.isDirectory()) {
                myResult.put(myTable.getName(), count(myTable.getName()));
            }
        }

        return myResult;
    }

    @Override
    public SortedMap<String, Row> getRows(String aTable, String aFromRow, int aNumRows) {
        File myTableDirectory = new File(getTableDirectory(aTable));

        if (!myTableDirectory.exists() || !myTableDirectory.isDirectory()) {
            return null;
        }

        return recursiveAddRows(myTableDirectory, aFromRow).entrySet().stream()
                .limit(aNumRows + 1)
                .collect(
                        ConcurrentSkipListMap::new,
                        (aMap, aEntry) -> aMap.put(aEntry.getKey(), aEntry.getValue()),
                        ConcurrentSkipListMap::putAll);
    }

    @Override
    public Stream<Row> getRowDataStream(String aTable, String aStartRow, String aEndRowExclusive) {
        File myTableDirectory = new File(getTableDirectory(aTable));

        if (!myTableDirectory.exists() || !myTableDirectory.isDirectory()) {
            return null;
        }

        File[] myRowFilesOrSubdirectories = myTableDirectory.listFiles();

        return Arrays.stream(myRowFilesOrSubdirectories)
                .flatMap(myRowFileOrSubdirectory -> {
                    if (myRowFileOrSubdirectory.isDirectory()) {
                        return Arrays.stream(myRowFileOrSubdirectory.listFiles());
                    } else {
                        return Stream.of(myRowFileOrSubdirectory);
                    }
                })
                .filter(Objects::nonNull)
                .map(myFile -> readRowFromFile(myFile.getAbsolutePath()))
                .filter(Objects::nonNull);
    }

    @Override
    public OpStatus delete(String aTable) {
        File myTableDirectory = new File(getTableDirectory(aTable));

        if (!myTableDirectory.exists() || !myTableDirectory.isDirectory()) {
            return OpStatus.TABLE_NOT_FOUND;
        }

        File[] myRowOrSubdirectories = myTableDirectory.listFiles();

        Arrays.stream(myRowOrSubdirectories).forEach(myRowOrSubdirectory -> {
            if (myRowOrSubdirectory.isDirectory()) {
                File[] myRowFiles = myRowOrSubdirectory.listFiles();
                if (myRowFiles != null) {
                    Arrays.stream(myRowFiles).forEach(File::delete);
                }
            }
            myRowOrSubdirectory.delete();
        });
        return myTableDirectory.delete() ? OpStatus.SUCCESS : OpStatus.SERVER_ERROR;
    }

    @Override
    public OpStatus rename(String aTable, String aNewName) {
        File myTableDirectory = new File(getTableDirectory(aTable));

        if (!myTableDirectory.exists() || !myTableDirectory.isDirectory()) {
            return OpStatus.TABLE_NOT_FOUND;
        }

        File myNewTableDirectory = new File(theDataDirectory + File.separator + aNewName);

        if (myNewTableDirectory.exists()) {
            return OpStatus.TABLE_ALREADY_EXISTS;
        }

        return myTableDirectory.renameTo(myNewTableDirectory) ? OpStatus.SUCCESS : OpStatus.SERVER_ERROR;
    }

    @Override
    public int count(String aTable) {
        File myTableDirectory = new File(getTableDirectory(aTable));

        if (!myTableDirectory.exists() || !myTableDirectory.isDirectory()) {
            return -1;
        }

        int myCount = 0;

        for (File myRowOrSubdirectory : myTableDirectory.listFiles()) {
            if (myRowOrSubdirectory.isDirectory()) {
                File[] myRowFiles = myRowOrSubdirectory.listFiles();
                if (myRowFiles != null) {
                    myCount += myRowFiles.length;
                }
            } else {
                myCount++;
            }
        }

        return myCount;
    }

    @Override
    public OpStatus fromMap(String aTableName, ConcurrentMap<String, Row> aTable) {
        File myTableDirectory = new File(getTableDirectory(aTableName));

        if (myTableDirectory.exists()) {
            return OpStatus.TABLE_ALREADY_EXISTS;
        }

        for (Map.Entry<String, Row> myEntry : aTable.entrySet()) {
            String myRowFileName = getRowFileName(aTableName, myEntry.getKey());
            File myRowFile = new File(myRowFileName);
            if (!myRowFile.getParentFile().exists()) {
                if (!myRowFile.getParentFile().mkdirs()) {
                    LOGGER.error("Failed to create directory");
                    return OpStatus.SERVER_ERROR;
                }
            }

            if (!writeRowToFile(myRowFileName, myEntry.getValue())) {
                return OpStatus.SERVER_ERROR;
            }
        }
        return OpStatus.SUCCESS;
    }

    @Override
    public ConcurrentMap<String, Row> getMap(String aTableName) {
        SortedMap<String, Row> myRows = recursiveAddRows(new File(getTableDirectory(aTableName)), null);
        ConcurrentMap<String, Row> myResult = new ConcurrentSkipListMap<>();
        myResult.putAll(myRows);
        return myResult;
    }

    private String getRowFileName(String aTable, String aKey) {
        String myEncodedKey = encode(aKey);
        String myFileName;

        if (myEncodedKey.length() >= MAX_KEY_LENGTH) {
            myFileName = theDataDirectory
                    + File.separator
                    + aTable
                    + File.separator
                    + SUBDIRECTORY_PREFIX
                    + myEncodedKey.substring(0, KEY_SUBSTRING_LENGTH)
                    + File.separator
                    + myEncodedKey;
        } else {
            myFileName = theDataDirectory + File.separator + aTable + File.separator + encode(aKey);
        }

        return myFileName;
    }

    private String getTableDirectory(String aTable) {
        return theDataDirectory + File.separator + aTable;
    }

    private SortedMap<String, Row> recursiveAddRows(File aDirectory, String aFromRow) {
        SortedMap<String, Row> myResult = new ConcurrentSkipListMap<>();
        File[] myRowFilesOrSubdirectories = aDirectory.listFiles();

        if (myRowFilesOrSubdirectories == null) {
            return myResult;
        }

        for (File myRowFileOrSubdirectory : myRowFilesOrSubdirectories) {
            if (myRowFileOrSubdirectory.isDirectory()) {
                myResult.putAll(recursiveAddRows(myRowFileOrSubdirectory, aFromRow));
            } else {
                try {
                    LOGGER.info("Reading from file: " + myRowFileOrSubdirectory.getName());
                    Row myRow = readRowFromFile(myRowFileOrSubdirectory.getAbsolutePath());
                    if (myRow != null && (aFromRow == null || myRow.key().compareTo(aFromRow) >= 0)) {
                        myResult.put(myRow.key(), myRow);
                    }
                } catch (Exception e) {
                    LOGGER.error("Failed to read from file", e);
                }
            }
        }

        return myResult;
    }
}
