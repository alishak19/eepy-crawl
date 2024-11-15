package cis5550.kvs.datastore;

import cis5550.kvs.datamodels.OpStatus;
import cis5550.kvs.Row;
import cis5550.tools.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Stream;

import static cis5550.tools.KeyEncoder.encode;

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
                myRow = Row.readFrom(new FileInputStream(myFile));
            } catch (Exception e) {
                LOGGER.error("Failed to read from file", e);
                return -1;
            }
        }

        myRow.put(aColumn, aValue);

        try (FileOutputStream myWriter = new FileOutputStream(myFile)) {
            myWriter.write(myRow.toByteArray());
            myWriter.flush();
        } catch (IOException e) {
            LOGGER.error("Failed to write to file", e);
            return -1;
        }

        return 0;
    }

    @Override
    public Row get(String aTable, String aKey) {
        String myFileName = getRowFileName(aTable, aKey);

        File myFile = new File(myFileName);
        if (!myFile.exists()) {
            return null;
        }

        try {
            return Row.readFrom(new FileInputStream(myFile));
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

        File[] myRowFilesorSubdirectories = myTableDirectory.listFiles();

        return Arrays.stream(myRowFilesorSubdirectories)
                .flatMap(myRowFileOrSubdirectory -> {
                    if (myRowFileOrSubdirectory.isDirectory()) {
                        return Arrays.stream(myRowFileOrSubdirectory.listFiles());
                    } else {
                        return Stream.of(myRowFileOrSubdirectory);
                    }
                })
                .map(myRowFile -> {
                    try {
                        return new FileInputStream(myRowFile);
                    } catch (IOException e) {
                        LOGGER.error("Failed to read from file", e);
                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .map(myFileStream -> {
                    try {
                        return Row.readFrom(myFileStream);
                    } catch (Exception e) {
                        LOGGER.error("Failed to parse file stream", e);
                        return null;
                    }
                })
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

        return myTableDirectory.listFiles().length;
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
                    Row myRow = Row.readFrom(new FileInputStream(myRowFileOrSubdirectory));
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
