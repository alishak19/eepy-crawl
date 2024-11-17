package cis5550.utils;

import cis5550.kvs.Row;
import cis5550.tools.Logger;

import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;

public class FileIOUtils {
    public static final Logger LOGGER = Logger.getLogger(FileIOUtils.class);

    public static Row readRowFromFile(String aFilePath) {
        try (RandomAccessFile myFile = new RandomAccessFile(aFilePath, "r");
             FileChannel myChannel = myFile.getChannel()) {
            LOGGER.debug("Obtaining lock on file: " + aFilePath);
            FileLock myLock = myChannel.lock(0L, Long.MAX_VALUE, true);
            LOGGER.debug("Shared lock obtained on file: " + aFilePath);

            Row myRow = Row.readFrom(myFile);
            myLock.release();
            LOGGER.debug("Lock released on file: " + aFilePath);
            return myRow;
        } catch (Exception e) {
            LOGGER.error("Error reading row from file: " + aFilePath, e);
            return null;
        }
    }

    public static boolean writeRowToFile(String aFilePath, Row aRow) {
        try (RandomAccessFile myFile = new RandomAccessFile(aFilePath, "rw");
             FileChannel myChannel = myFile.getChannel()) {
            LOGGER.debug("Obtaining lock on file: " + aFilePath);
            FileLock myLock = myChannel.lock(0L, Long.MAX_VALUE, false);
            LOGGER.debug("Exclusive lock obtained on file: " + aFilePath);

            myFile.write(aRow.toByteArray());
            myLock.release();
            LOGGER.debug("Lock released on file: " + aFilePath);
            return true;
        } catch (Exception e) {
            LOGGER.error("Error writing row to file: " + aFilePath, e);
            return false;
        }
    }
}
