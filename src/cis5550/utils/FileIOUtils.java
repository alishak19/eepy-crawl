package cis5550.utils;

import cis5550.kvs.Row;
import cis5550.tools.Logger;

import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;

public class FileIOUtils {
    public static final Logger LOGGER = Logger.getLogger(FileIOUtils.class);
    public static final ConcurrentMap<String, ReentrantLock> theFileLocks = new ConcurrentHashMap<>();

    public static Row readRowFromFile(String aFilePath) {
        addLockIfNotAdded(aFilePath);

        theFileLocks.get(aFilePath).lock();
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
        } finally {
            theFileLocks.get(aFilePath).unlock();
        }
    }

    public static boolean writeRowToFile(String aFilePath, Row aRow) {
        addLockIfNotAdded(aFilePath);

        theFileLocks.get(aFilePath).lock();
        try (RandomAccessFile myFile = new RandomAccessFile(aFilePath, "rw");
             FileChannel myChannel = myFile.getChannel()) {
            long myLockBefore = System.nanoTime();
            LOGGER.debug("Obtaining lock on file: " + aFilePath);
            FileLock myLock = myChannel.lock(0L, Long.MAX_VALUE, false);
            LOGGER.debug("Exclusive lock obtained on file: " + aFilePath);
            long myLockAfter = System.nanoTime();
            LOGGER.info("Time to obtain lock: " + (myLockAfter - myLockBefore) + " ns");

            long myWriteBefore = System.nanoTime();
            ByteBuffer myBuffer = ByteBuffer.wrap(aRow.toByteArray());
            int myBytesWritten = 0;
            while (myBuffer.hasRemaining()) {
                myBytesWritten = myChannel.write(myBuffer);
                if (myBytesWritten == 0) {
                    LOGGER.error("No bytes written to file: " + aFilePath);
                    break;
                }
            }
            long myWriteAfter = System.nanoTime();
            LOGGER.info("Time to write to file: " + (myWriteAfter - myWriteBefore) + " ns");
            myLock.release();
            LOGGER.debug("Lock released on file: " + aFilePath);
            return true;
        } catch (Exception e) {
            LOGGER.error("Error writing row to file: " + aFilePath, e);
            return false;
        } finally {
            theFileLocks.get(aFilePath).unlock();
        }
    }

    private static void addLockIfNotAdded(String aFilePath) {
        if (!theFileLocks.containsKey(aFilePath)) {
            theFileLocks.put(aFilePath, new ReentrantLock());
        }
    }
}
