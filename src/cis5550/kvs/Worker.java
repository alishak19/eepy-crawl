package cis5550.kvs;

import cis5550.kvs.datastore.DatastoreContainer;
import cis5550.tools.Logger;
import cis5550.webserver.Request;
import cis5550.webserver.Route;

import java.io.*;
import java.util.SortedMap;
import java.util.stream.Stream;

import static cis5550.kvs.datamodels.IPPort.fromString;
import static cis5550.kvs.HTMLGenerator.generateTableEntries;
import static cis5550.kvs.HTMLGenerator.generateWorkerEntries;
import static cis5550.kvs.IDGenerator.generateLowerCaseID;
import static cis5550.webserver.Server.*;

public class Worker extends cis5550.generic.Worker {
    public static final Logger LOGGER = Logger.getLogger(Worker.class);

    public static final String ID_FILE = "id";
    public static final int ID_LENGTH = 5;
    public static final int PAGE_SIZE = 10;

    private static DatastoreContainer theData;
    private static WorkerReplicationManager theReplicationManager;

    public static void main(String[] args) {
        if (args.length != 3) {
            System.err.println("Usage: Worker <port> <directory> <coordinator ip:port>");
            System.exit(1);
        }

        int myPort = Integer.valueOf(args[0]);
        String myDirectory = args[1];
        String myCoordinatorIPPort = args[2];

        if (!myCoordinatorIPPort.contains(":")) {
            System.err.println("Usage: Worker <port> <directory> <coordinator ip:port>");
            System.exit(1);
        }

        LOGGER.info("Starting KVS worker on port " + myPort + " with directory " + myDirectory + " and coordinator "
                + myCoordinatorIPPort);

        File myDir = new File(myDirectory);

        if (!myDir.exists() || !myDir.isDirectory()) {
            if (!myDir.mkdirs()) {
                LOGGER.error("Failed to create directory");
                System.exit(1);
            }
        }

        String myId = null;
        File myIdFile = new File(myDirectory + File.separator + ID_FILE);
        if (myIdFile.exists()) {
            try {
                myId = new BufferedReader(new FileReader(myIdFile)).readLine();
            } catch (IOException e) {
                LOGGER.error("Failed to read id file");
                System.exit(1);
                return;
            }
        }
        if (myId == null) {
            myId = generateLowerCaseID(ID_LENGTH);
            try {
                BufferedWriter myWriter = new BufferedWriter(new FileWriter(myIdFile));
                myWriter.write(myId);
                myWriter.flush();
                myWriter.close();
            } catch (IOException e) {
                LOGGER.error("Failed to write id file");
                System.exit(1);
                return;
            }
        }

        theData = new DatastoreContainer(myDirectory);
        theReplicationManager = new WorkerReplicationManager(myId, fromString(myCoordinatorIPPort));

        port(myPort);
        startPingThread(myId, myPort, myCoordinatorIPPort);

        get("/", getTables());
        get("/view/:table", getTableRows());
        get("/data/:table/:row", getRow());
        get("/data/:table", streamRows());
        put("/data/:table/:row/:column", putCell());
        get("/data/:table/:row/:column", getCell());
        put("/delete/:table", deleteTable());
        put("/rename/:table", renameTable());
        get("/count/:table", rowCount());
    }

    private static Route putCell() {
        return (req, res) -> {
            forwardPutRequest(req);
            String myTable = req.params("table");
            String myRow = req.params("row");
            String myColumn = req.params("column");
            byte[] myValue = req.bodyAsBytes();

            if (myTable == null || myRow == null || myColumn == null || myValue == null) {
                res.status(400, "Bad Request");
                return "Bad Request";
            }

            String myIfColumn = req.queryParams("ifcolumn");
            String myIfEquals = req.queryParams("equals");

            if (myIfColumn != null && myIfEquals != null) {
                Row myRowObject = theData.get(myTable, myRow);
                if (myRowObject == null
                        || myRowObject.get(myIfColumn) == null
                        || !myIfEquals.equals(myRowObject.get(myIfColumn))) {
                    return "FAIL";
                }
            }

            int myVersion = theData.put(myTable, myRow, myColumn, myValue);

            res.header("Version", String.valueOf(myVersion));
            res.status(200, "OK");
            return "OK";
        };
    }

    private static Route getRow() {
        return (req, res) -> {
            String myTable = req.params("table");
            String myRow = req.params("row");

            if (myTable == null || myRow == null) {
                res.status(400, "Bad Request");
                return "Bad Request";
            }

            Row myRowObject = theData.get(myTable, myRow);
            if (myRowObject == null) {
                res.status(404, "Not Found");
                return "Not Found";
            }

            res.status(200, "OK");
            res.type("application/octet-stream");
            res.bodyAsBytes(myRowObject.toByteArray());
            return null;
        };
    }

    private static Route getCell() {
        return (req, res) -> {
            String myTable = req.params("table");
            String myRow = req.params("row");
            String myColumn = req.params("column");

            if (myTable == null || myRow == null || myColumn == null) {
                res.status(400, "Bad Request");
                return "Bad Request";
            }

            String myVersionString = req.queryParams("version");
            int myVersion;
            Row myRowObject;

            if (myVersionString == null) {
                myRowObject = theData.get(myTable, myRow);
                if (myRowObject == null) {
                    res.status(404, "Not Found");
                    return "Not Found";
                }

                myVersion = theData.getVersion(myTable, myRow);
            } else {
                try {
                    myVersion = Integer.parseInt(myVersionString);
                } catch (NumberFormatException e) {
                    LOGGER.error("Failed to parse version", e);
                    res.status(400, "Bad Request");
                    return "Bad Request";
                }

                myRowObject = theData.get(myTable, myRow, myVersion);
                if (myRowObject == null) {
                    res.status(404, "Not Found");
                    return "Not Found";
                }
            }

            if (myRowObject.get(myColumn) == null) {
                res.status(404, "Not Found");
                return "Not Found";
            }

            res.status(200, "OK");
            res.bodyAsBytes(myRowObject.getBytes(myColumn));
            res.header("Version", String.valueOf(myVersion));
            return null;
        };
    }

    private static Route streamRows() {
        return (req, res) -> {
            String myTable = req.params("table");
            String myStartRow = req.queryParams("startRow");
            String myEndRowExclusive = req.queryParams("endRowExclusive");

            if (myTable == null) {
                res.status(400, "Bad Request");
                return "Bad Request";
            }
            Stream<Row> myRowStream = theData.getRowDataStream(myTable, myStartRow, myEndRowExclusive);
            if (myRowStream == null) {
                res.status(404, "Not Found");
                return "Not Found";
            }
            res.type("text/plain");
            myRowStream.forEach(myRow -> {
                try {
                    res.write(myRow.toByteArray());
                    res.write(new byte[] {'\n'});
                } catch (Exception e) {
                    LOGGER.error("Failed to write row to response", e);
                }
            });
            try {
                res.write(new byte[] {'\n'});
            } catch (Exception e) {
                LOGGER.error("Failed to write final LF", e);
            }
            return null;
        };
    }

    private static Route deleteTable() {
        return (req, res) -> {
            forwardPutRequest(req);
            String myTable = req.params("table");

            if (myTable == null) {
                res.status(400, "Bad Request");
                return "Bad Request";
            }

            switch (theData.delete(myTable)) {
                case SUCCESS:
                    res.status(200, "OK");
                    return "OK";
                case TABLE_NOT_FOUND:
                    res.status(404, "Not found");
                    return "Not found";
                default:
                    res.status(500, "Internal Server Error");
                    return "Internal Server Error";
            }
        };
    }

    private static Route renameTable() {
        return (req, res) -> {
            forwardPutRequest(req);
            String myTable = req.params("table");
            String myNewName = req.body();

            if (myTable == null || myNewName == null) {
                res.status(400, "Bad Request");
                return "Bad Request";
            }

            switch (theData.rename(myTable, myNewName)) {
                case SUCCESS:
                    res.status(200, "OK");
                    return "OK";
                case TABLE_NOT_FOUND:
                    res.status(404, "Not found");
                    return "Not found";
                case TABLE_ALREADY_EXISTS:
                    res.status(409, "Conflict");
                    return "Conflict";
                case WRONG_NAME_FORMAT:
                    res.status(400, "Bad Request");
                    return "Bad Request";
                default:
                    res.status(500, "Internal Server Error");
                    return "Internal Server Error";
            }
        };
    }

    private static Route rowCount() {
        return (req, res) -> {
            String myTable = req.params("table");

            if (myTable == null) {
                res.status(400, "Bad Request");
                return "Bad Request";
            }

            int myCount = theData.count(myTable);

            if (myCount < 0) {
                res.status(404, "Not Found");
                return "Not Found";
            }

            res.status(200, "OK");
            return myCount;
        };
    }

    private static Route getTables() {
        return (req, res) -> {
            res.status(200, "OK");
            return generateWorkerEntries(theData.getTables());
        };
    }

    private static Route getTableRows() {
        return (req, res) -> {
            String myTable = req.params("table");
            String myFromRow = req.queryParams("fromRow");
            if (myTable == null) {
                res.status(400, "Bad Request");
                return "Bad Request";
            }
            res.status(200, "OK");
            SortedMap<String, Row> myRows = theData.getRows(myTable, myFromRow, PAGE_SIZE);

            return generateTableEntries(myTable, theData.getRows(myTable, myFromRow, PAGE_SIZE), PAGE_SIZE);
        };
    }

    private static void forwardPutRequest(Request aReq) {
        if (aReq.queryParams("replica") != null) {
            return;
        }
        theReplicationManager.forwardPuts(aReq);
    }
}
