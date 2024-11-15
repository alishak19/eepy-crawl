package cis5550.generic;

import java.util.List;
import java.util.Vector;

import static cis5550.webserver.Server.get;

public class Coordinator {
    private static final WorkerTable<String> theWorkerTable = new WorkerTable<>();

    public static String getWorkers() {
        return theWorkerTable.getWorkers();
    }

    public static Vector<String> getWorkersList() {
        return theWorkerTable.getWorkersList();
    }

    public static String workerTable() {
        return theWorkerTable.buildWorkerTable();
    }

    public static void registerRoutes() {
        get("/ping", (req, res) -> {
            String myID = req.queryParams("id");
            String myPort = req.queryParams("port");
            String myIP = req.ip();

            if (myID == null || myPort == null) {
                res.status(400, "Bad Request");
                return null;
            }

            try {
                Integer.parseInt(myPort);
            } catch (NumberFormatException e) {
                res.status(400, "Bad Request");
                return null;
            }

            theWorkerTable.addOrUpdate(myID, myIP, Integer.parseInt(myPort));
            res.status(200, "OK");
            return "OK";
        });
        get("/workers", (req, res) -> {
            return getWorkers();
        });
    }

}
