package cis5550.kvs;

import cis5550.tools.Logger;

import static cis5550.webserver.Server.get;
import static cis5550.webserver.Server.port;

public class Coordinator extends cis5550.generic.Coordinator {
    public static Logger LOGGER = Logger.getLogger(Coordinator.class);

    public static void main(String[] args) {
        if (args.length != 1) {
            System.err.println("Usage: Coordinator <port>");
            System.exit(1);
        }

        LOGGER.info("Starting Coordinator on port " + args[0]);

        int myPort = Integer.parseInt(args[0]);
        port(myPort);
        registerRoutes();
        get("/", (req, res) -> {
            String myWorkerTable = workerTable();
            return "<html><h1>Welcome to the KVS Coordinator</h1>" + myWorkerTable + "</html>";
        });
    }

}
