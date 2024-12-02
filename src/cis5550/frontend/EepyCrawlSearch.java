package cis5550.frontend;

import cis5550.tools.Logger;
import cis5550.webserver.datamodels.ContentType;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;

import static cis5550.webserver.Server.get;
import static cis5550.webserver.Server.port;

public class EepyCrawlSearch {
    private static final Logger LOGGER = Logger.getLogger(EepyCrawlSearch.class);
    private static final String PAGE_DIR = "pages";

    public static void main(String[] args) {
        if (args.length != 1 || !args[0].matches("\\d+")) {
            LOGGER.error("Usage: EepyCrawlSearch <port>");
        }

        int myPort = Integer.parseInt(args[0]);

        port(myPort);
        LOGGER.info("Starting EepyCrawlSearch on port " + myPort);

        get("/", (req, res) -> {
            res.type(ContentType.HTML.getTypeString());
            return new String(Files.readAllBytes(Paths.get(PAGE_DIR + File.separator + "index.html")));
        });
    }
}
