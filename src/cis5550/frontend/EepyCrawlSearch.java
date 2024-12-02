package cis5550.frontend;

import cis5550.tools.Logger;
import cis5550.webserver.Route;
import cis5550.webserver.datamodels.ContentType;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

import static cis5550.webserver.Server.*;

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
        get("/search", searchRoute());
    }

    private static Route searchRoute() {
        return (req, res) -> {
            String myQuery = req.queryParams("q");
            LOGGER.info("Received search query: " + myQuery);

            List<SearchResult> myResults = getSearchResults(myQuery);
            return JSONBuilders.buildSearchResults(myResults);
        };
    }

    private static List<SearchResult> getSearchResults(String aQuery) {
        return List.of(
            new SearchResult("Title 1", "https://www.example.com/1", "Snippet 1"),
            new SearchResult("Title 2", "https://www.example.com/2", "Snippet 2"),
            new SearchResult("Title 3", "https://www.example.com/3", "Snippet 3")
        );
    }
}
