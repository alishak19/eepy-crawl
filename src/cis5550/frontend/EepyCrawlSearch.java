package cis5550.frontend;

import cis5550.tools.Logger;
import cis5550.webserver.Route;
import cis5550.webserver.datamodels.ContentType;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

import static cis5550.webserver.Server.*;

public class EepyCrawlSearch {
    private static final Logger LOGGER = Logger.getLogger(EepyCrawlSearch.class);
    private static final String PAGE_DIR = "pages";
    private static final int PORT = 80;

    public static void main(String[] args) {
        port(PORT);
        LOGGER.info("Starting EepyCrawlSearch on port " + PORT);

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
        if (aQuery == null || aQuery.isEmpty()) {
            return List.of();
        }

        try {
            List<SearchResult> myCachedResults = FrontendKVSClient.getFromCache(aQuery);
            if (myCachedResults != null) {
                return myCachedResults;
            }
        } catch (IOException e) {
            LOGGER.error("Error getting cached results from KVS");
        }

        List<SearchResult> myResults = null;

        Map<String, Double> myTFIDFScores = TFIDF.getTFIDFScores(aQuery);

        if (myTFIDFScores == null) {
            return List.of();
        }

        try {
            Map<String, Double> myPagerankScores = FrontendKVSClient.getPagerankScores(myTFIDFScores.keySet());
        } catch (IOException e) {
            LOGGER.error("Error getting pagerank scores from KVS");
        }

        try {
            FrontendKVSClient.putInCache(aQuery, myResults);
        } catch (IOException e) {
            LOGGER.error("Error putting search results in cache");
        }

        return List.of(
            new SearchResult("Title 1", "https://www.example.com/1", "Snippet 1"),
            new SearchResult("Title 2", "https://www.example.com/2", "Snippet 2"),
            new SearchResult("Title 3", "https://www.example.com/3", "Snippet 3")
        );
    }
}
