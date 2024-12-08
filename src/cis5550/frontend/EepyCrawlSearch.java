package cis5550.frontend;

import cis5550.tools.Logger;
import cis5550.webserver.Route;
import cis5550.webserver.datamodels.ContentType;
import com.sun.source.tree.Tree;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

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

            myQuery = myQuery.toLowerCase().trim();

            List<SearchResult> myResults = getSearchResults(myQuery);
            return JSONBuilders.buildSearchResults(myResults);
        };
    }

    private static List<SearchResult> getSearchResults(String aQuery) {
        LOGGER.info("Getting search results for query: " + aQuery);
        if (aQuery == null || aQuery.isEmpty()) {
            return List.of();
        }

        try {
            LOGGER.debug("Checking cache for query: " + aQuery);
            List<SearchResult> myCachedResults = FrontendKVSClient.getFromCache(aQuery);
            if (myCachedResults != null) {
                return myCachedResults;
            }
        } catch (IOException e) {
            LOGGER.error("Error getting cached results from KVS");
        }

        List<SearchResult> myResults = null;

        Map<String, Double> myTFIDFScores = TFIDF.getTFIDFScores(aQuery);

        Map<String, Double> myPagerankScores = new HashMap<>();

        try {
            myPagerankScores = FrontendKVSClient.getPagerankScores(myTFIDFScores.keySet());
        } catch (IOException e) {
            LOGGER.error("Error getting pagerank scores from KVS");
        }

        Map<String, Double> myCombinedScores = new HashMap<>();

        for (String myUrl : myTFIDFScores.keySet()) {
            double myTFIDFScore = myTFIDFScores.get(myUrl);
            double myPagerankScore = 0.0;
            if (myPagerankScores.containsKey(myUrl)) {
                myPagerankScore = myPagerankScores.get(myUrl);
            }
            myCombinedScores.put(myUrl, getFinalCombinedScore(myTFIDFScore, myPagerankScore));
        }

        myResults = buildSearchResultsFromScores(myCombinedScores);

        try {
            FrontendKVSClient.putInCache(aQuery, myResults);
        } catch (IOException e) {
            LOGGER.error("Error putting search results in cache");
        }

        LOGGER.info("Returning search results for query: " + aQuery);

        return myResults;
    }

    private static Double getFinalCombinedScore(Double aTFIDFScore, Double aPagerankScore) {
        return aTFIDFScore + aPagerankScore;
    }

    private static List<SearchResult> buildSearchResultsFromScores(Map<String, Double> aScores) {
        return aScores.entrySet().stream().sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                .map(myEntry -> new SearchResult(" ", myEntry.getKey(), " "))
                .collect(Collectors.toList());
    }
}
