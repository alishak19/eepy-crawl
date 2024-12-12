package cis5550.frontend;

import cis5550.tools.Logger;
import cis5550.webserver.Route;
import cis5550.webserver.datamodels.ContentType;

import java.io.File;
import java.io.IOException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

import static cis5550.webserver.Server.*;

public class EepyCrawlSearch {
    private static final Logger LOGGER = Logger.getLogger(EepyCrawlSearch.class);

    private static final String PAGE_DIR = "pages";
    private static final double WEIGHT_TFIDF = 10;
    private static final double WEIGHT_PAGERANK = 1;
    private static final double UPDATE_IN_TITLE = 3.0;
    private static final double UPDATE_IN_SNIPPET = 2.0;
    private static final double PENALTY_TITLE_MISSING = 0.01;
    private static final double PENALTY_SNIPPET_MISSING = 0.1;

    private static int port = 443;

    public static void main(String[] args) {
        if (args.length > 0) {
            port = Integer.parseInt(args[0]);
        }
        port(port);
        LOGGER.info("Starting EepyCrawlSearch on port " + port);

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

        Map<String, Double> myTFIDFScores = TFIDF.aggregateTFIDFScores(aQuery);
        Map<String, Double> myPagerankScores = new HashMap<>();
        try {
            myPagerankScores = FrontendKVSClient.getPagerankScores(myTFIDFScores.keySet());
        } catch (IOException e) {
            LOGGER.error("Error getting pagerank scores from KVS");
        }

        Map<String, UrlInfo> myInfoPerUrl = new HashMap<>();
        try {
            myInfoPerUrl = FrontendKVSClient.getInfoPerUrl(myTFIDFScores.keySet().stream().toList());
        } catch (IOException e) {
            LOGGER.error("Error getting titles per URL from KVS");
        }

        Map<String, Double> myCombinedScores = new HashMap<>();
        for (String myUrl : myTFIDFScores.keySet()) {
            double myTFIDFScore = myTFIDFScores.get(myUrl);
            double myPagerankScore = 0.0;
            if (myPagerankScores.containsKey(myUrl)) {
                myPagerankScore = myPagerankScores.get(myUrl);
            }
            myCombinedScores.put(myUrl, getFinalCombinedScore(myTFIDFScore, myPagerankScore, aQuery, myUrl, myInfoPerUrl));
        }

        List<SearchResult> myResults = buildSearchResultsFromScores(myCombinedScores, myInfoPerUrl);
        try {
            FrontendKVSClient.putInCache(aQuery, myResults);
        } catch (IOException e) {
            LOGGER.error("Error putting search results in cache");
        }

        LOGGER.info("Returning search results for query: " + aQuery);

        return myResults;
    }

    private static Double getFinalCombinedScore(Double aTFIDFScore, Double aPagerankScore, String aQuery, String aUrl, Map<String, UrlInfo> aInfoPerUrl) {

        if (Double.isNaN(aTFIDFScore) || Double.isInfinite(aTFIDFScore)) {
            System.out.println("TFIDF score is NaN or infinite, returning 0.0");
            return 0.0;
        }

        if (Double.isNaN(aPagerankScore) || Double.isInfinite(aPagerankScore)) {
            System.out.println("Pagerank score is NaN or infinite, returning 0.0");
            return 0.0;
        }

        String myDecodedUrl = URLDecoder.decode(aUrl, StandardCharsets.UTF_8);
        UrlInfo myInfo = aInfoPerUrl.get(myDecodedUrl);

        double myNormalizationFactor = 1;
        double myNormalizedTFIDF = aTFIDFScore / myNormalizationFactor;
        double myNormalizedPagerank = aPagerankScore / myNormalizationFactor;

        double myCombinedScore = 0.0;

        if (myInfo != null) {

            String myTitle = myInfo.title().toLowerCase();
            String mySnippet = myInfo.snippet().toLowerCase();
            String[] myQueryTerms = aQuery.split(" ");

            for (int i = 0; i < Math.min(5, myQueryTerms.length); i++) {
                if (myTitle.contains(myQueryTerms[i])) {
                    System.out.println("Title " + myTitle + " contains query term " + myQueryTerms[i] + ", boosting TFIDF");
                    myNormalizedTFIDF *= UPDATE_IN_TITLE;
                    break;
                }
                if (mySnippet.contains(myQueryTerms[i])) {
                    System.out.println("Snippet " + mySnippet + " contains query term " + myQueryTerms[i] + ", boosting TFIDF");
                    myNormalizedTFIDF *= UPDATE_IN_SNIPPET;
                    break;
                }
            }

            myCombinedScore = WEIGHT_TFIDF * myNormalizedTFIDF + WEIGHT_PAGERANK * myNormalizedPagerank;

            if (myTitle == null || myTitle.isEmpty() || myTitle.equalsIgnoreCase(myDecodedUrl)) {
                System.out.println("Title missing, decreased score from " + myCombinedScore + " to " + (myCombinedScore * 0.3));
                myCombinedScore *= PENALTY_TITLE_MISSING;
            }
            if (mySnippet == null || mySnippet.isEmpty()) {
                System.out.println("Snippet missing, decreased score from " + myCombinedScore + " to " + (myCombinedScore * 0.3));
                myCombinedScore *= PENALTY_SNIPPET_MISSING;
            }
        }

        System.out.println("Final combined score for " + aUrl + " is " + myCombinedScore);

        return myCombinedScore;
    }

    private static List<SearchResult> buildSearchResultsFromScores(Map<String, Double> aScores, Map<String, UrlInfo> aInfoPerUrl) {
        List<SearchResult> results = aScores.entrySet().stream().sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                .map(entry -> {
                    try {
                        String decodedUrl = URLDecoder.decode(entry.getKey(), StandardCharsets.UTF_8);
                        if (decodedUrl == null || aInfoPerUrl.get(decodedUrl) == null || !decodedUrl.contains("http")) {
                            return null;
                        } else {
                            return new SearchResult(
                                    aInfoPerUrl.get(decodedUrl).title(),
                                    decodedUrl,
                                    aInfoPerUrl.get(decodedUrl).snippet(),
                                    entry.getValue()
                            );
                        }
                    } catch (Exception e) {
                        LOGGER.error("Failed to debug URL: " + e);
                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .toList();

        aScores.clear();
        aInfoPerUrl.clear();

        return results;
    }
}
