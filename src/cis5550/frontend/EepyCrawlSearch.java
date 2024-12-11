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

        Map<String, Double> myTFIDFScores = TFIDF.getTFIDFScores(aQuery);
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
        double myTFIDFWeight = 0.95;
        double myTitleWeightUpdate = 3.0;
        double myTitleMissingUpdate = 0.4;
        double mySnippetMissingUpdate = 0.3;

        double myNormalizationFactor = aTFIDFScore + aPagerankScore;

        if (myNormalizationFactor == 0) {
            return 0.0;
        }

        double myNormalizedTFIDF = aTFIDFScore / myNormalizationFactor;
        double myNormalizedPagerank = aPagerankScore / myNormalizationFactor;

        // TODO: If the term is in the title, boost its TFIDF * 2
        String myDecodedUrl = URLDecoder.decode(aUrl, StandardCharsets.UTF_8);
        UrlInfo myInfo = aInfoPerUrl.get(myDecodedUrl);
        if (myInfo != null) {
            String myTitle = myInfo.title().toLowerCase();
            String mySnippet = myInfo.snippet().toLowerCase();
            String[] myQueryTerms = aQuery.split(" ");
            for (int i = 0; i < Math.min(5, myQueryTerms.length); i++) {
                if (myTitle.contains(myQueryTerms[i])) {
                    System.out.println("Title " + myTitle + " contains query term " + myQueryTerms[i] + ", boosting TFIDF");
                    myNormalizedTFIDF *= myTitleWeightUpdate;
                    break;
                }
                if (mySnippet.contains(myQueryTerms[i])) {
                    System.out.println("Snippet " + mySnippet + " contains query term " + myQueryTerms[i] + ", boosting TFIDF");
                    myNormalizedTFIDF *= myTitleWeightUpdate;
                    break;
                }
            }
        }

        double myCombinedScore = myTFIDFWeight * myNormalizedTFIDF + (1 - myTFIDFWeight) * myNormalizedPagerank;
        if (Double.isNaN(myCombinedScore)) {
            System.out.println("Combined score is NaN, returning 0.0");
            return 0.0;
        }


        if (myInfo != null) {
            String myTitle = myInfo.title().toLowerCase();
            if (myTitle == null || myTitle.isEmpty() || myTitle.equalsIgnoreCase(myDecodedUrl)) {
                System.out.println("Title missing, decreased score from " + myCombinedScore + " to " + (myCombinedScore * 0.3));
                myCombinedScore *= myTitleMissingUpdate;
            }
            String mySnippet = myInfo.snippet().toLowerCase();
            if (mySnippet == null || mySnippet.isEmpty()) {
                System.out.println("Snippet missing, decreased score from " + myCombinedScore + " to " + (myCombinedScore * 0.3));
                myCombinedScore *= mySnippetMissingUpdate;
            }
        }

        return myCombinedScore;
    }

    private static List<SearchResult> buildSearchResultsFromScores(Map<String, Double> aScores, Map<String, UrlInfo> aInfoPerUrl) {
        return aScores.entrySet().stream().sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                .map(entry -> {
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
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }
}
