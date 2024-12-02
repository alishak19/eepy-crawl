package cis5550.frontend;

import cis5550.tools.Logger;
import cis5550.frontend.FrontendKVSClient;

import java.io.IOException;
import java.util.Map;

public class TFIDF {
    private static final Logger LOGGER = Logger.getLogger(TFIDF.class);

    private static final String CRAWL_TABLE = "pt-crawl";
    private static final String INDEX_TABLE = "pt-index";
    
    public static Map<String, Integer> getTFIDFScores(String aQuery) {
        Map<String, Integer> myUrlCountData = null;
        try {
            myUrlCountData = FrontendKVSClient.getUrlCountData(aQuery);
        } catch (IOException e) {
            LOGGER.error("Error getting URL count data from KVS");
        }

        return null;
    }
}