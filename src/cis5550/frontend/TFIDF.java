package cis5550.frontend;

import cis5550.tools.Hasher;
import cis5550.tools.Logger;
import cis5550.frontend.FrontendKVSClient;
import cis5550.tools.PorterStemmer;
import cis5550.utils.CollectionsUtils;

import java.io.IOException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TFIDF {
    private static final Logger LOGGER = Logger.getLogger(TFIDF.class);

    private static final double APPROX_CORPUS_SIZE = 100000.0;

    public static Map<String, Double> getTFIDFScores(String aQuery) {
        LOGGER.info("Getting TF-IDF scores for query: " + aQuery);
        Map<String, Integer> myUrlCountData = new HashMap<>();

        PorterStemmer myStemmer = new PorterStemmer();
        myStemmer.add(aQuery.toCharArray(), aQuery.length());
        myStemmer.stem();
        String myStemmedWord = new String(myStemmer.getResultBuffer());

        Map<String, Integer> myStemmedUrlCountData;
        try {
            myUrlCountData = FrontendKVSClient.getUrlCountData(aQuery);
            myStemmedUrlCountData = FrontendKVSClient.getUrlCountData(myStemmedWord);

            for (String myUrl : myStemmedUrlCountData.keySet()) {
                if (myUrlCountData.containsKey(myUrl)) {
                    myUrlCountData.put(myUrl, myUrlCountData.get(myUrl) + myStemmedUrlCountData.get(myUrl));
                } else {
                    myUrlCountData.put(myUrl, myStemmedUrlCountData.get(myUrl));
                }
            }
        } catch (IOException e) {
            LOGGER.error("Error getting URL count data from KVS");
        }

        Map<String, Integer> myUrlTermCountData = new HashMap<>();
        try {
            myUrlTermCountData = FrontendKVSClient.getNumTermsPerUrl(myUrlCountData.keySet());
        } catch (IOException e) {
            LOGGER.error("Error getting URL Term count data from KVS");
        }

        int myDF = myUrlCountData.size();
        double myIDF = Math.log(APPROX_CORPUS_SIZE / (1 + myDF));

        Map<String, Double> myTFIDFScores = new HashMap<>();
        for (String myUrl : myUrlCountData.keySet()) {
            Integer myQueryCount = myUrlCountData.get(myUrl);
            Integer myTermCount = myUrlTermCountData.get(Hasher.hash(myUrl));
            if (myQueryCount != null && myTermCount != null) {
                double myTF = (double) myQueryCount / myTermCount;
                myTFIDFScores.put(myUrl, myTF * myIDF);
            }
        }

        LOGGER.info("TF-IDF scores for query: " + aQuery + " are: " + myTFIDFScores);

        return myTFIDFScores;
    }
}