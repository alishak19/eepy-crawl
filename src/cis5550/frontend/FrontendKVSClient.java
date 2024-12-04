package cis5550.frontend;

import cis5550.jobs.datamodels.TableColumns;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.utils.CacheTableEntryUtils;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.*;

import static cis5550.utils.Stopwords.isStopWord;

public class FrontendKVSClient {
    private static final String KVS_COORDINATOR = "localhost:8080";
    private static final KVSClient KVS_CLIENT = new KVSClient(KVS_COORDINATOR);

    private static final String CRAWL_TABLE = "pt-crawl";
    private static final String INDEX_TABLE = "pt-index";
    private static final String PAGERANK_TABLE = "pt-pagerank";
    private static final String CACHE_TABLE = "pt-cache";

    private static final String COLON = ":";
    private static final String COMMA = ",";

    public static Map<String, Integer> getUrlCountData(String aQuery) throws IOException {
        Row myRow = KVS_CLIENT.getRow(INDEX_TABLE, aQuery);

        if (myRow == null) {
            return new HashMap<>();
        }

        String myIndexEntry = myRow.get(TableColumns.VALUE.value());
        String[] myIndexItems = myIndexEntry.split(COMMA);

        Map<String, Integer> myUrlCountData = new ConcurrentHashMap<>();

        for (String myIndexItem : myIndexItems) {
            String[] myIndexEntryArr = myIndexItem.split(COLON);
            Integer myCount = Integer.parseInt(myIndexEntryArr[myIndexEntryArr.length - 1]);
            myUrlCountData.put(myIndexItem.substring(0, myIndexItem.lastIndexOf(COLON)), myCount);
        }

        return myUrlCountData;
    }

    public static Map<String, Integer> getNumTermsPerUrl(Set<String> aUrlSet) throws IOException {
        Map<String, Integer> numTermsPerUrl = new HashMap<>();
        for (String myUrl : aUrlSet) {
            int myNumTerms = getNumTermsInUrl(myUrl);
            numTermsPerUrl.put(myUrl, myNumTerms);
        }
        return numTermsPerUrl;
    }

    private static Integer getNumTermsInUrl(String aUrl) throws IOException {
        Row myRow = KVS_CLIENT.getRow(CRAWL_TABLE, aUrl);
        if (myRow == null) {
            return 0;
        }

        String myPage = myRow.get(TableColumns.PAGE.value());
        if (myPage == null) {
            return 0;
        }

        Set<String> myTerms = new HashSet<>();

        Pattern wordPattern = Pattern.compile("\\b[a-zA-Z0-9]{5,19}\\b"); // Words between 5 and 19 characters long
        String textContent = myPage.replaceAll("<[^>]*>", " ");

        Matcher matcher = wordPattern.matcher(textContent.toLowerCase());
        while (matcher.find()) {
            String word = matcher.group();
            word = normalizeWord(word);

            if (!isStopWord(word)) {
                myTerms.add(word);
            }
        }

        return myTerms.size();
    }

    private static String normalizeWord(String word) {
        return word.replaceAll("[^a-zA-Z0-9]", "")
                .toLowerCase();
    }

    public static Map<String, Double> getPagerankScores(Collection<String> aUrls) throws IOException {
        Map<String, Double> myPagerankScores = new HashMap<>();
        for (String myUrl : aUrls) {
            Row myRow = KVS_CLIENT.getRow(PAGERANK_TABLE, myUrl);
            if (myRow == null) {
                continue;
            }
            myPagerankScores.put(myUrl, Double.parseDouble(myRow.get(TableColumns.RANK.value())));
        }
        return myPagerankScores;
    }

    public static List<SearchResult> getFromCache(String aQuery) throws IOException {
        Row myQueryRow = KVS_CLIENT.getRow(CACHE_TABLE, aQuery);
        if (myQueryRow == null) {
            return null;
        }
        return CacheTableEntryUtils.parseEntry(myQueryRow.get(TableColumns.VALUE.value()));
    }

    public static void putInCache(String aQuery, List<SearchResult> aSearchResults) throws IOException {
        if (aSearchResults == null) {
            return;
        }
        String myEntry = CacheTableEntryUtils.createEntry(aSearchResults);
        Row myRow = new Row(aQuery);
        myRow.put(TableColumns.VALUE.value(), myEntry);
        KVS_CLIENT.putRow(CACHE_TABLE, myRow);
    }
}
