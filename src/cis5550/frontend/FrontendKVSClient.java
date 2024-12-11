package cis5550.frontend;

import cis5550.jobs.datamodels.TableColumns;
import cis5550.tools.HTMLParser;
import cis5550.utils.CollectionsUtils;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Hasher;
import cis5550.tools.Logger;
import cis5550.utils.CacheTableEntryUtils;

import java.io.IOException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.*;
import java.util.stream.Collectors;

import static cis5550.jobs.datamodels.TableName.*;
import static cis5550.kvs.Worker.NULL_RETURN;
import static cis5550.utils.Stopwords.isStopWord;

public class FrontendKVSClient {
    private static final Logger LOGGER = Logger.getLogger(FrontendKVSClient.class);

    private static final String KVS_COORDINATOR = "localhost:8000";
    private static final KVSClient KVS_CLIENT = new KVSClient(KVS_COORDINATOR);

    private static final String COLON = ":";
    private static final String COMMA = ",";

    private static final int CRAWL_TABLE_BATCH_SIZE = 50;
    private static final int NUM_THREADS = 8;

    public static Map<String, Integer> getUrlCountData(String aQuery) throws IOException {
        LOGGER.info("Getting URL count data for query: " + aQuery);
        Row myRow = KVS_CLIENT.getRow(INDEX_TABLE.getName(), aQuery);

        if (myRow == null) {
            return new HashMap<>();
        }

        String myIndexEntry = myRow.get(TableColumns.VALUE.value());
        String[] myIndexItems = myIndexEntry.split(COMMA);

        Map<String, Integer> myUrlCountData = new ConcurrentHashMap<>();

        for (String myIndexItem : myIndexItems) {
            String[] myIndexEntryArr = myIndexItem.split(COLON);

            try {
                Integer myCount = Integer.parseInt(myIndexEntryArr[myIndexEntryArr.length - 1]);
                if (myIndexItem.lastIndexOf(COLON) > 0) {
                    String myUrl = myIndexItem.substring(0, myIndexItem.lastIndexOf(COLON));
                    myUrlCountData.put(URLDecoder.decode(myUrl, StandardCharsets.UTF_8), myCount);
                }
            } catch (NumberFormatException e) {
                LOGGER.error("Error parsing count from index entry: " + myIndexItem);
            }
        }
        LOGGER.info("URL count data for query: " + aQuery + " is: " + myUrlCountData);

        return myUrlCountData;
    }

    public static Map<String, UrlInfo> getInfoPerUrl(List<String> aUrlList) throws IOException {
        LOGGER.info("Getting titles for " + aUrlList.size() + " URLs");

        Map<String, String> myPageContents = partitionedBatchedGetCrawlTableValues(aUrlList);

        Pattern patternTitle = Pattern.compile("<title[^>]*>([\\s\\S]*?)</title>", Pattern.CASE_INSENSITIVE);
        Pattern patternSnippet = Pattern.compile("<meta\\s+name\\s*=\\s*['\"]description['\"]\\s+content\\s*=\\s*['\"](.*?)['\"]", Pattern.CASE_INSENSITIVE);

        Map<String, UrlInfo> infoPerUrl = new HashMap<>();
        for (String s : aUrlList) {
            try {
                String myNormalizedUrl = URLDecoder.decode(s, StandardCharsets.UTF_8);
                String myPageContent = myPageContents.get(Hasher.hash(myNormalizedUrl));
                if (myPageContent == null) {
                    continue;
                }
                Matcher matcherTitle = patternTitle.matcher(myPageContent);
                Matcher matcherSnippet = patternSnippet.matcher(myPageContent);

                String title = myNormalizedUrl;
                String snippet = "No preview available";

                if (matcherTitle.find()) {
                    title = matcherTitle.group(1).trim();
                    title = HTMLParser.unescapeHtml(title);
                }
                if (matcherSnippet.find()) {
                    snippet = matcherSnippet.group(1).trim();
                    snippet = HTMLParser.unescapeHtml(snippet);
                }

                infoPerUrl.put(myNormalizedUrl, new UrlInfo(title, snippet));
            } catch (Exception e) {
                LOGGER.error("Exception thrown while getting URL info: " + e);
            }
        }
        return infoPerUrl;
    }

    public static Map<String, Integer> getNumTermsPerUrl(Set<String> aUrlSet) throws IOException {
        LOGGER.info("Getting number of terms per URL for " + aUrlSet.size() + " URLs");

        Map<String, String> myPageContents = partitionedBatchedGetCrawlTableValues(aUrlSet);
        Map<String, Integer> myNumTermsPerUrl = myPageContents.entrySet().stream()
                .map(myEntry -> Map.entry(myEntry.getKey(), getNumTermsInUrl(myEntry.getValue())))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        myPageContents.clear();

        LOGGER.info("Number of terms per URL for " + aUrlSet.size() + " URLs: " + myNumTermsPerUrl);
        return myNumTermsPerUrl;
    }

    private static Integer getNumTermsInUrl(String aPage) {
        if (aPage == null) {
            return 0;
        }

        Set<String> myTerms = new HashSet<>();

        Pattern wordPattern = Pattern.compile("\\b[a-zA-Z0-9]{5,19}\\b"); // Words between 5 and 19 characters long
        String textContent = aPage.replaceAll("<[^>]*>", " ");

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
        LOGGER.info("Getting pagerank scores for " + aUrls.size() + " URLs");
        Map<String, Double> myPagerankScores = new HashMap<>();

        List<String> myUrlList = new ArrayList<>(aUrls);

        List<String> myPageranks = KVS_CLIENT.batchGetColValue(PAGERANK_TABLE.getName(), TableColumns.RANK.value(), myUrlList);

        for (int i = 0; i < myUrlList.size(); i++) {
            String myUrl = myUrlList.get(i);
            String myPagerank = myPageranks.get(i);
            if (myPagerank != null && !myPagerank.equals(NULL_RETURN)) {
                myPagerankScores.put(myUrl, Double.parseDouble(myPagerank));
            }
        }
        LOGGER.info("Pagerank scores for " + aUrls.size() + " URLs: " + myPagerankScores);
        return myPagerankScores;
    }

    public static List<SearchResult> getFromCache(String aQuery) throws IOException {
        aQuery = aQuery.replaceAll("\\s+", "");
        Row myQueryRow = KVS_CLIENT.getRow(CACHE_TABLE.getName(), aQuery);
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
        aQuery = aQuery.replaceAll("\\s+", "");
        Row myRow = new Row(aQuery);
        myRow.put(TableColumns.VALUE.value(), myEntry);
        KVS_CLIENT.putRow(CACHE_TABLE.getName(), myRow);
    }

    private static Map<String, String> partitionedBatchedGetCrawlTableValues(Collection<String> aAllUrls) {
        ConcurrentMap<String, String> myPageContents = new ConcurrentHashMap<>();
        Collection<String> myUrlHashes = aAllUrls.stream()
                .map(Hasher::hash)
                .toList();

        ExecutorService myExecutor = Executors.newFixedThreadPool(NUM_THREADS);

        List<Collection<String>> myPartitionedUrlHahes = CollectionsUtils.partition(myUrlHashes, CRAWL_TABLE_BATCH_SIZE);
        List<Callable<Map<String, String>>> myTasks = new ArrayList<>(myPartitionedUrlHahes.size());

        for (Collection<String> myUrlHashesBatch : myPartitionedUrlHahes) {
            myTasks.add(() -> {
                try {
                    List<String> myPartitionPageContents = KVS_CLIENT.batchGetColValue(CRAWL_TABLE.getName(), TableColumns.PAGE.value(), myUrlHashesBatch.stream().toList());
                    Map<String, String> myPartitionPageContentsMap = new HashMap<>();

                    Iterator<String> hashIterator = myUrlHashesBatch.iterator();
                    for (String pageContent : myPartitionPageContents) {
                        if (hashIterator.hasNext()) {
                            myPartitionPageContentsMap.put(hashIterator.next(), pageContent);
                        }
                    }

                    return myPartitionPageContentsMap;
                } catch (IOException e) {
                    LOGGER.error("Error getting page contents from KVS");
                    return null;
                }
            });
        }

        try {
            myExecutor.invokeAll(myTasks).forEach(myFuture -> {
                try {
                    myPageContents.putAll(myFuture.get());
                } catch (InterruptedException | ExecutionException e) {
                    LOGGER.error("Error getting page contents from KVS");
                }
            });
        } catch (InterruptedException e) {
            LOGGER.error("Error getting page contents from KVS, executor interrupted");
        }
        return myPageContents;
    }
}
