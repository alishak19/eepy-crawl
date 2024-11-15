package cis5550.jobs;

import cis5550.flame.FlameContext;
import cis5550.flame.FlamePair;
import cis5550.flame.FlamePairRDD;
import cis5550.flame.FlameRDD;
import cis5550.jobs.datamodels.TableColumns;
import cis5550.tools.Logger;
import cis5550.tools.PorterStemmer;

import java.util.*;

public class Indexer {
    public static final Logger LOGGER = Logger.getLogger(Indexer.class);
    public static final String CRAWL_TABLE = "pt-crawl";
    public static final String INDEX_TABLE = "pt-index";
    public static final String UNIQUE_SEPARATOR = "&#!#&";
    public static final String COLON = ":";
    public static final String SPACE = " ";
    public static final String COMMA = ",";

    public static void run(FlameContext aContext, String[] aArgs) throws Exception {
        FlameRDD myPageUrls = aContext.fromTable(
                CRAWL_TABLE,
                myRow -> myRow.get(TableColumns.URL.value()) + UNIQUE_SEPARATOR + myRow.get(TableColumns.PAGE.value()));

        FlamePairRDD myPageUrlsPair = myPageUrls.mapToPair(myUrlPageString -> {
            String[] myUrlPage = myUrlPageString.split(UNIQUE_SEPARATOR);
            if (myUrlPage.length != 2) {
                LOGGER.error("Invalid URL page pair: " + myUrlPageString);
                return null;
            }
            return new FlamePair(myUrlPage[0], myUrlPage[1]);
        });

        FlamePairRDD myWordUrlPair = myPageUrlsPair.flatMapToPair(myPair -> {
            String myUrl = myPair._1();
            String myPage = myPair._2();

            Map<String, List<Integer>> myWords = extractWords(myPage);
            return myWords.entrySet().stream()
                    .map(myWord -> new FlamePair(
                            myWord.getKey(),
                            myUrl
                                    + COLON
                                    + String.join(
                                    SPACE,
                                    myWord.getValue().stream()
                                            .map(x -> Integer.toString(x))
                                            .toList())))
                    .toList();
        });

        myWordUrlPair.foldByKey("", (a, b) -> {
            if (a.isEmpty()) {
                return b;
            }

            List<String> myUrls = new LinkedList<>(Arrays.asList(a.split(COMMA)));
            String[] myUrlWord = b.split(COLON);
            int myWordCount = myUrlWord[3].split(SPACE).length;
            for (int i = 0; i < myUrls.size(); i++) {
                String[] myUrlWordCount = myUrls.get(i).split(COLON);
                int myCount = myUrlWordCount[3].split(SPACE).length;
                if (myWordCount > myCount) {
                    myUrls.add(i, b);
                    return String.join(COMMA, myUrls);
                }
            }
            myUrls.add(b);
            return String.join(COMMA, myUrls);
        }).saveAsTable(INDEX_TABLE);
    }

    private static Map<String, List<Integer>> extractWords(String aPage) {
        String myText = aPage.replaceAll("<[^>]*>", " ");

        myText = myText.toLowerCase().replaceAll("[^a-z0-9\\s]", " ");

        String[] myWords = myText.split("\\s+");

        Map<String, List<Integer>> myWordMap = new HashMap<>();
        for (int i = 0; i < myWords.length; i++) {
            String myWord = myWords[i];
            if (myWord.isEmpty()) {
                continue;
            }

            PorterStemmer myStemmer = new PorterStemmer();
            myStemmer.add(myWord.toCharArray(), myWord.length());
            myStemmer.stem();

            String myStemmedWord = myStemmer.toString();
            myWordMap.computeIfAbsent(myWord, k -> new LinkedList<>());
            myWordMap.get(myWord).add(i + 1);
            if (!myStemmedWord.equals(myWord)) {
                myWordMap.computeIfAbsent(myStemmedWord, k -> new LinkedList<>());
                myWordMap.get(myStemmedWord).add(i + 1);
            }
        }

        return myWordMap;
    }
}
