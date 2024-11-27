package cis5550.jobs;

import cis5550.flame.FlameContext;
import cis5550.flame.FlamePair;
import cis5550.flame.FlamePairRDD;
import cis5550.jobs.datamodels.TableColumns;
import cis5550.tools.Hasher;
import cis5550.tools.Logger;

import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static cis5550.tools.URLHelper.cleanupUrl;
import static cis5550.tools.URLHelper.normalizeURL;

public class NewPageRank {
    private static final Logger LOGGER = Logger.getLogger(NewPageRank.class);
    private static final String CRAWL_TABLE = "pt-crawl";
    private static final String PAGERANK_TABLE = "pt-pageranks";
    private static final String UNIQUE_SEPARATOR = "@#!@!#@";

    private static final int CONVERGENCE_PERCENTAGE = 100;
    private static final double CONVERGENCE_THRESHOLD = 0.01;

    private static final double INIT_PAGERANK = 1.0;
    private static final String COMMA = ",";
    private static final double DAMPING_FACTOR = 0.85;
    private static final double RANK_SOURCE = 0.15;

    public static void run(FlameContext aContext, String[] aArgs) throws Exception {

        FlamePairRDD myPageRankRDD = prepareInitPagerankTable(aContext);

        int myIterations = 1;
        while (true) {
            LOGGER.debug("Iteration " + myIterations++);
            myPageRankRDD = pagerankIterate(myPageRankRDD);

            if (hasConverged(myPageRankRDD)) {
                break;
            }
        }

        savePagerankTable(aContext, myPageRankRDD);
        aContext.output("PageRank completed");
    }

    private static List<String> extractUrls(String aContent) {
        List<String> myUrls = new LinkedList<>();

        Pattern myTagPattern = Pattern.compile("<\\s*a\\b[^>]*>", Pattern.CASE_INSENSITIVE);
        Pattern myHrefPattern = Pattern.compile("href\\s*=\\s*\"([^\"]*)\"", Pattern.CASE_INSENSITIVE);

        Matcher myTagMatcher = myTagPattern.matcher(aContent);
        while (myTagMatcher.find()) {
            String myTag = myTagMatcher.group();

            Matcher myHrefMatcher = myHrefPattern.matcher(myTag);
            if (myHrefMatcher.find()) {
                String myUrl = myHrefMatcher.group(1);
                myUrls.add(myUrl);
            }
        }
        return myUrls;
    }

    private static FlamePairRDD prepareInitPagerankTable(FlameContext aContext) throws Exception {
        return aContext.fromTable(CRAWL_TABLE, myRow -> {
                    try {
                        String myUrl = myRow.get(TableColumns.URL.value());
                        String myPage = myRow.get(TableColumns.PAGE.value());
                        return myUrl + UNIQUE_SEPARATOR + myPage;
                    } catch (Exception e) {
                        LOGGER.error("Error while reading an element from crawl table. Skipping. ");
                        return null;
                    }
                })
                .mapToPair(myUrlPageString -> {
                    if (myUrlPageString != null) {
                        String[] myUrlPage = myUrlPageString.split(UNIQUE_SEPARATOR);
                        if (myUrlPage.length != 2) {
                            LOGGER.error("Invalid URL page pair: " + myUrlPageString);
                            return null;
                        }

                        String myUrl = myUrlPage[0];
                        String myPage = myUrlPage[1];

                        String[] myUrlParts = cleanupUrl(myUrl);
                        String myCleanedUrl = myUrlParts[0] + "://" + myUrlParts[1] + ":" + myUrlParts[2] + myUrlParts[3];
                        List<String> myNormalizedUrls = extractUrls(myPage).stream()
                                .map(url -> normalizeURL(myCleanedUrl, url))
                                .filter(Objects::nonNull)
                                .toList();

                        Set<String> myUrlHashes = myNormalizedUrls.stream().map(Hasher::hash).collect(Collectors.toSet());

                        String myBaseUrlHash = Hasher.hash(myUrl);
                        String myPageRankInit =
                                INIT_PAGERANK + COMMA + INIT_PAGERANK + COMMA + String.join(COMMA, myUrlHashes);
                        return new FlamePair(myBaseUrlHash, myPageRankInit);
                    }
                    return null;
                });
    }

    private static FlamePairRDD pagerankIterate(FlamePairRDD aPageRankRDD) throws Exception {
        FlamePairRDD myTransferTable = aPageRankRDD
                .flatMapToPair(myPair -> {
                    String myUrlHash = myPair._1();
                    String[] myPageRankParts = myPair._2().split(COMMA);
                    double myPageRank = Double.parseDouble(myPageRankParts[0]);
                    List<String> myUrlHashes = List.of(myPageRankParts).subList(2, myPageRankParts.length);

                    List<FlamePair> myResults = new LinkedList<>();
                    myResults.add(new FlamePair(myUrlHash, String.valueOf(0.0)));
                    for (String myOtherUrlHash : myUrlHashes) {
                        myResults.add(new FlamePair(
                                myOtherUrlHash, String.valueOf(DAMPING_FACTOR * myPageRank / myUrlHashes.size())));
                    }
                    return myResults;
                })
                .foldByKey("", (a, b) -> {
                    if (a.isEmpty()) {
                        return b;
                    }
                    double myPageRank = Double.parseDouble(a) + Double.parseDouble(b);
                    return String.valueOf(myPageRank);
                });

        return aPageRankRDD.join(myTransferTable).flatMapToPair(myPair -> {
            String myUrlHash = myPair._1();
            List<String> myParts = List.of(myPair._2().split(COMMA));
            double myPageRank = Double.parseDouble(myParts.getFirst());
            double myNewPageRank = Double.parseDouble(myParts.getLast()) + RANK_SOURCE;
            List<String> myOutlinks = myParts.subList(2, myParts.size() - 1);

            return List.of(new FlamePair(
                    myUrlHash, myNewPageRank + COMMA + myPageRank + COMMA + String.join(COMMA, myOutlinks)));
        });
    }

    private static boolean hasConverged(FlamePairRDD aPageRankRDD)
            throws Exception {
        String myMaxDiffString = aPageRankRDD
                .flatMap(myPair -> {
                    String[] myParts = myPair._2().split(COMMA);
                    double myPageRank = Double.parseDouble(myParts[0]);
                    double myPreviousPageRank = Double.parseDouble(myParts[1]);
                    return List.of(String.valueOf(Math.abs(myPageRank - myPreviousPageRank)));
                })
                .flatMap(myDiff -> Double.parseDouble(myDiff) < CONVERGENCE_THRESHOLD ? List.of("1,1") : List.of("0,1"))
                .fold("0,0", (a, b) -> {
                    String[] myParts = a.split(COMMA);
                    String[] myOtherParts = b.split(COMMA);
                    int myCount = Integer.parseInt(myParts[0]) + Integer.parseInt(myOtherParts[0]);
                    int myTotal = Integer.parseInt(myParts[1]) + Integer.parseInt(myOtherParts[1]);

                    return myCount + COMMA + myTotal;
                });

        String[] myParts = myMaxDiffString.split(COMMA);
        int myCount = Integer.parseInt(myParts[0]);
        int myTotal = Integer.parseInt(myParts[1]);

        return (myCount / myTotal * 100 >= CONVERGENCE_PERCENTAGE);
    }

    private static void savePagerankTable(FlameContext aContext, FlamePairRDD aPageRankRDD) throws Exception {
        aPageRankRDD
                .flatMapToPair(myPair -> {
                    String myUrlHash = myPair._1();
                    String[] myParts = myPair._2().split(COMMA);
                    double myPageRank = Double.parseDouble(myParts[0]);
                    aContext.getKVS()
                            .put(PAGERANK_TABLE, myUrlHash, TableColumns.RANK.value(), String.valueOf(myPageRank));
                    return List.of();
                });
    }
}
