package cis5550.jobs;

import cis5550.flame.FlameContext;
import cis5550.flame.FlamePair;
import cis5550.flame.FlamePairRDD;
import cis5550.flame.FlameRDD;
import cis5550.jobs.datamodels.TableColumns;
import cis5550.tools.Hasher;
import cis5550.tools.Logger;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static cis5550.tools.URLHelper.cleanupUrl;
import static cis5550.tools.URLHelper.normalizeURL;

public class NewPageRank {
    private static final Logger LOGGER = Logger.getLogger(NewPageRank.class);
    private static final String CRAWL_TABLE = "pt-crawl";
    private static final String PAGERANK_TABLE = "pt-pageranks";

    private static final int CONVERGENCE_PERCENTAGE = 95;
    private static final double CONVERGENCE_THRESHOLD = 0.05;

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
        LOGGER.debug("Pagerank completed");
        aContext.output("RAHHH");
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
        LOGGER.debug("Preparing Pagerank Table");
        return aContext.pairFromTable(CRAWL_TABLE, myRow -> {
            try {
                String myUrl = myRow.get(TableColumns.URL.value());
                String myPage = myRow.get(TableColumns.PAGE.value());

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
            } catch (Exception e) {
                LOGGER.error("Error while reading an element from crawl table. Skipping. ");
                return null;
            }
        });
    }

    private static FlamePairRDD pagerankIterate(FlamePairRDD aPageRankRDD) throws Exception {
        FlamePairRDD myPageRankCalculations = aPageRankRDD
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
                });

        FlamePairRDD myTransferTable = myPageRankCalculations
                .foldByKey("", (a, b) -> {
                    if (a.isEmpty()) {
                        return b;
                    }
                    double myPageRank = Double.parseDouble(a) + Double.parseDouble(b);
                    return String.valueOf(myPageRank);
                });

        myPageRankCalculations.destroy();

        FlamePairRDD myJoinedRDD = aPageRankRDD.join(myTransferTable);

        aPageRankRDD.destroy();
        myTransferTable.destroy();

        FlamePairRDD myNextPageRankRDD = myJoinedRDD.flatMapToPair(myPair -> {
            String myUrlHash = myPair._1();
            List<String> myParts = List.of(myPair._2().split(COMMA));
            double myPageRank = Double.parseDouble(myParts.getFirst());
            double myNewPageRank = Double.parseDouble(myParts.getLast()) + RANK_SOURCE;
            List<String> myOutlinks = myParts.subList(2, myParts.size() - 1);

            return List.of(new FlamePair(
                    myUrlHash, myNewPageRank + COMMA + myPageRank + COMMA + String.join(COMMA, myOutlinks)));
        });

        myJoinedRDD.destroy();

        return myNextPageRankRDD;
    }

    private static boolean hasConverged(FlamePairRDD aPageRankRDD)
            throws Exception {

        LOGGER.debug("Checking convergence");
        FlamePairRDD.StringPairToString foldLambda = (a, b) -> {
            String[] myCounts = a.split(COMMA);
            int myConvergedCount = Integer.parseInt(myCounts[0]);
            int myTotalCount = Integer.parseInt(myCounts[1]);

            String[] myParts = b._2().split(COMMA);
            double myPageRank = Double.parseDouble(myParts[0]);
            double myPreviousPageRank = Double.parseDouble(myParts[1]);
            double myPageRankDiff = Math.abs(myPageRank - myPreviousPageRank);

            myTotalCount += 1;
            if (myPageRankDiff < CONVERGENCE_THRESHOLD) {
                myConvergedCount += 1;
            }

            return myConvergedCount + COMMA + myTotalCount;
        };

        FlamePairRDD.TwoStringsToString aggLambda = (a, b) -> {
            String[] myParts = a.split(COMMA);
            String[] myOtherParts = b.split(COMMA);
            int myCount = Integer.parseInt(myParts[0]) + Integer.parseInt(myOtherParts[0]);
            int myTotal = Integer.parseInt(myParts[1]) + Integer.parseInt(myOtherParts[1]);

            return myCount + COMMA + myTotal;
        };

        String myMaxDiffString = aPageRankRDD.fold("0,0", foldLambda, aggLambda);

        String[] myParts = myMaxDiffString.split(COMMA);
        int myCount = Integer.parseInt(myParts[0]);
        int myTotal = Integer.parseInt(myParts[1]);

        double myConvergedPercentage = ((double) myCount / myTotal) * 100;

        LOGGER.info("Converged URLs: " + myCount + "/" + myTotal + " (" + myConvergedPercentage + "%)");

        return myConvergedPercentage >= CONVERGENCE_PERCENTAGE;
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
        aPageRankRDD.destroy();
    }
}
