package cis5550.jobs;

import cis5550.flame.FlameContext;
import cis5550.flame.FlamePair;
import cis5550.flame.FlamePairRDD;
import cis5550.flame.FlameRDD;
import cis5550.jobs.datamodels.TableColumns;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Hasher;
import cis5550.tools.Logger;
import cis5550.tools.URLHelper;

import java.util.*;
import java.util.stream.Collectors;

public class PageRank {

    private static final Logger LOGGER = Logger.getLogger(PageRank.class);

    private static final String CRAWL_TABLE = "pt-crawl";
    private static final String PAGERANK_TABLE = "pt-pageranks";
    private static final String STATE_TABLE = "pt-state";
    private static final String NEW_RANKS_TABLE = "pt-new-ranks";

    private static final String COMMA = ",";
    private static final String ZERO_DOUBLE = "0.0";
    private static final String ORIG_STATE = "1.0,1.0,";
    private static final String ONE_SINGLETON = "1";
    private static final String ZERO_SINGLETON = "0";

    public static void run(FlameContext context, String[] args) throws Exception {

        LOGGER.debug("Starting PageRank run job...");

        double convergenceRate;
        double convergencePercentage;

        if (args.length > 0) {
            convergenceRate = Double.parseDouble(args[0]);
        } else {
            convergenceRate = 0.01;
        }

        if (args.length > 1) {
            convergencePercentage = Double.parseDouble(args[1]);
        } else {
            convergencePercentage = 100.0;
        }

        FlameRDD urlPageStrings = context.fromTable(CRAWL_TABLE, row -> {
            String url = row.get(TableColumns.URL.value());
            String pageContent = TableColumns.PAGE.value();
            return url + COMMA + pageContent;
        });

        FlamePairRDD stateTable = urlPageStrings.mapToPair(record -> {
            String[] parts = record.split(COMMA, 2);
            String url = parts[0];
            byte[] contentBytes = parts[1].getBytes();

            List<String> extractedLinks = URLHelper.extractUrls(contentBytes);

            String normalizedLinks = String.join(COMMA, extractedLinks.stream()
                    .map(link -> URLHelper.normalizeURL(url, link))
                    .filter(Objects::nonNull)
                    .map(Hasher::hash)
                    .collect(Collectors.toSet()));

            String stateValue = ORIG_STATE + normalizedLinks;
            return new FlamePair(Hasher.hash(url), stateValue);
        });

        stateTable.saveAsTable(STATE_TABLE);

        int iteration = 1;
        while (true) {
            LOGGER.debug("Starting iteration: " + iteration);
            KVSClient client = context.getKVS();

            FlamePairRDD transferTable = stateTable.flatMapToPair(pair -> {
                LOGGER.debug("Original pair: " + pair._1() + ", " + pair._2());

                String urlHash = pair._1();
                String[] stateParts = pair._2().split(COMMA, 3);
                double currentRank = Double.parseDouble(stateParts[0]);
                String[] links = stateParts[2].isEmpty() ? new String[0] : stateParts[2].split(COMMA);
                int numLinks = links.length;

                double transferValue = (numLinks > 0) ? 0.85 * currentRank / numLinks : 0.0;

                List<FlamePair> transferPairs = Arrays.stream(links)
                        .map(link -> new FlamePair(link, String.valueOf(transferValue)))
                        .collect(Collectors.toList());

                if (!Arrays.asList(links).contains(urlHash)) {
                    transferPairs.add(new FlamePair(urlHash, ZERO_DOUBLE));
                }

                transferPairs.forEach(pairOutput ->
                        LOGGER.debug("Emitted pair: (" + pairOutput._1() + ", " + pairOutput._2() + ")")
                );

                return transferPairs;
            });

            FlamePairRDD aggregatedTransfers = transferTable.foldByKey(ZERO_DOUBLE, (totalRankStr, transferStr) -> {
                double totalRank = Double.parseDouble(totalRankStr);
                double transferValue = Double.parseDouble(transferStr);

                double newRank = totalRank + transferValue;
                return String.valueOf(newRank);
            });

            client.delete(NEW_RANKS_TABLE);
            aggregatedTransfers.saveAsTable(NEW_RANKS_TABLE);

            FlamePairRDD newStateTable = stateTable.join(aggregatedTransfers)
                    .flatMapToPair(pair -> {
                        String urlHash = pair._1();
                        String[] stateAndTransfer = pair._2().split(COMMA, -1);
                        if (stateAndTransfer.length < 4) {
                            return Collections.emptyList();
                        }

                        double currentRank = Double.parseDouble(stateAndTransfer[0]);
                        double transferValue = Double.parseDouble(stateAndTransfer[stateAndTransfer.length - 1]);
                        String[] linkHashes = Arrays.copyOfRange(stateAndTransfer, 2, stateAndTransfer.length - 1);

                        double newCurrentRank = transferValue + 0.15;

                        return Collections.singletonList(new FlamePair(urlHash,
                                newCurrentRank + COMMA + currentRank + COMMA + String.join(COMMA, linkHashes)));
                    });

            client.delete(STATE_TABLE);
            newStateTable.saveAsTable(STATE_TABLE);
            stateTable = newStateTable;

            FlameRDD rankChanges = stateTable.flatMap(pair -> {
                String[] stateValues = pair._2().split(COMMA, -1);

                if (stateValues.length < 2) {
                    return Collections.emptyList();
                }

                double newRank = Double.parseDouble(stateValues[0]);
                double oldRank = Double.parseDouble(stateValues[1]);

                String rankChange = String.valueOf(Math.abs(newRank - oldRank));
                return Collections.singletonList(rankChange);
            });

            double maxRankChange = Double.parseDouble(rankChanges.fold(ZERO_DOUBLE, (accumulatedChangeStr, currentChangeStr) -> {
                double accumulatedChange = Double.parseDouble(accumulatedChangeStr);
                double currentChange = Double.parseDouble(currentChangeStr);
                return String.valueOf(Math.max(accumulatedChange, currentChange));
            }));

            LOGGER.debug("Maximum rank change: " + maxRankChange);

            FlameRDD convergedUrls = stateTable.flatMap(pair -> {
                String[] stateValues = pair._2().split(COMMA, -1);
                if (stateValues.length < 2) {
                    return Collections.emptyList();
                }

                double newRank = Double.parseDouble(stateValues[0]);
                double oldRank = Double.parseDouble(stateValues[1]);

                double rankChange = Math.abs(newRank - oldRank);
                if (rankChange <= convergenceRate) {
                    return Collections.singletonList(ONE_SINGLETON);
                } else {
                    return Collections.singletonList(ZERO_SINGLETON);
                }
            });

            double convergedCount = Double.parseDouble(convergedUrls.fold(ZERO_DOUBLE, (accumulated, current) ->
                    String.valueOf(Double.parseDouble(accumulated) + Double.parseDouble(current)))
            );

            double totalUrls = rankChanges.count();
            double convergedPercentage = (convergedCount / totalUrls) * 100;

            LOGGER.debug("Converged URLs: " + convergedCount + "/" + totalUrls + " (" + convergedPercentage + "%)");

            if (convergedPercentage >= convergencePercentage) {
                LOGGER.debug("Convergence achieved with " + convergedPercentage + "% of URLs on iteration " + iteration);
                break;
            } else if (maxRankChange < convergenceRate) {
                LOGGER.debug("Convergence achieved on iteration " + iteration);
                break;
            } else {
                LOGGER.debug("Continuing iterations with maximum rank change: " + maxRankChange);
                iteration += 1;
            }
        }

        LOGGER.debug("Storing final pageranks!");
        stateTable.flatMap(pair -> {
            String urlHash = pair._1();
            String[] stateValues = pair._2().split(COMMA);

            KVSClient client = context.getKVS();
            if (stateValues.length > 0) {
                Row pagerankRow = new Row(urlHash);
                pagerankRow.put(TableColumns.RANK.value(), stateValues[0]);
                client.putRow(PAGERANK_TABLE, pagerankRow);
            }

            return Collections.emptyList();
        });
    }
}