package cis5550.jobs;

import cis5550.flame.FlameContext;
import cis5550.flame.FlamePair;
import cis5550.flame.FlamePairRDD;
import cis5550.flame.FlamePairRDD.PairToPairIterable;
import cis5550.flame.FlamePairRDD.PairToStringIterable;
import cis5550.flame.FlamePairRDD.TwoStringsToString;
import cis5550.flame.FlameRDD;
import cis5550.flame.FlameContext.RowToString;
import cis5550.flame.FlameRDD.StringToPair;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Hasher;
import cis5550.jobs.datamodels.TableColumns;
import cis5550.jobs.Crawler;
import cis5550.tools.URLHelper;
import cis5550.tools.Logger;

import java.util.*;

public class PageRank {
    private static final Logger LOGGER = Logger.getLogger(Indexer.class);
    private static final String CRAWL_TABLE = "pt-crawl";
    private static final String RANK_TABLE = "pt-pageranks";
    private static final String URL_REF = TableColumns.URL.value();
    private static final String PAGE_REF = TableColumns.PAGE.value();
    private static final String DEFAULT_VAL = "0.0";
    private static final String RANK_REF = TableColumns.RANK.value();
	public static void run(FlameContext context, String[] arr) throws Exception {
        double threshold = 1.0;
        if (arr.length > 0) {
            threshold = Double.parseDouble(arr[0]);
        } else {
            LOGGER.error("No threshold");
            return;
        }
        double convergence = -1.0;

        if (arr.length == 2) {
            convergence = Double.parseDouble(arr[1]);
        }

        RowToString lambda1 = (Row r) -> {
            if (r.columns().contains(URL_REF) && r.columns().contains(PAGE_REF)) {
                final String url = r.get(URL_REF);
                final String page = r.get(PAGE_REF);

                return url + "," + page;
            } else {
                return null;
            }
        };
        FlameRDD mappedStrings = context.fromTable(CRAWL_TABLE, lambda1);

        HashMap<String, String> urlsMappedToHash = new HashMap<>();
        for (String url : mappedStrings.collect()) {
            String u = url.substring(0, url.indexOf(","));

            String uNorm = URLHelper.normalizeURL(u, u);
            u = Hasher.hash(u);
            uNorm = Hasher.hash(uNorm);

            urlsMappedToHash.put(uNorm, u);
        }

        StringToPair lambda2 = (String s) -> {
            String url = s.substring(0, s.indexOf(","));
            String page = s.substring(s.indexOf(",") + 1);

            if (page != null) {
                List<String> extractedUrls = URLHelper.extractUrls(page.getBytes());
                String L = "";
                HashSet<String> noRepeats = new HashSet<>();
                for (String extrUrl : extractedUrls) {
                    String normLink = URLHelper.normalizeURL(url, extrUrl);
                    noRepeats.add(normLink);
                }

                for (String link : noRepeats) {
                    if (!L.equals("")) {
                        L += ",";
                    }

                    L += Hasher.hash(link);
                }

                if (url != null) {
                    url = URLHelper.normalizeURL(url, url);
                    FlamePair pair = new FlamePair(Hasher.hash(url), "1.0,1.0," + L);
                    return pair;
                }
            }
            return null;
        };
        FlamePairRDD stateTable = mappedStrings.mapToPair(lambda2);

        // loop
        while (true) {
            PairToPairIterable lambda3 = (FlamePair f) -> {
                List<FlamePair> pairList = new ArrayList<>();
                String[] elems = f._2().split(",");

                double rC = Double.parseDouble(elems[0]);
                double rP = Double.parseDouble(elems[1]);

                double n = (double) elems.length - 2;

                for (int i = 2; i < elems.length; i++) {
                    String liHash = elems[i];
                    double v = 0.85 * (rC / n);
                    FlamePair p = new FlamePair(liHash, v + "");
                    FlamePair inDeg0 = new FlamePair(liHash, DEFAULT_VAL);

                    pairList.add(p);
                    pairList.add(inDeg0);
                }
                FlamePair fp = new FlamePair(f._1(), DEFAULT_VAL);
                pairList.add(fp);

                return pairList;
            };
            FlamePairRDD transferTable = stateTable.flatMapToPair(lambda3);

            TwoStringsToString lambda4 = (String acc, String newElem) -> {
                if (acc.equals("")) {
                    return newElem;
                }
                double vAcc = Double.parseDouble(acc);
                double vNew = Double.parseDouble(newElem);
                double vSum = vAcc + vNew;

                return vSum + "";
            };
            FlamePairRDD aggregated = transferTable.foldByKey("", lambda4);
            transferTable.destroy();

            FlamePairRDD joined = stateTable.join(aggregated);
            aggregated.destroy();

            PairToPairIterable lambda5 = (FlamePair f) -> {
                String[] elems = f._2().split(",");
                double rC = Double.parseDouble(elems[0]);
                double rP = rC;

                double newRC = Double.parseDouble(elems[elems.length - 1]) + 0.15;

                String L = "";
                for (int i = 2; i < elems.length - 1; i++) {
                    if (!L.equals("")) {
                        L += ",";
                    }
                    L += elems[i];
                }

                String new2 = newRC + "," + rP + "," + L;

                FlamePair fNew = new FlamePair(f._1(), newRC + "," + rP + "," + L);
                List<FlamePair> li = new ArrayList<>();
                li.add(fNew);
                return li;
            };
            FlamePairRDD updatedStateTable = joined.flatMapToPair(lambda5);
            joined.destroy();

            stateTable = updatedStateTable;
            updatedStateTable.destroy();

            PairToStringIterable lambda6 = (FlamePair f) -> {
                List<String> rChange = new ArrayList<>();
                String[] elems = f._2().split(",");

                double rC = Double.parseDouble(elems[0]);
                double rP = Double.parseDouble(elems[1]);
                rChange.add(Math.abs(rC - rP) + "");

                return rChange;
            };
            FlameRDD rankChanges = stateTable.flatMap(lambda6);

            TwoStringsToString lambda7 = (String max, String curr) -> {
                if (max.equals("")) {
                    return curr;
                }
                double maxVal = Double.parseDouble(max);
                double currVal = Double.parseDouble(curr);

                if (maxVal >= currVal) {
                    return max;
                } else {
                    return curr;
                }
            };
            String maxChange = rankChanges.fold("-1.0", lambda7);

            if (convergence == -1.0) {
                double change = Double.parseDouble(maxChange);
                if (change < threshold) {
                    break;
                }
            } else {
                // convergence criterion
                List<String> changes = rankChanges.collect();
                double count = rankChanges.count() * 0.01 * convergence;
                double num = 0.0;
                for (String c : changes) {
                    double ch = Double.parseDouble(c);
                    if (ch <= threshold) {
                        num += 1.0;
                    }
                }
                if (num >= count) {
                    break;
                }
            }
            rankChanges.destroy();
        }

        PairToPairIterable lambda8 = (FlamePair f) -> {
            KVSClient client = context.getKVS();
            List<FlamePair> li = new ArrayList<>();
            String[] elems = f._2().split(",");

            double rC = Double.parseDouble(elems[0]);
            Row r = new Row(urlsMappedToHash.get(f._1()));
            r.put(RANK_REF, rC + "");
            client.putRow(RANK_TABLE, r);

            return li;
        };
        FlamePairRDD finality = stateTable.flatMapToPair(lambda8);
        stateTable.destroy();
    }
}
