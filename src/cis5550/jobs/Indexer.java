package cis5550.jobs;

import cis5550.jobs.datamodels.TableColumns;
import cis5550.tools.PorterStemmer;
import cis5550.flame.FlameContext;
import cis5550.flame.FlamePair;
import cis5550.flame.FlamePairRDD;
import cis5550.flame.FlameRDD;
import cis5550.tools.Logger;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class Indexer {

    private static final Logger LOGGER = Logger.getLogger(Indexer.class);

    private static final String CRAWL_TABLE = "pt-crawl";
    private static final String INDEX_TABLE = "pt-index";

    private static final String COLON = ":";
    private static final String SPACE = " ";
    private static final String COMMA = ",";
    private static final String EMPTY = "";

    public static void run(FlameContext context, String[] args) throws Exception {

        LOGGER.debug("Starting Indexer run job...");

        FlameRDD urlPageStrings = context.fromTable(CRAWL_TABLE, row -> {
            String url = row.get(TableColumns.URL.value());
            String pageContent = row.get(TableColumns.PAGE.value());
            return url + COMMA + pageContent;
        });

        FlamePairRDD urlPagePairs = urlPageStrings.mapToPair(record -> {
            String[] parts = record.split(COMMA, 2);
            String url = parts[0];
            String pageContent = parts[1];
            return new FlamePair(url, pageContent);
        });

        Pattern htmlTagPattern = Pattern.compile("<[^>]*>");

        FlamePairRDD wordUrlPairs = urlPagePairs.flatMapToPair(pair -> {
            String url = URLDecoder.decode(pair._1(), StandardCharsets.UTF_8);
            String content = pair._2();

            content = htmlTagPattern.matcher(content).replaceAll(SPACE);
            content = content.replaceAll("[\\p{Punct}\r\n\t]", SPACE).toLowerCase();
            List<String> words = Arrays.asList(content.split("\\s+"));

            Map<String, List<Integer>> wordPositions = new HashMap<>();
            for (int i = 0; i < words.size(); i++) {
                String word = words.get(i);
                if (!word.isEmpty()) {
                    wordPositions.computeIfAbsent(word, k -> new ArrayList<>()).add(i + 1); // 1-indexed
                }
            }

            List<FlamePair> pairs = new ArrayList<>();
            for (Map.Entry<String, List<Integer>> entry : wordPositions.entrySet()) {
                String word = entry.getKey();
                String positions = entry.getValue().stream().sorted().map(String::valueOf).collect(Collectors.joining(SPACE));

                PorterStemmer stemmer = new PorterStemmer();
                stemmer.add(word.toCharArray(), word.length());
                stemmer.stem();
                String stemmedWord = stemmer.toString();

                if (!word.equals(stemmedWord)) {
                    LOGGER.debug("Adding a stem of " + word + ": " + stemmedWord);
                    pairs.add(new FlamePair(stemmedWord, url + COLON + positions));
                    pairs.add(new FlamePair(word, url + COLON + positions));
                } else {
                    pairs.add(new FlamePair(word, url + COLON + positions));
                }
            }

            return pairs;
        });

        FlamePairRDD invertedIndex = wordUrlPairs.foldByKey(
                EMPTY,
                (existingUrls, newUrl) -> {
                    if (existingUrls.isEmpty()) {
                        return newUrl;
                    } else {
                        List<String> combinedUrls = new ArrayList<>(Arrays.asList(existingUrls.split(COMMA)));
                        combinedUrls.add(newUrl);
                        combinedUrls.sort((url1, url2) -> {
                            String[] parts1 = url1.split(":(?=[^:]+$)");
                            String[] parts2 = url2.split(":(?=[^:]+$)");

                            String positions1 = parts1.length > 1 ? parts1[1] : EMPTY;
                            String positions2 = parts2.length > 1 ? parts2[1] : EMPTY;

                            int count1 = positions1.split(SPACE).length;
                            int count2 = positions2.split(SPACE).length;

                            return Integer.compare(count2, count1);
                        });
                        return String.join(COMMA, combinedUrls);
                    }
                }
        );

        invertedIndex.saveAsTable(INDEX_TABLE);
        LOGGER.debug("Indexer run job complete!");
    }
}
