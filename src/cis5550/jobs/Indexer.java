package cis5550.jobs;
import java.io.*;

import cis5550.tools.PorterStemmer;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.*;
import cis5550.kvs.*;
import cis5550.flame.*;
import cis5550.flame.FlameContext.RowToString;
import cis5550.flame.FlameContext.RowToPair;
import cis5550.flame.FlamePairRDD.PairToPairIterable;
import cis5550.flame.FlamePairRDD.PairToStringIterable;
import cis5550.flame.FlamePairRDD.TwoStringsToString;
import cis5550.flame.FlameRDD.StringToPairIterable;
import cis5550.flame.FlameRDD.StringToPair;
import cis5550.tools.Hasher;
import cis5550.jobs.datamodels.TableColumns;
import cis5550.tools.Logger;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;

public class Indexer {
	private static final Logger LOGGER = Logger.getLogger(Indexer.class);
	private static final String CRAWL_TABLE = "pt-crawl";
	private static final String INDEX_TABLE = "pt-index";
	private static final String TO_INDEX = "pt-toindex";
	private static final String URL_REF = TableColumns.URL.value();
	private static final String PAGE_REF = TableColumns.PAGE.value();
	private static final String INDEXED_TABLE = "pt-indexed";
	private static final String SPACE = " ";
	public static final String UNIQUE_SEPARATOR = "&#!#&";
	public static void run(FlameContext context, String[] arr) throws Exception {
		try {
			RowToPair lambda1 = (Row myRow) -> {
				try {
					if (myRow.get(URL_REF) != null && myRow.get(PAGE_REF) != null) {
						return new FlamePair(myRow.get(URL_REF), myRow.get(PAGE_REF));
					} else {
						return null;
					}
				} catch (Exception e) {
					LOGGER.error("KVS error: loading crawl table");
				}
				return null;
			};

			FlamePairRDD myPairs = context.pairFromTable(CRAWL_TABLE, lambda1);

			PairToPairIterable lambda3 = (FlamePair f) -> {
				String removedTags = f._2().replaceAll("<[^>]*>", " ");
				removedTags = removedTags.toLowerCase().replaceAll("[^a-z0-9\\s]", " ");

				String[] wordsList = removedTags.split("\\s");

				HashSet<String> words = new HashSet<>();
				HashMap<String, String> wordPositions = new HashMap<>();
				int index = 0;

				for (String word : wordsList) {
					if (word == null || word.equals(SPACE) || word.equals("")) {
						continue;
					}
					index++;
					words.add(word);
					if (wordPositions.containsKey(word)) {
						wordPositions.put(word, wordPositions.get(word) + SPACE + index);
					} else {
						wordPositions.put(word, index + "");
					}
				}

				String url = URLDecoder.decode(f._1(), StandardCharsets.UTF_8);
				System.out.println(url);
				List<FlamePair> wordPairs = new ArrayList<>();

				for (String w : words) {
					w.replaceAll("\\s+", "");
					if (w.length() > 25) {
						w = w.substring(0, 25);
					}
					if (w.length() > 0) {
						FlamePair curr = new FlamePair(w, url + ":" + wordPositions.get(w));
						wordPairs.add(curr);
					}

				}

				return wordPairs;
			};
			FlamePairRDD inverted = myPairs.flatMapToPair(lambda3);
			myPairs.destroy();

			TwoStringsToString lambda4 = (String urlOne, String urlTwo) -> {
				if (urlOne.equals("")) {
					return urlTwo;
				}
				return urlOne + "," + urlTwo;
			};
			FlamePairRDD invertedList = inverted.foldByKey("", lambda4);
			inverted.destroy();

			try {
				invertedList.saveAsTable(INDEX_TABLE);
			} catch (Exception e) {
				LOGGER.error("error saving table, check values");
			}
		} catch (Exception e) {
			LOGGER.error("somewhere");
		}

		// invertedList.destroy();
	}
}