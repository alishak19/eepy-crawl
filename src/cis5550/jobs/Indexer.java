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
import cis5550.flame.FlamePairRDD.TwoStringsToString;
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
	private static final String ALR_INDEXED = "pt-alrindexed";
	private static final String URL_REF = TableColumns.URL.value();
	private static final String PAGE_REF = TableColumns.PAGE.value();
	private static final String PUNCTUATION = ".,:;!?\'\"()-=/+{}[]_#$&\\";
	private static final String SPACE = " ";
	public static void run(FlameContext context, String[] arr) throws Exception {
		RowToPair lambda1 = (Row myRow) -> {
			if (myRow.columns().contains(URL_REF) && myRow.columns().contains(PAGE_REF)) {
				KVSClient kvsClient = context.getKVS();
				try {
					// using hashed url as key
					if (kvsClient.existsRow(ALR_INDEXED, myRow.key())) {
						return null;
					} else {
						kvsClient.putRow(ALR_INDEXED, myRow);
						if (myRow.get(URL_REF) != null && myRow.get(PAGE_REF) != null) {
							return new FlamePair(URLDecoder.decode(myRow.get(URL_REF), StandardCharsets.UTF_8), myRow.get(PAGE_REF));
						} else {
							return null;
						}
					}
				} catch (Exception e) {
					LOGGER.error("KVS error: putting/accessing alr-indexed table");
				}
				return null;

			} else {
				return null;
			}
		};
		FlamePairRDD myPairs = context.pairFromTable(CRAWL_TABLE, lambda1);
		KVSClient tempClient = context.getKVS();
		tempClient.delete(ALR_INDEXED);
		
		PairToPairIterable lambda3 = (FlamePair f) -> {
			String removedTags = "";
			boolean tag = false;

			removedTags = f._2().replaceAll("<[^>]*>", " ");
			removedTags = removedTags.toLowerCase().replaceAll("[^a-z0-9\\s]", " ");

			String[] wordsList = removedTags.split(SPACE);

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

			KVSClient kvsClient = context.getKVS();
			String url = URLDecoder.decode(f._1(), StandardCharsets.UTF_8);
			System.out.println(f._1());
			for (String w : words) {
				try {
					String val = url + ":" + wordPositions.get(w);
					if (w != null && !w.equals("") && !w.equals(" ") && !val.equals("")) {
						if (w.charAt(w.length() - 1) == ' ') {
							w = w.substring(0, w.length() - 1);
						}
						if (w.charAt(0) == ' ') {
							w = w.substring(1);
						}
						if (w.length() <= 25) {
							kvsClient.appendToRow(INDEX_TABLE, w, URL_REF, val, ",");
						}
					}
				} catch (Exception e) {
					e.printStackTrace();
					LOGGER.error("Error:" + w);
				}

			}
			return null;
		};
		FlamePairRDD inverted = myPairs.flatMapToPair(lambda3);
		myPairs.destroy();
	}
}
