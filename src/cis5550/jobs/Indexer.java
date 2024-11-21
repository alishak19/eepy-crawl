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

public class Indexer {
	private static final Logger LOGGER = Logger.getLogger(Indexer.class);
	private static final String CRAWL_TABLE = "pt-crawl";
	private static final String INDEX_TABLE = "pt-index";
	private static final String ALR_INDEXED = "pt-alrindexed";
	private static final String URL_REF = TableColumns.URL.value();
	private static final String PAGE_REF = TableColumns.PAGE.value();
	private static final String PUNCTUATION = ".,:;!?\'\"()-=/+{}[]_#$&";
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
							return new FlamePair(URLDecoder.decode(myRow.get(URL_REF)), myRow.get(PAGE_REF));
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
			
			for (int i = 0; i < f._2().length(); i++) {
				if (f._2().charAt(i) == '<') {
					tag = true;
				} else if (f._2().charAt(i) == '>') {
					tag = false;
					removedTags += " ";
				} else if (!tag) {
					removedTags += f._2().charAt(i);
				}
			}
			String[] wordsList = removedTags.split(" ");
			HashSet<String> words = new HashSet<>();
			HashMap<String, String> wordPositions = new HashMap<>();
			int index = 0;
			
			for (String word : wordsList) {
				index++;
				if (word == null || word.equals("\n")) {
					continue;
				}
				
				word = removePunctuation(word);
				if (word.equals(" ") || word.equals("")) {
					continue;
				}

				word = word.toLowerCase();
				if (word.contains(" ")) {
					List<String> spaceSplit = Arrays.asList(word.split(" "));
					for (String wordX : spaceSplit) {
						if (!wordX.equals("") && !wordX.equals(" ")) {
							words.add(wordX);
							if (wordPositions.containsKey(wordX)) {
								wordPositions.put(wordX, wordPositions.get(wordX) + " " + index);
							} else {
								wordPositions.put(wordX, index + "");
							}
						}
					}
					
				} else {
					words.add(word);
					if (wordPositions.containsKey(word)) {
						wordPositions.put(word, wordPositions.get(word) + " " + index);
					} else {
						wordPositions.put(word, index + "");
					}
				}
			}
			
			for (String w : words) {
				KVSClient kvsClient = context.getKVS();
				try {
					String val = f._1() + ":" + wordPositions.get(w);
					kvsClient.appendToRow(INDEX_TABLE, w, URL_REF, val, ",");
				} catch (Exception e) {
					LOGGER.error("Error: issue with input: " + w);
				}

			}
			return null;
		};
		FlamePairRDD inverted = myPairs.flatMapToPair(lambda3);
		myPairs.destroy();
	}
	
	private static String removePunctuation(String s) {
		String ans = "";
		
		for (char c : s.toCharArray()) {
			if (!PUNCTUATION.contains(c + "") && c != '\n' && c != '\r' && c != '\t') {
				ans += c + "";
			} else {
				ans += " ";
			}
		}
		
		return ans;
	}
}
