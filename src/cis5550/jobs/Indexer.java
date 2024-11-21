package cis5550.jobs;
import java.io.*;

import cis5550.tools.PorterStemmer;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.*;
import cis5550.kvs.*;
import cis5550.flame.*;
import cis5550.flame.FlameContext.RowToString;
import cis5550.flame.FlamePairRDD.PairToPairIterable;
import cis5550.flame.FlamePairRDD.TwoStringsToString;
import cis5550.flame.FlameRDD.StringToPair;
import cis5550.tools.Hasher;
import cis5550.jobs.datamodels.TableColumns;
import cis5550.tools.Logger;

public class Indexer {
	private static final Logger LOGGER = Logger.getLogger(Indexer.class);
	private static final String CRAWL_TABLE = "pt-crawl";
	private static final String INDEX_TABLE = "pt-index";
	private static final String ALR_INDEXED = "pt-alrindexed";
	private static final String URL_REF = TableColumns.URL.value();
	private static final String PAGE_REF = TableColumns.PAGE.value();
	public static void run(FlameContext context, String[] arr) throws Exception {
		RowToString lambda1 = (Row r) -> {
			if (r.columns().contains(URL_REF) && r.columns().contains(PAGE_REF)) {
				KVSClient client = context.getKVS();
				try {
					// using hashed url as key
					if (client.existsRow(ALR_INDEXED, r.key())) {
						return null;
					} else {
						client.putRow(ALR_INDEXED, r);
						return r.get(URL_REF) + "," + r.get(PAGE_REF);
					}
				} catch (Exception e) {
					LOGGER.error("KVS error: putting/accessing alr-indexed table");
				}
				return null;

			} else {
				return null;
			}
		};
		FlameRDD mappedStrings = context.fromTable(CRAWL_TABLE, lambda1);
		
		StringToPair lambda2 = (String s) -> {
			int index = s.indexOf(",");
			FlamePair pair = new FlamePair(s.substring(0, index), s.substring(index + 1));
			
			return pair;
		};
		FlamePairRDD pairs = mappedStrings.mapToPair(lambda2);
		if (mappedStrings.count() > 0) {
			mappedStrings.destroy();
			System.out.println("success");
		}

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
				if (word.equals("\n")) {
					continue;
				}
				
				word = removePunctuation(word);
				if (word.equals(" ") || word.equals("")) {
					continue;
				}

				word = word.toLowerCase();
				if (word.contains("/")) {
					List<String> slashSplit = Arrays.asList(word.split("/"));
					words.addAll(slashSplit);
					for (String w : slashSplit) {
						if (wordPositions.containsKey(w)) {
							wordPositions.put(w, wordPositions.get(w) + " " + index);
						} else {
							wordPositions.put(w, index + "");
						}
					}
				} else if (word.contains(" ")) {
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
				KVSClient client = context.getKVS();
				if (client.existsRow(INDEX_TABLE, w)) {
					Row curr = client.getRow(INDEX_TABLE, w);
					for (String col : curr.columns()) {
						String val = curr.get(col);
						val += "," + f._1() + ":" + wordPositions.get(w);
						client.put(INDEX_TABLE, w, col, val);
					}
				} else {
					Row r = new Row(w);
					r.put(URL_REF, f._1() + ":" + wordPositions.get(w));
					client.putRow(INDEX_TABLE, r);
				}
			}
			return null;
		};
		FlamePairRDD inverted = pairs.flatMapToPair(lambda3);
	}
	
	public static String removePunctuation(String s) {
		String punctuation = ".,:;!?\'\"()-";
		String ans = "";
		
		for (char c : s.toCharArray()) {
			if (!punctuation.contains(c + "") && c != '\n' && c != '\r' && c != '\t') {
				ans += c + "";
			} else {
				ans += " ";
			}
		}
		
		return ans;
	}
}
