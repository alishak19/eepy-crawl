package cis5550.jobs;
import java.io.*;

import cis5550.tools.PorterStemmer;
// import static cis5550.external.PorterStemmer;
import cis5550.tools.PorterStemmer.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.*;
import cis5550.kvs.*;
import cis5550.flame.*;
import cis5550.flame.FlameContext.RowToString;
import cis5550.flame.FlamePairRDD.PairToPairIterable;
import cis5550.flame.FlamePairRDD.TwoStringsToString;
import cis5550.flame.FlameRDD.StringToPair;

public class Indexer {
	public static void run(FlameContext context, String[] arr) throws Exception {
		KVSClient client = context.getKVS();
		RowToString lambda1 = (Row r) -> {
			if (r.columns().contains("url") && r.columns().contains("page")) {
				String url = r.get("url");
				String page = r.get("page");
				
				return url + "," + page;
			} else {
				return null;
			}
		};
		FlameRDD mappedStrings = context.fromTable("pt-crawl", lambda1);
		
		StringToPair lambda2 = (String s) -> {
			int index = s.indexOf(",");
			FlamePair pair = new FlamePair(s.substring(0, index), s.substring(index + 1));
			
			return pair;
		};
		FlamePairRDD pairs = mappedStrings.mapToPair(lambda2);
		try {
			mappedStrings.destroy();
		} catch (Exception e) {

		}

		PairToPairIterable lambda3 = (FlamePair f) -> {
			List<FlamePair> wordPairs = new ArrayList<>();
			String page = f._2();
			
			String removedTags = "";
			boolean tag = false;
			
			for (int i = 0; i < page.length(); i++) {
				if (page.charAt(i) == '<') {
					tag = true;
				} else if (page.charAt(i) == '>') {
					tag = false;
					removedTags += " ";
				} else if (!tag) {
					removedTags += page.charAt(i);
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
				String positions = wordPositions.get(w);
				PorterStemmer p = new PorterStemmer();
				for (char c : w.toCharArray()) {
					p.add(c);
				}
				p.stem();
				String stemmed = p.toString();
				if (!stemmed.equals(w)) {
					FlamePair currS = new FlamePair(stemmed, f._1() + ":" + wordPositions.get(w));
					wordPairs.add(currS);
				}
				FlamePair curr = new FlamePair(w, f._1() + ":" + wordPositions.get(w));
				wordPairs.add(curr);
			}
			return wordPairs;
		};
		FlamePairRDD inverted = pairs.flatMapToPair(lambda3);
		try {
			pairs.destroy();
		} catch (Exception e) {

		}
		
		TwoStringsToString lambda4 = (String one, String two) -> {
			if (one.equals("")) {
				return two;
			}
			return one + "," + two;
		};
		FlamePairRDD invertedList = inverted.foldByKey("", lambda4);
		try {
			inverted.destroy();
		} catch (Exception e) {

		}
		invertedList.saveAsTable("pt-index");
		
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
