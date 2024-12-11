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
	private static final String INDEX_TABLE = "at-index";
	private static final String TO_INDEX = "pt-toindex";
	private static final String URL_REF = TableColumns.URL.value();
	private static final String PAGE_REF = TableColumns.PAGE.value();
	private static final String VAL_REF = TableColumns.VALUE.value();
	private static final String INDEXED_TABLE = "pt-indexed";
	private static final String SPACE = " ";
	public static final String UNIQUE_SEPARATOR = "&#!#&";
	private static final Set<String> STOPWORDS1 = Set.of(
			"a", "an", "the", "and", "or", "but", "is", "are", "was", "were",
			"be", "has", "he", "it", "its", "that", "will", "i", "me", "my",
			"myself", "we", "our", "ours", "ourselves", "you", "your",
			"yours", "yourself", "yourselves", "him", "his", "himself",
			"she", "her", "hers", "herself", "they", "them",
			"their", "theirs", "themselves", "what", "which", "who", "whom", "this",
			"these", "those", "am", "been", "being", "have", "had", "having",
			"do", "does", "did", "doing", "about", "against", "between", "into", "through",
			"during", "before", "after", "above", "below", "to", "from", "up", "down", "in",
			"out", "on", "off", "over", "under", "again", "further", "then", "once", "here",
			"there", "when", "where", "why", "how", "all", "any", "both", "each", "few", "more",
			"most", "other", "some", "such", "no", "nor", "not", "only", "own", "same",
			"so", "than", "too", "very", "s", "t", "can", "just", "don", "should", "now"
	);

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
				HashMap<String, Integer> wordCount = new HashMap<>();
				int index = 0;

				for (String word : wordsList) {
					if (word == null || word.equals(SPACE) || word.equals("") || STOPWORDS1.contains(word)) {
						continue;
					}
					index++;
					words.add(word);
					if (wordCount.containsKey(word)) {
						wordCount.put(word, wordCount.get(word) + 1);
					} else {
						wordCount.put(word, 1);
					}
				}

				String url = URLDecoder.decode(f._1(), StandardCharsets.UTF_8);
				System.out.println(url);
				List<FlamePair> wordPairs = new ArrayList<>();

				for (String w : words) {
					w.replaceAll("\\s+", "");
					if (w.length() > 20) {
						continue;
					}
					if (w.length() > 0) {
						PorterStemmer p = new PorterStemmer();
						for (char c : w.toCharArray()) {
							p.add(c);
						}
						p.stem();
						String stemmed = p.toString();
						if (!stemmed.equals(w)) {
							FlamePair currS = new FlamePair(stemmed, f._1() + ":" + wordCount.get(w));
							wordPairs.add(currS);
						}
						FlamePair curr = new FlamePair(w, url + ":" + wordCount.get(w));
						wordPairs.add(curr);
					}

				}

				return wordPairs;
			};
			myPairs.flatMapToPairTable(lambda3, INDEX_TABLE, VAL_REF);
		} catch (Exception e) {
			LOGGER.error("somewhere");
		}

		// invertedList.destroy();
	}
}