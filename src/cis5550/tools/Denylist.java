package cis5550.tools;

import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;

import java.util.*;
import java.util.regex.Pattern;

public class Denylist {
    private static final String COLUMN_NAME = "pattern";
    private static final Map<String, Double> PROBABILISTIC_FILTERING = Map.of(
            "en.wikipedia.org", 0.5
    );

    private final List<Pattern> thePatterns;

    private Denylist(List<Pattern> aPatterns) {
        thePatterns = aPatterns;
    }

    public Denylist() {
        thePatterns = List.of(
                Pattern.compile("http.*://.*/cgi-bin/.*", Pattern.CASE_INSENSITIVE), // Dynamic scripts
                Pattern.compile(".*\\.pdf$", Pattern.CASE_INSENSITIVE),              // PDF files
                Pattern.compile(".*\\.jpg$", Pattern.CASE_INSENSITIVE),              // Image files
                Pattern.compile(".*\\.png$", Pattern.CASE_INSENSITIVE),              // Image files
                Pattern.compile(".*\\.gif$", Pattern.CASE_INSENSITIVE),              // Image files
                Pattern.compile(".*\\.css$", Pattern.CASE_INSENSITIVE),              // Stylesheets
                Pattern.compile(".*\\.js$", Pattern.CASE_INSENSITIVE),               // JS
                Pattern.compile("https?://(?!en\\.)[a-z0-9-]+\\.wikipedia\\.org(:\\d+)?/.*",
                        Pattern.CASE_INSENSITIVE)                                          // Non English Wikipedia
        );
    }

    public static Denylist fromKVSTable(KVSClient aKVSClient, String aTableName) {
        if (aTableName == null) {
            return new Denylist(List.of());
        }
        Iterator<Row> myBlockedPatternRows;
        try {
            myBlockedPatternRows = aKVSClient.scan(aTableName);
        } catch (Exception e) {
            return new Denylist(List.of());
        }
        List<Pattern> myPatterns = new LinkedList<>();
        while (myBlockedPatternRows.hasNext()) {
            Row myRow = myBlockedPatternRows.next();
            if (myRow.get(COLUMN_NAME) != null) {
                Pattern myPattern =
                        Pattern.compile(myRow.get(COLUMN_NAME).replace("*", ".*"), Pattern.CASE_INSENSITIVE);
                myPatterns.add(myPattern);
            }
        }
        return new Denylist(myPatterns);
    }

    public boolean isBlocked(String aUrl) {
        for (Pattern myPattern : thePatterns) {
            if (myPattern.matcher(aUrl).matches()) {
                return true;
            }
        }
        return false;
    }

    public static boolean probabilisticDomainFilter(String aUrl) {
        Random random = new Random();
        for (String key : PROBABILISTIC_FILTERING.keySet()) {
            if (aUrl.contains(key) && random.nextDouble() < PROBABILISTIC_FILTERING.get(key)) {
                return false;
            }
        }
        return true;
    }
}
