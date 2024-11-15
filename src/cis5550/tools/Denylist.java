package cis5550.tools;

import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Pattern;

public class Denylist {
    public static final String COLUMN_NAME = "pattern";

    private final List<Pattern> thePatterns;

    private Denylist(List<Pattern> aPatterns) {
        thePatterns = aPatterns;
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

    public static void main(String[] args) {
        List<String> patterns = List.of("http*://*/cgi-bin/*", "*.pdf", "http://dangerous.com:80/*");

        Denylist denylist = new Denylist(patterns.stream()
                .map(x -> x.replace("*", ".*"))
                .map(Pattern::compile)
                .toList());

        System.out.println(denylist.isBlocked("http://example.com/cgi-bin/test")); // true
        System.out.println(denylist.isBlocked("http://safe.com/document.pdf")); // true
        System.out.println(denylist.isBlocked("http://dangerous.com:80/home")); // true
        System.out.println(denylist.isBlocked("http://safe.com/page")); // false
    }
}
