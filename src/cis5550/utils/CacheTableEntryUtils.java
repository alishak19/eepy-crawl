package cis5550.utils;

import cis5550.frontend.SearchResult;

import java.util.LinkedList;
import java.util.List;

public class CacheTableEntryUtils {
    private static final String CACHE_TABLE_SEPARATOR = "&&##&#&";

    public static String createEntry(List<SearchResult> aSearchResults) {
        StringBuilder myBuilder = new StringBuilder();
        for (SearchResult mySearchResult : aSearchResults) {
            myBuilder.append(mySearchResult.string());
            myBuilder.append(CACHE_TABLE_SEPARATOR);
        }
        return myBuilder.toString();
    }

    public static List<SearchResult> parseEntry(String aEntry) {
        String[] myParts = aEntry.split(CACHE_TABLE_SEPARATOR);
        List<SearchResult> myResults = new LinkedList<>();
        for (String myPart : myParts) {
            myResults.add(SearchResult.fromString(myPart));
        }
        return myResults;
    }
}
