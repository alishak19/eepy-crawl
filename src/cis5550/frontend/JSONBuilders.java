package cis5550.frontend;

import java.util.List;

public class JSONBuilders {
    public static String buildSearchResults(List<SearchResult> aSearchResults) {
        StringBuilder myBuilder = new StringBuilder();
        myBuilder.append("[");
        for (SearchResult myResult : aSearchResults) {
            myBuilder.append("{");
            myBuilder.append("\"title\": \"").append(myResult.title()).append("\",");
            myBuilder.append("\"url\": \"").append(myResult.url()).append("\",");
            myBuilder.append("\"snippet\": \"").append(myResult.snippet()).append("\"");
            myBuilder.append("},");
        }
        myBuilder.deleteCharAt(myBuilder.length() - 1);
        myBuilder.append("]");
        return myBuilder.toString();
    }
}
