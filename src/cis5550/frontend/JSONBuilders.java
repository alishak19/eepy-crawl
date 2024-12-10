package cis5550.frontend;

import java.util.List;

public class JSONBuilders {
    public static String buildSearchResults(List<SearchResult> aSearchResults) {
        if (aSearchResults == null || aSearchResults.isEmpty()) {
            return "[]";
        }
        StringBuilder myBuilder = new StringBuilder();
        myBuilder.append("[");
        for (SearchResult myResult : aSearchResults) {
            if (isValidJsonString(myResult.title()) && isValidJsonString(myResult.url()) &&
                    isValidJsonString(myResult.snippet())) {
                myBuilder.append("{");
                myBuilder.append("\"title\": \"").append(escapeJsonString(myResult.title())).append("\",");
                myBuilder.append("\"url\": \"").append(escapeJsonString(myResult.url())).append("\",");
                myBuilder.append("\"snippet\": \"").append(escapeJsonString(myResult.snippet())).append("\"");
                myBuilder.append("},");
            }
        }
        myBuilder.deleteCharAt(myBuilder.length() - 1);
        myBuilder.append("]");
        return myBuilder.toString();
    }

    private static boolean isValidJsonString(String input) {
        if (input == null) return false;
        for (int i = 0; i < input.length(); i++) {
            char c = input.charAt(i);
            if (c == '"' || c == '\\') {
                return false;
            }
        }
        return true;
    }

    private static String escapeJsonString(String input) {
        if (input == null) return "";
        StringBuilder escaped = new StringBuilder();
        for (int i = 0; i < input.length(); i++) {
            char c = input.charAt(i);
            if (c == '"') {
                escaped.append("\\\"");
            } else if (c == '\\') {
                escaped.append("\\\\");
            } else {
                escaped.append(c);
            }
        }
        return escaped.toString();
    }
}
