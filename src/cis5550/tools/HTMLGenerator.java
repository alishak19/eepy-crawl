package cis5550.tools;

import cis5550.kvs.Row;

import java.util.*;

public class HTMLGenerator {
    public static String generateWorkerEntries(Map<String, Integer> aTableSizes) {
        if (aTableSizes == null) {
            return "<html><h1>No tables</h1></html>";
        }
        StringBuilder myBuilder = new StringBuilder();
        myBuilder.append("<html><table><tr><th>Table</th><th>Size</th></tr>");
        for (Map.Entry<String, Integer> myEntry : aTableSizes.entrySet()) {
            myBuilder
                    .append("<tr><td><a href=\"view/")
                    .append(myEntry.getKey())
                    .append("\">")
                    .append(myEntry.getKey())
                    .append("</td><td>")
                    .append(myEntry.getValue())
                    .append("</td></tr>");
        }
        myBuilder.append("</table></html>");
        return myBuilder.toString();
    }

    public static String generateTableEntries(String aTableName, Map<String, Row> aTable, int aNumRows) {
        if (aTable == null) {
            return "<html><h1>Table not found</h1></html>";
        }
        Set<String> myColumns = new HashSet<>();
        for (Row myRow : aTable.values()) {
            myColumns.addAll(myRow.columns());
        }

        StringBuilder myBuilder = new StringBuilder();
        myBuilder.append("<html><h1>");
        myBuilder.append(aTableName);
        myBuilder.append("</h1>");
        myBuilder.append("<table><tr><th>Key</th>");
        for (String myColumn : myColumns) {
            myBuilder.append("<th>").append(myColumn).append("</th>");
        }
        myBuilder.append("</tr>");
        int myRowCounter = 0;
        for (Row myRow : aTable.values()) {
            if (myRowCounter >= aNumRows) {
                myBuilder
                        .append("<tr><a href=\"/view/")
                        .append(aTableName)
                        .append("?fromRow=")
                        .append(myRow.key())
                        .append("\">Next<a></tr>");
                break;
            }
            myBuilder.append("<tr><td>").append(myRow.key()).append("</td>");
            for (String myColumn : myColumns) {
                myBuilder
                        .append("<td>")
                        .append(myRow.get(myColumn) != null ? myRow.get(myColumn) : "")
                        .append("</td>");
            }
            myBuilder.append("</tr>");
            myRowCounter++;
        }
        myBuilder.append("</table></html>");
        return myBuilder.toString();
    }
}
