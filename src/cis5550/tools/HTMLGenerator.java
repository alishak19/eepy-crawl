package cis5550.tools;

import cis5550.kvs.Row;

import java.util.*;

public class HTMLGenerator {
    public static String generateWorkerEntries(Map<String, Integer> aTableSizes) {
        if (aTableSizes == null || aTableSizes.isEmpty()) {
            return """
                        <html>
                            <style>
                                body {
                                    font-family: Arial, sans-serif;
                                }
                                h1 {
                                    text-align: center;
                                    color: #333;
                                }
                            </style>
                            <h1>No tables available</h1>
                        </html>
                    """;
        }

        StringBuilder myBuilder = new StringBuilder();

        myBuilder.append("""
                    <html>
                        <style>
                            table {
                                border-collapse: collapse;
                                width: 50%;
                                margin: 20px auto;
                                font-size: 16px;
                                text-align: left;
                                border: 1px solid #ddd;
                            }
                            th, td {
                                padding: 10px;
                                border: 1px solid #ddd;
                            }
                            th {
                                background-color: #f4f4f4;
                            }
                            tr:hover {
                                background-color: #f9f9f9;
                            }
                            a {
                                text-decoration: none;
                                color: #007BFF;
                            }
                            a:hover {
                                text-decoration: underline;
                            }
                        </style>
                        <body>
                            <table>
                                <tr>
                                    <th>Table</th>
                                    <th>Size</th>
                                </tr>
                """);

        for (Map.Entry<String, Integer> myEntry : aTableSizes.entrySet()) {
            myBuilder.append("<tr>")
                    .append("<td><a href=\"view/")
                    .append(myEntry.getKey())
                    .append("\">")
                    .append(myEntry.getKey())
                    .append("</a></td>")
                    .append("<td>")
                    .append(myEntry.getValue())
                    .append("</td>")
                    .append("</tr>");
        }

        myBuilder.append("""
                            </table>
                        </body>
                    </html>
                """);

        return myBuilder.toString();
    }

    public static String generateTableEntries(String aTableName, Map<String, Row> aTable, int aNumRows) {
        if (aTable == null || aTable.isEmpty()) {
            return """
                        <html>
                            <style>
                                body {
                                    font-family: Arial, sans-serif;
                                }
                                h1 {
                                    text-align: center;
                                    color: #333;
                                }
                            </style>
                            <h1>Table not found</h1>
                        </html>
                    """;
        }

        Set<String> myColumns = new HashSet<>();
        for (Row myRow : aTable.values()) {
            myColumns.addAll(myRow.columns());
        }

        StringBuilder myBuilder = new StringBuilder();

        myBuilder.append("""
                        <html>
                            <style>
                                table {
                                    border-collapse: collapse;
                                    width: 100%;
                                    margin: 20px 0;
                                    font-size: 16px;
                                    text-align: left;
                                }
                                th, td {
                                    border: 1px solid #ddd;
                                    padding: 8px;
                                }
                                th {
                                    background-color: #f4f4f4;
                                    font-weight: bold;
                                }
                                tr:hover {
                                    background-color: #f9f9f9;
                                }
                                a {
                                    text-decoration: none;
                                    color: #007BFF;
                                }
                                a:hover {
                                    text-decoration: underline;
                                }
                            </style>
                            <body>
                                <h1 style="text-align: center;">""")
                .append(aTableName)
                .append("</h1>")
                .append("<table>")
                .append("<tr><th>Key</th>");

        for (String myColumn : myColumns) {
            myBuilder.append("<th>").append(escapeHtml(myColumn)).append("</th>");
        }
        myBuilder.append("</tr>");

        int myRowCounter = 0;
        for (Row myRow : aTable.values()) {
            if (myRowCounter >= aNumRows) {
                myBuilder.append("""
                                <tr>
                                    <td colspan=""").append(myColumns.size() + 1).append("""
                                " style="text-align: center;">
                                    <a href="/view/""")
                        .append(aTableName)
                        .append("?fromRow=")
                        .append(myRow.key())
                        .append("\">Next</a></td></tr>");
                break;
            }
            myBuilder.append("<tr><td>").append(escapeHtml(myRow.key())).append("</td>");
            for (String myColumn : myColumns) {
                String cellValue = myRow.get(myColumn) != null ? myRow.get(myColumn) : "";
                myBuilder.append("<td>").append(escapeHtml(cellValue)).append("</td>");
            }
            myBuilder.append("</tr>");
            myRowCounter++;
        }

        myBuilder.append("</table></body></html>");
        return myBuilder.toString();
    }

    private static String escapeHtml(String aInput) {
        if (aInput == null) return "";
        return aInput.replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;")
                .replace("\"", "&quot;")
                .replace("'", "&#39;");
    }
}
