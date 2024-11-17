package cis5550.tools;

import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RobotsHelper {

    private static final Logger LOGGER = Logger.getLogger(RobotsHelper.class);
    private static final String USER_AGENT = "cis5550-crawler";

    public static String getRobotsTxt(KVSClient client, String host) throws IOException {
        String robotsKey = Hasher.hash(host + "/robots.txt");
        Row robotsRow = client.getRow("hosts", robotsKey);

        if (robotsRow != null) {
            return robotsRow.get("robotsTxt");
        }

        try {
            URI robotsUri = new URI("http", host, "/robots.txt", null);
            URL robotsUrl = robotsUri.toURL();

            HttpURLConnection robotsConnection = (HttpURLConnection) robotsUrl.openConnection();
            robotsConnection.setRequestMethod("GET");
            robotsConnection.setRequestProperty("User-Agent", USER_AGENT);
            robotsConnection.connect();

            if (robotsConnection.getResponseCode() == 200) {
                InputStream is = robotsConnection.getInputStream();
                ByteArrayOutputStream byteBuffer = new ByteArrayOutputStream();
                byte[] buffer = new byte[4096];
                int bytesRead;
                while ((bytesRead = is.read(buffer)) != -1) {
                    byteBuffer.write(buffer, 0, bytesRead);
                }
                byte[] contentBytes = byteBuffer.toByteArray();
                is.close();

                Row robotsRowToSave = new Row(robotsKey);
                robotsRowToSave.put("robotsTxt", contentBytes);
                client.putRow("hosts", robotsRowToSave);

                return new String(contentBytes, StandardCharsets.UTF_8);
            }
        } catch (Exception e) {
            Logger.getLogger(RobotsHelper.class).error("Error fetching robots.txt from " + host + ": " + e.getMessage());
        }
        return null;
    }

    public static boolean isUrlAllowed(String robotsTxt, String url) {
        if (robotsTxt == null || robotsTxt.isEmpty()) {
            return true;
        }

        String urlPath;
        try {
            URI uri = new URI(url);
            urlPath = uri.getPath();
        } catch (URISyntaxException e) {
            LOGGER.error(e.getMessage());
            return false;
        }

        Map<String, List<String>> agentRules = new HashMap<>();
        agentRules.put("cis5550-crawler", new ArrayList<>());
        agentRules.put("*", new ArrayList<>());

        String currentUserAgent = null;
        String[] rules = robotsTxt.split("\n");

        for (String line : rules) {
            line = line.trim();
            if (line.startsWith("User-agent:")) {
                currentUserAgent = line.substring(11).trim();
            } else if (line.startsWith("Disallow:") || line.startsWith("Allow:")) {
                if (currentUserAgent != null && (currentUserAgent.equals("cis5550-crawler") || currentUserAgent.equals("*"))) {
                    String ruleType = line.startsWith("Disallow:") ? "D" : "A";
                    String rulePath = line.substring(line.indexOf(":") + 1).trim();
                    if (!rulePath.isEmpty()) {
                        agentRules.get(currentUserAgent).add(ruleType + " " + rulePath);
                    }
                }
            }
        }

        List<String> applicableRules = agentRules.get("cis5550-crawler").isEmpty()
                ? agentRules.get("*") : agentRules.get("cis5550-crawler");

        for (String rule : applicableRules) {
            String ruleType = rule.substring(0, 1);
            String rulePath = rule.substring(2);

            if (urlPath.startsWith(rulePath)) {
                return ruleType.equals("A");
            }
        }

        return true;
    }

    public static Double getCrawlDelay(String robotsTxt) {
        if (robotsTxt == null || robotsTxt.isEmpty()) {
            return null;
        }

        Map<String, Double> agentDelays = new HashMap<>();

        String currentUserAgent = null;
        String[] rules = robotsTxt.split("\n");

        for (String line : rules) {
            line = line.trim();
            if (line.startsWith("User-agent:")) {
                currentUserAgent = line.substring(11).trim();
            } else if (line.startsWith("Crawl-delay:")) {
                if (currentUserAgent != null && (currentUserAgent.equals("cis5550-crawler") || currentUserAgent.equals("*"))) {
                    String delay = line.substring(line.indexOf(":") + 1).trim();
                    if (!delay.isEmpty()) {
                        agentDelays.put(currentUserAgent, Double.valueOf(delay));
                    }
                }
            }
        }

        return agentDelays.get("cis5550-crawler") == null ? agentDelays.get("*") : agentDelays.get("cis5550-crawler");
    }
}
