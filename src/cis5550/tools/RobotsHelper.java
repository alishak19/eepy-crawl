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
            robotsConnection.setInstanceFollowRedirects(true);
            robotsConnection.setRequestMethod("GET");
            robotsConnection.setRequestProperty("User-Agent", USER_AGENT);
            robotsConnection.setConnectTimeout(5000);
            robotsConnection.setReadTimeout(5000);
            robotsConnection.connect();

            int responseCode = robotsConnection.getResponseCode();

            if (responseCode == 200) {
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
            } else if (responseCode == 301 || responseCode == 302 || responseCode == 303 ||
                    responseCode == 307 || responseCode == 308) {
                String newLocation = robotsConnection.getHeaderField("Location");
                LOGGER.debug("Redirect detected for robots.txt, new location: " + newLocation);

                if (newLocation != null) {
                    return getRobotsTxtFromRedirect(newLocation, robotsKey, client);
                } else {
                    LOGGER.warn("Redirect without a valid location for: " + host);
                    return null;
                }
            } else {
                LOGGER.error("Unexpected response code: " + responseCode + " for host: " + host);
                return null;
            }
        } catch (Exception e) {
            Logger.getLogger(RobotsHelper.class).error("Error fetching robots.txt from " + host + ": " + e.getMessage());
        }
        return null;
    }

    private static String getRobotsTxtFromRedirect(String redirectUrl, String robotsKey, KVSClient client) {
        try {
            URL robotsUrl = new URI(redirectUrl).toURL();
            HttpURLConnection connection = (HttpURLConnection) robotsUrl.openConnection();
            connection.setRequestMethod("GET");
            connection.setRequestProperty("User-Agent", USER_AGENT);
            connection.setInstanceFollowRedirects(true);
            connection.connect();

            if (connection.getResponseCode() == 200) {
                LOGGER.debug("Successfully followed redirect and fetched robots.txt from: " + redirectUrl);

                InputStream is = connection.getInputStream();
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
            } else {
                LOGGER.warn("Failed to fetch redirected robots.txt. Response code: " + connection.getResponseCode());
                return null;
            }
        } catch (Exception e) {
            LOGGER.error("Error following redirect for robots.txt: " + e.getMessage());
            return null;
        }
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
