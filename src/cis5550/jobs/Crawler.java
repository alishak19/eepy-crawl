package cis5550.jobs;

import cis5550.flame.FlameContext;
import cis5550.flame.FlameRDD;
import cis5550.jobs.datamodels.TableColumns;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Hasher;
import cis5550.tools.Logger;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.regex.Pattern;

import static cis5550.tools.RobotsHelper.*;
import static cis5550.tools.URLHelper.*;

public class Crawler {

    private static final Logger LOGGER = Logger.getLogger(Crawler.class);

    public static final String CRAWLER_NAME = "cis5550-crawler";
    private static final String CRAWL_TABLE = "pt-crawl";
    private static final String HOSTS_TABLE = "hosts";
    private static final String CONTENT_TABLE = "content-hashes";
    public static final int THREAD_SLEEP = 10;

    public static void run(FlameContext context, String[] args) throws Exception {
        if (!(args.length <= 2)) {
            context.output("Error: Expected argument 1 to be a single seed URL. Optional argument 2 for blacklist " +
                    "table address");
            return;
        }

        List<String> initialURL = new ArrayList<>();
        String normalizedURL = normalizeURL(args[0], args[0]);
        initialURL.add(normalizedURL);
        LOGGER.debug(normalizedURL);

        FlameRDD urlQueue = context.parallelize(initialURL);

        while (urlQueue.count() > 0) {
            urlQueue = urlQueue.flatMap(url -> {

                if (url == null || url.isEmpty()) {
                    context.output("Error: Received a null or empty URL.");
                    return Collections.emptyList();
                }

                URI uri = new URI(url);
                URL urlObj = uri.toURL();
                String host = uri.getHost();
                LOGGER.debug("Processing URL: " + url);

                KVSClient client = context.getKVS();

                // EC #2
                List<Pattern> blacklistPatterns;
                if (args.length == 2) {
                    String blacklistTable = args[1];
                    LOGGER.debug("Getting blacklisted urls from: " + blacklistTable);
                    blacklistPatterns = loadBlacklistPatterns(context.getKVS(), blacklistTable);
                    if (isBlacklisted(url, blacklistPatterns)) {
                        LOGGER.debug("URL is blacklisted: " + url);
                        return Collections.emptyList();
                    }
                }

                // Robots.txt
                String robotsTxt = getRobotsTxt(client, host);
                long crawlDelay = 1000;

                if (!isUrlAllowed(robotsTxt, url)) {
                    LOGGER.debug("URL is not allowed by robots.txt: " + url);
                    return Collections.emptyList();
                } else {
                    Double robotsDelay = getCrawlDelay(robotsTxt);
                    if (robotsDelay != null) {
                        crawlDelay = (long) (1000 * robotsDelay);
                        LOGGER.debug("Crawl delay updated: " + crawlDelay);
                    } else {
                        LOGGER.debug("Using default crawl delay");
                    }
                }

                // Duplicate crawls
                String urlHash = Hasher.hash(url);
                Row dupRow = client.getRow(CRAWL_TABLE, urlHash);
                if (dupRow != null && dupRow.get(TableColumns.URL.value()) != null) {
                    LOGGER.debug("Skipping already seen URL: " + url);
                    return Collections.emptyList();
                }

                // Rate limiting based on host access time
                Row hostRow = client.getRow(HOSTS_TABLE, host);
                long currentTime = System.currentTimeMillis();
                if (hostRow != null) {
                    long lastAccessed = Long.parseLong(hostRow.get(TableColumns.LAST_ACCESSED.value()));
                    if ((currentTime - lastAccessed) < crawlDelay) {
                        LOGGER.debug("Rerun due to crawl delay");
                        return Collections.singletonList(url);
                    }
                }

                LOGGER.debug("Updating last accessed time to " + currentTime);
                Row newHostRow = new Row(host);
                newHostRow.put(TableColumns.LAST_ACCESSED.value(), String.valueOf(currentTime));
                client.putRow(HOSTS_TABLE, newHostRow);

                // Request to get header and status
                LOGGER.debug("HEAD request");
                HttpURLConnection headConnection = (HttpURLConnection) urlObj.openConnection();
                headConnection.setRequestMethod("HEAD");
                headConnection.setRequestProperty("User-Agent", CRAWLER_NAME);
                headConnection.setInstanceFollowRedirects(false);
                headConnection.connect();

                int responseCode = headConnection.getResponseCode();
                String contentType = headConnection.getContentType();
                int contentLength = headConnection.getContentLength();
                String location = headConnection.getHeaderField("Location");

                Row contentRow = client.getRow(CRAWL_TABLE, urlHash);
                if (contentRow == null) {
                    contentRow = new Row(urlHash);
                }
                contentRow.put(TableColumns.URL.value(), url);
                contentRow.put(TableColumns.RESPONSE_CODE.value(), String.valueOf(responseCode));

                if (contentType != null) {
                    contentRow.put(TableColumns.CONTENT_TYPE.value(), contentType);
                }
                if (contentLength != -1) {
                    contentRow.put(TableColumns.CONTENT_LENGTH.value(), String.valueOf(contentLength));
                }

                client.putRow(CRAWL_TABLE, contentRow);

                // Handle redirects
                if (responseCode == 301 || responseCode == 302 || responseCode == 303 ||
                        responseCode == 307 || responseCode == 308
                ) {
                    List<String> newUrls = new ArrayList<>();

                    if (location != null && !location.isEmpty()) {
                        LOGGER.debug("Redirecting to: " + location);
                        String newUrl = normalizeURL(url, location);
                        if (newUrl != null) {
                            newUrls.add(newUrl);
                        }
                    }
                    return newUrls;
                }

                if (responseCode == 200 && contentType != null && contentType.startsWith("text/html")) {
                    LOGGER.debug("GET request");

                    HttpURLConnection getConnection = (HttpURLConnection) urlObj.openConnection();
                    getConnection.setRequestMethod("GET");
                    getConnection.setRequestProperty("User-Agent", CRAWLER_NAME);
                    getConnection.connect();

                    int getResponseCode = getConnection.getResponseCode();
                    if (getResponseCode == 200) {
                        InputStream is = getConnection.getInputStream();
                        ByteArrayOutputStream byteBuffer = new ByteArrayOutputStream();
                        byte[] buffer = new byte[4096];
                        int bytesRead;
                        while ((bytesRead = is.read(buffer)) != -1) {
                            byteBuffer.write(buffer, 0, bytesRead);
                        }
                        byte[] contentBytes = byteBuffer.toByteArray();
                        is.close();

                        // EC #1
                        String contentHash = Hasher.hash(new String(contentBytes, StandardCharsets.UTF_8));
                        Row existingHashRow = client.getRow(CONTENT_TABLE, contentHash);
                        if (existingHashRow != null) {
                            LOGGER.debug("Adding existing content from URL: " + url);
                            contentRow.put(TableColumns.CANONICAL_URL.value(), existingHashRow.get(TableColumns.URL.value()));
                            contentRow.put(TableColumns.RESPONSE_CODE.value(), String.valueOf(getResponseCode));
                            client.putRow(CRAWL_TABLE, contentRow);
                            return Collections.emptyList();
                        } else {
                            LOGGER.debug("Adding new URL: " + url);

                            contentRow.put(TableColumns.PAGE.value(), contentBytes);
                            contentRow.put(TableColumns.RESPONSE_CODE.value(), String.valueOf(getResponseCode));
                            client.putRow(CRAWL_TABLE, contentRow);

                            LOGGER.debug("Extracting new URLs");
                            List<String> newUrls = new ArrayList<>();
                            Map<String, List<String>> extractedUrlsMap = extractUrlsAndAnchors(contentBytes);

                            for (Map.Entry<String, List<String>> entry : extractedUrlsMap.entrySet()) {
                                String href = entry.getKey();
                                List<String> anchorTexts = entry.getValue();

                                String normalizedUrl = normalizeURL(url, href);
                                if (normalizedUrl != null) {
                                    newUrls.add(normalizedUrl);
//                                    if (!anchorTexts.isEmpty() && isUrlAllowed(robotsTxt, normalizedUrl) &&
//                                            !isBlacklisted(url, blacklistPatterns)) {
//                                        String combinedAnchorText = String.join(" ", anchorTexts);
//
//                                        Row targetRow = client.getRow(CRAWL_TABLE, Hasher.hash(normalizedUrl));
//                                        LOGGER.debug("Adding anchor to: " + normalizedUrl + " with key " + Hasher.hash(normalizedUrl));
//                                        if (targetRow == null) {
//                                            targetRow = new Row(Hasher.hash(normalizedUrl));
//                                        }
//
//                                        targetRow.put(TableColumns.ANCHOR_PREFIX.value() + Hasher.hash(url), combinedAnchorText);
//                                        client.putRow(CRAWL_TABLE, targetRow);
//                                    }
                                }
                            }

                            LOGGER.debug("Caching content of page");
                            Row hashRow = new Row(contentHash);
                            hashRow.put(TableColumns.URL.value(), url);
                            client.putRow(CONTENT_TABLE, hashRow);

                            return newUrls;
                        }
                    }
                }
                return Collections.emptyList();
            });
            Thread.sleep(THREAD_SLEEP);
        }
    }

    private static boolean isBlacklisted(String url, List<Pattern> blacklistPatterns) {
        for (Pattern pattern : blacklistPatterns) {
            if (pattern.matcher(url).matches()) {
                return true;
            }
        }
        return false;
    }

    private static List<Pattern> loadBlacklistPatterns(KVSClient client, String tableName) throws IOException {
        List<Pattern> patterns = new ArrayList<>();
        Iterator<Row> rows = client.scan(tableName);
        while (rows.hasNext()) {
            Row row = rows.next();
            String pattern = row.get(TableColumns.PATTERNS.value());
            if (pattern != null) {
                String regexPattern = pattern
                        .replace(".", "\\.")
                        .replace("*", ".*");
                patterns.add(Pattern.compile(regexPattern));
            }
        }
        return patterns;
    }
}
