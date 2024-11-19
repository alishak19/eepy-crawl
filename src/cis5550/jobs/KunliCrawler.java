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
import java.util.*;
import java.util.regex.Pattern;

import static cis5550.tools.CrawlerBlacklist.*;
import static cis5550.tools.RobotsHelper.*;
import static cis5550.tools.URLHelper.*;

public class KunliCrawler {

    private static final Logger LOGGER = Logger.getLogger(Crawler.class);

    private static final String CRAWLER_NAME = "cis5550-crawler";
    private static final String CRAWL_TABLE = "pt-crawl";
    private static final String HOSTS_TABLE = "hosts";
    private static final int THREAD_SLEEP = 10;
    private static final int DEFAULT_CRAWL_DELAY = 1000;
    private static final int CONNECT_TIMEOUT = 10000;
    private static final int READ_TIMEOUT = 60000;

    private static final String RESPONSE_CODE = "responseCode";
    private static final String CONTENT_TYPE = "contentType";
    private static final String CONTENT_LENGTH = "contentLength";
    private static final String LOCATION = "location";
    private static final String CONTENT = "content";

    public static void run(FlameContext context, String[] args) throws Exception {
        if (!(args.length <= 2)) {
            context.output("Error: Expected argument 1 to be a single seed URL. Optional argument 2 for blacklist " +
                    "table address");
            return;
        }

        List<String> initialURL = new ArrayList<>();
        String normalizedURL = normalizeURL(args[0], args[0]);
        initialURL.add(normalizedURL);
        LOGGER.debug("Starting crawler from" + normalizedURL);

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
                long crawlDelay = DEFAULT_CRAWL_DELAY;

                if (robotsTxt == null) {
                    LOGGER.debug("Unexpected response code from: " + url);
                    return Collections.emptyList();
                } else if (!isUrlAllowed(robotsTxt, url)) {
                    LOGGER.debug("URL is not allowed by robots.txt: " + url);
                    return Collections.emptyList();
                } else {
                    Double robotsDelay = getCrawlDelay(robotsTxt);
                    if (robotsDelay != null) {
                        crawlDelay = (long) (DEFAULT_CRAWL_DELAY * robotsDelay);
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
                Map<String, Object> headerAttr = new HashMap<>();
                HttpURLConnection headConnection;

                try {
                    headConnection = (HttpURLConnection) urlObj.openConnection();
                    headConnection.setRequestMethod("HEAD");
                    headConnection.setRequestProperty("User-Agent", CRAWLER_NAME);
                    headConnection.setInstanceFollowRedirects(false);
                    headConnection.setConnectTimeout(CONNECT_TIMEOUT);
                    headConnection.setReadTimeout(READ_TIMEOUT);
                    headConnection.connect();

                    headerAttr.put(RESPONSE_CODE, headConnection.getResponseCode());
                    headerAttr.put(CONTENT_TYPE, headConnection.getContentType());
                    headerAttr.put(CONTENT_LENGTH, headConnection.getContentLength());
                    headerAttr.put(LOCATION, headConnection.getHeaderField("Location"));
                } catch (Exception e) {
                    LOGGER.error("HEAD connection failed: " + e.getMessage());
                    return Collections.emptyList();
                }

                try {
                    Row contentRow = client.getRow(CRAWL_TABLE, urlHash);
                    if (contentRow == null) {
                        contentRow = new Row(urlHash);
                    }
                    contentRow.put(TableColumns.URL.value(), url);
                    contentRow.put(TableColumns.RESPONSE_CODE.value(), String.valueOf(headerAttr.get(RESPONSE_CODE)));

                    if (headerAttr.get(CONTENT_TYPE) != null) {
                        contentRow.put(TableColumns.CONTENT_TYPE.value(), (String) headerAttr.get(CONTENT_TYPE));
                    }
                    if ((int) headerAttr.get(CONTENT_LENGTH) != -1) {
                        contentRow.put(TableColumns.CONTENT_LENGTH.value(), String.valueOf(headerAttr.get(CONTENT_LENGTH)));
                    }

                    client.putRow(CRAWL_TABLE, contentRow);
                } catch (Exception e) {
                    LOGGER.error("Updating crawl table with header data failed: " + e.getMessage());
                    return Collections.emptyList();
                }

                // Handle redirects
                int responseCode = (int) headerAttr.get(RESPONSE_CODE);
                String location = (String) headerAttr.get(LOCATION);

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

                if (responseCode == 200 && headerAttr.get(CONTENT_TYPE) != null &&
                        ((String) headerAttr.get(CONTENT_TYPE)).startsWith("text/html")) {
                    LOGGER.debug("GET request");
                    HttpURLConnection getConnection;
                    Map<String, Object> bodyAttr = new HashMap<>();

                    try {
                        getConnection = (HttpURLConnection) urlObj.openConnection();
                        getConnection.setRequestMethod("GET");
                        getConnection.setRequestProperty("User-Agent", CRAWLER_NAME);
                        getConnection.setConnectTimeout(CONNECT_TIMEOUT);
                        getConnection.setReadTimeout(READ_TIMEOUT);
                        getConnection.connect();
                    } catch (Exception e) {
                        LOGGER.error("GET connection failed: " + e.getMessage());
                        return Collections.emptyList();
                    }

                    try {
                        bodyAttr.put(RESPONSE_CODE, getConnection.getResponseCode());
                        if ((int) bodyAttr.get(RESPONSE_CODE) == 200) {
                            InputStream is = getConnection.getInputStream();
                            ByteArrayOutputStream byteBuffer = new ByteArrayOutputStream();
                            byte[] buffer = new byte[4096];
                            int bytesRead;
                            while ((bytesRead = is.read(buffer)) != -1) {
                                byteBuffer.write(buffer, 0, bytesRead);
                            }
                            byte[] contentBytes = byteBuffer.toByteArray();
                            is.close();

                            bodyAttr.put(CONTENT, contentBytes);
                        }
                    } catch (Exception e) {
                        LOGGER.error("Crawler failed while reading GET connection: " + e.getMessage());
                        return Collections.emptyList();
                    }

                    try {
                        Row contentRow = client.getRow(CRAWL_TABLE, urlHash);
                        if (contentRow == null) {
                            contentRow = new Row(urlHash);
                        }

                        LOGGER.debug("Adding new URL: " + url);
                        contentRow.put(TableColumns.PAGE.value(), (byte[]) bodyAttr.get(CONTENT));
                        contentRow.put(TableColumns.RESPONSE_CODE.value(), String.valueOf((int) bodyAttr.get(RESPONSE_CODE)));
                        client.putRow(CRAWL_TABLE, contentRow);

                        LOGGER.debug("Extracting new URLs");
                        List<String> newUrls = new ArrayList<>();
                        Map<String, List<String>> extractedUrlsMap = extractUrlsAndAnchors((byte[]) bodyAttr.get(CONTENT));

                        for (Map.Entry<String, List<String>> entry : extractedUrlsMap.entrySet()) {
                            String href = entry.getKey();
                            String normalizedUrl = normalizeURL(url, href);
                            if (normalizedUrl != null) {
                                newUrls.add(normalizedUrl);
                            }
                        }

                        return newUrls;
                    } catch (Exception e) {
                        LOGGER.error("Crawler failed while updating crawl table with body: " + e.getMessage());
                        return Collections.emptyList();
                    }
                }
                return Collections.emptyList();
            });
            Thread.sleep(THREAD_SLEEP);
        }
    }
}