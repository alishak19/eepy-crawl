package cis5550.jobs;

import cis5550.flame.FlameContext;
import cis5550.flame.FlameRDD;
import cis5550.jobs.datamodels.TableColumns;
import cis5550.kvs.Row;
import cis5550.tools.*;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static cis5550.tools.RobotsHelper.*;
import static cis5550.tools.URLHelper.*;
import static cis5550.tools.Denylist.*;

public class NewCrawler {
    private static final String CRAWLER_NAME = "cis5550-crawler";
    private static final String HOSTS_TABLE = "hosts";

    private static final String CRAWL_TABLE = "pt-crawl";
    private static final String NEXT_FRONTER = "pt-to-crawl";
    private static final String ALL_CRAWLED = "pt-crawled";

    private static final int THREAD_SLEEP = 10;
    private static final int DEFAULT_CRAWL_DELAY = 1000;
    private static final int CONNECT_TIMEOUT = 1000;
    private static final int READ_TIMEOUT = 4000;

    private static final Logger LOGGER = Logger.getLogger(Crawler.class);
    private static final Denylist myDenylist = new Denylist();

    public static void run(FlameContext aContext, String[] aArgs) throws Exception {
        aContext.output("OK");
        FlameRDD myUrlQueue;
        if (aArgs.length > 0) {
            myUrlQueue = aContext.parallelize(List.of(aArgs[0]));
        } else {
            myUrlQueue = aContext.fromTable(NEXT_FRONTER, row -> row.get(TableColumns.VALUE.value()));
        }

        while (myUrlQueue != null && myUrlQueue.count() != 0) {
            Thread.sleep(THREAD_SLEEP);
            aContext.getKVS().delete(NEXT_FRONTER);
            myUrlQueue.saveAsTable(NEXT_FRONTER);
            myUrlQueue = myUrlQueue.flatMap(myURLString -> {
                LOGGER.debug("Crawling: " + myURLString);

                String[] myUrlParts = cleanupUrl(myURLString);
                String myCleanedUrl = myUrlParts[0] + "://" + myUrlParts[1] + ":" + myUrlParts[2] + myUrlParts[3];
                try {
                    addToTraversed(aContext, myCleanedUrl);
                } catch (Exception e) {
                    LOGGER.error("Adding to traversed failed: " + e.getMessage());
                    return Collections.emptyList();
                }

                URI myUri;
                String myHost;
                try {
                    myUri = new URI(myCleanedUrl);
                    myHost = myUri.getHost();
                } catch (Exception e) {
                    LOGGER.error("URI syntax exception: " + e.getMessage());
                    return Collections.emptyList();
                }

                String robotsTxt = getRobotsTxt(aContext.getKVS(), myHost);
                long crawlDelay = DEFAULT_CRAWL_DELAY;

                try {
                    if (robotsTxt == null) {
                        LOGGER.debug("Unexpected response code from: " + myCleanedUrl);
                        return Collections.emptyList();
                    } else if (!isUrlAllowed(robotsTxt, myCleanedUrl)) {
                        LOGGER.debug("URL is not allowed by robots.txt: " + myCleanedUrl);
                        return Collections.emptyList();
                    } else {
                        Double robotsDelay = getCrawlDelay(robotsTxt);
                        if (robotsDelay != null) {
                            crawlDelay = (long) (DEFAULT_CRAWL_DELAY * robotsDelay);
                        }
                    }
                } catch (Exception e) {
                    LOGGER.error("Robots exception: " + e.getMessage());
                    return Collections.emptyList();
                }

                if (hasPassedTime(aContext, crawlDelay, myUrlParts[1])) {
                    aContext.getKVS()
                            .put(
                                    HOSTS_TABLE,
                                    Hasher.hash(myUrlParts[1]),
                                    TableColumns.TIMESTAMP.value(),
                                    String.valueOf(System.currentTimeMillis()));
                } else {
                    return List.of(myURLString);
                }

                URL myUrl = myUri.toURL();

                HttpURLConnection myHeadConnection;
                myHeadConnection = (HttpURLConnection) myUrl.openConnection();
                myHeadConnection.setRequestMethod("HEAD");
                myHeadConnection.setRequestProperty("User-Agent", CRAWLER_NAME);
                myHeadConnection.setInstanceFollowRedirects(false);
                myHeadConnection.setConnectTimeout(CONNECT_TIMEOUT);
                myHeadConnection.setReadTimeout(READ_TIMEOUT);

                int myHeadResponseCode;
                String myContentType;
                int myContentLength;

                try {
                    myHeadResponseCode = myHeadConnection.getResponseCode();
                    myContentType = myHeadConnection.getContentType();
                    myContentLength = myHeadConnection.getContentLength();
                } catch (Exception e) {
                    LOGGER.error("HEAD connection failed: " + e.getMessage());
                    return Collections.emptyList();
                }

                if (isRedirectCode(myHeadResponseCode)) {
                    try {
                        putPageInTable(
                                aContext, myCleanedUrl, myHeadResponseCode, myContentType, myContentLength, null);
                        String myLocation = myHeadConnection.getHeaderField("Location");
                        if (myLocation != null) {
                            String myNormalizedRedirectUrl = normalizeURL(myLocation, myCleanedUrl);
                            if (myNormalizedRedirectUrl != null && !alreadyTraversed(aContext, myCleanedUrl)) {
                                return List.of(myNormalizedRedirectUrl);
                            }
                            return Collections.emptyList();
                        }
                    } catch (Exception e) {
                        LOGGER.error("Redirect failed: " + e.getMessage());
                        return Collections.emptyList();
                    }
                } else if (myHeadResponseCode == 200) {
                    LOGGER.debug("HTTP GET request");
                    HttpURLConnection myConnection = (HttpURLConnection) myUrl.openConnection();
                    myConnection.setRequestMethod("GET");
                    myConnection.setRequestProperty("User-Agent", CRAWLER_NAME);
                    myConnection.setInstanceFollowRedirects(false);
                    myConnection.setConnectTimeout(CONNECT_TIMEOUT);
                    myConnection.setReadTimeout(READ_TIMEOUT);

                    int myResponseCode;
                    try {
                        LOGGER.debug("Getting response");
                        myResponseCode = myConnection.getResponseCode();
                        if (myResponseCode != 200) {
                            return Collections.emptyList();
                        }
                    } catch (Exception e) {
                        LOGGER.error("GET connection failed: " + e.getMessage());
                        return Collections.emptyList();
                    }

                    byte[] myContent = null;
                    try {
                        LOGGER.debug("getting content");
                        if (myContentType != null && myContentType.contains("text/html")) {
                            InputStream is = myConnection.getInputStream();
                            ByteArrayOutputStream byteBuffer = new ByteArrayOutputStream();
                            byte[] buffer = new byte[4096];
                            int bytesRead;
                            while ((bytesRead = is.read(buffer)) != -1) {
                                byteBuffer.write(buffer, 0, bytesRead);
                            }
                            myContent = byteBuffer.toByteArray();
                            is.close();
                        }
                    } catch (Exception e) {
                        LOGGER.error("Reading byte stream connection failed: " + e.getMessage());
                        return Collections.emptyList();
                    }

                    try {
                        putPageInTable(
                                aContext, myCleanedUrl, myResponseCode, myContentType, myContentLength, myContent);
                        if (myContent != null) {
                            List<String> myUrls = extractUrls(new String(myContent));
                            LOGGER.debug("Extracted urls");
                            List<String> myToTraverseUrls = new LinkedList<>();
                            for (String url : myUrls) {
                                String myNormalizedUrl = normalizeURL(myCleanedUrl, url);
                                if (myNormalizedUrl != null && !alreadyTraversed(aContext, myNormalizedUrl) &&
                                        probabilisticDomainFilter(myNormalizedUrl) && !myDenylist.isBlocked(myCleanedUrl)) {
                                    myToTraverseUrls.add(myNormalizedUrl);
                                }
                            }
                            LOGGER.debug("PUT finished");
                            return myToTraverseUrls;
                        }
                    } catch (Exception e) {
                        LOGGER.error("Failed while adding page content: " + e.getMessage());
                        return Collections.emptyList();
                    }
                } else {
                    try {
                        putPageInTable(
                                aContext, myCleanedUrl, myHeadResponseCode, myContentType, myContentLength, null);
                    } catch (Exception e) {
                        LOGGER.error("Failed while adding page content: " + e.getMessage());
                    }
                    return Collections.emptyList();
                }
                return Collections.emptyList();
            });
        }
    }

    private static List<String> extractUrls(String aContent) {
        List<String> myUrls = new LinkedList<>();

        Pattern myTagPattern = Pattern.compile("<\\s*a\\b[^>]*>", Pattern.CASE_INSENSITIVE);
        Pattern myHrefPattern = Pattern.compile("href\\s*=\\s*\"([^\"]*)\"", Pattern.CASE_INSENSITIVE);

        Matcher myTagMatcher = myTagPattern.matcher(aContent);
        while (myTagMatcher.find()) {
            String myTag = myTagMatcher.group();

            Matcher myHrefMatcher = myHrefPattern.matcher(myTag);
            if (myHrefMatcher.find()) {
                String myUrl = myHrefMatcher.group(1);
                myUrls.add(myUrl);
            }
        }
        return myUrls;
    }

    private static boolean isRedirectCode(int aCode) {
        return aCode == 301 || aCode == 302 || aCode == 303 || aCode == 307 || aCode == 308;
    }

    private static boolean alreadyTraversed(FlameContext aContext, String aUrl) throws Exception {
        LOGGER.debug("Checking traversed");
        return aContext.getKVS().get(ALL_CRAWLED, Hasher.hash(aUrl), TableColumns.URL.value()) != null;
    }

    private static void addToTraversed(FlameContext aContext, String aUrl) throws Exception {
        aContext.getKVS().put(ALL_CRAWLED, Hasher.hash(aUrl), TableColumns.URL.value(), aUrl);
    }

    private static void putPageInTable(
            FlameContext aContext,
            String aUrl,
            int aResponseCode,
            String aContentType,
            int aContentLength,
            byte[] aBody)
            throws Exception {
        LOGGER.debug("PUT request");
        String myHashedUrl = Hasher.hash(aUrl);
        Row row = new Row(myHashedUrl);
        row.put(TableColumns.URL.value(), aUrl);
        row.put(TableColumns.RESPONSE_CODE.value(), String.valueOf(aResponseCode));
        if (aContentType != null) {
            row.put(TableColumns.CONTENT_TYPE.value(), aContentType);
        }
        if (aContentLength >= 0) {
            row.put(TableColumns.CONTENT_LENGTH.value(), String.valueOf(aContentLength));
        }
        if (aBody != null) {
            row.put(TableColumns.PAGE.value(), aBody);
        }
        aContext.getKVS().putRow(CRAWL_TABLE, row);
    }

    private static boolean hasPassedTime(FlameContext aContext, long aDelay, String aHost) throws Exception {
        byte[] myLastAccessedTimeRaw =
                aContext.getKVS().get(HOSTS_TABLE, Hasher.hash(aHost), TableColumns.TIMESTAMP.value());
        if (myLastAccessedTimeRaw == null || myLastAccessedTimeRaw.length == 0) {
            return true;
        }
        long myLastAccessedTime = Long.valueOf(new String(myLastAccessedTimeRaw));
        return System.currentTimeMillis() - myLastAccessedTime > aDelay;
    }
}

