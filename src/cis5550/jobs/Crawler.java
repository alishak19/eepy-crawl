package cis5550.jobs;

import cis5550.flame.FlameContext;
import cis5550.flame.FlameRDD;
import cis5550.jobs.datamodels.TableColumns;
import cis5550.tools.*;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static cis5550.tools.URLNormalizer.*;

public class Crawler {
    public static final String CRAWLER_NAME = "cis5550-crawler";
    public static final int THREAD_SLEEP = 10;

    private static final String CRAWL_TABLE = "pt-crawl";
    private static final String HOSTS_TABLE = "hosts";
    private static final String CONTENT_TABLE = "content";

    private static Logger LOGGER = Logger.getLogger(Crawler.class);

    public static void run(FlameContext aContext, String[] aArgs) throws Exception {
        if (aArgs.length < 1) {
            aContext.output("Usage: Crawler <seed-url>");
            System.exit(1);
        }

        aContext.output("OK");

        String myInitUrl = aArgs[0];
        String myDenylistTable;

        if (aArgs.length == 2) {
            myDenylistTable = aArgs[1];
        } else {
            myDenylistTable = null;
        }

        FlameRDD myUrlQueue = aContext.parallelize(List.of(myInitUrl));

        while (myUrlQueue.count() != 0) {
            Thread.sleep(THREAD_SLEEP);
            myUrlQueue = myUrlQueue.distinct().flatMap(myURLString -> {
                String[] myUrlParts = cleanupUrl(myURLString);
                String myCleanedUrl = myUrlParts[0] + "://" + myUrlParts[1] + ":" + myUrlParts[2] + myUrlParts[3];

                RobotRuleFollower myRobotRuleFollower = getRobotRuleFollower(aContext, myUrlParts);

                if (!myRobotRuleFollower.isAllowed(myUrlParts[3])) {
                    return new LinkedList<>();
                }

                Denylist myDenylist = Denylist.fromKVSTable(aContext.getKVS(), myDenylistTable);

                if (myDenylist.isBlocked(myCleanedUrl)) {
                    return new LinkedList<>();
                }

                if (hasPassedTime(aContext, myRobotRuleFollower.getCrawlDelay(), myUrlParts[1])) {
                    aContext.getKVS()
                            .put(
                                    HOSTS_TABLE,
                                    Hasher.hash(myUrlParts[1]),
                                    TableColumns.TIMESTAMP.value(),
                                    String.valueOf(System.currentTimeMillis()));
                } else {
                    return List.of(myURLString);
                }

                URL myUrl = new URL(myCleanedUrl);

                HttpURLConnection myHeadConnection = (HttpURLConnection) myUrl.openConnection();
                myHeadConnection.setRequestMethod("HEAD");
                myHeadConnection.setRequestProperty("User-Agent", CRAWLER_NAME);
                myHeadConnection.setInstanceFollowRedirects(false);

                int myHeadResponseCode = myHeadConnection.getResponseCode();
                String myContentType = myHeadConnection.getContentType();
                int myContentLength = myHeadConnection.getContentLength();

                if (isRedirectCode(myHeadResponseCode)) {
                    putPageInTable(
                            aContext, myCleanedUrl, myHeadResponseCode, myContentType, myContentLength, null, null);
                    String myLocation = myHeadConnection.getHeaderField("Location");
                    if (myLocation != null) {
                        String myNormalizedRedirectUrl = normalizeAndFilterUrl(myLocation, myUrlParts);
                        if (myNormalizedRedirectUrl != null && !alreadyTraversed(aContext, myNormalizedRedirectUrl)) {
                            return List.of(myNormalizedRedirectUrl);
                        }
                        return new LinkedList<>();
                    }
                } else if (myHeadResponseCode == 200) {
                    HttpURLConnection myConnection = (HttpURLConnection) myUrl.openConnection();
                    myConnection.setRequestMethod("GET");
                    myConnection.setRequestProperty("User-Agent", CRAWLER_NAME);
                    myConnection.setInstanceFollowRedirects(false);

                    int myResponseCode = myConnection.getResponseCode();
                    if (myResponseCode != 200) {
                        return new LinkedList<>();
                    }
                    byte[] myContent = null;
                    if (myContentType != null && myContentType.compareTo("text/html") == 0) {
                        myContent = myConnection.getInputStream().readAllBytes();
                    }

                    String myCanonicalUrl = hasSeenContent(aContext, myContent);
                    if (myCanonicalUrl != null && !myCanonicalUrl.equals(myCleanedUrl)) {
                        putPageInTable(
                                aContext,
                                myCleanedUrl,
                                myResponseCode,
                                myContentType,
                                myContentLength,
                                null,
                                myCanonicalUrl);
                        return new LinkedList<>();
                    }

                    Map<String, List<String>> myAnchors =
                            extractAnchors(new String(myContent), myUrlParts, myRobotRuleFollower, myDenylist);

                    putPageInTable(
                            aContext, myCleanedUrl, myResponseCode, myContentType, myContentLength, myContent, null);

                    putPageContentInSeen(aContext, myContent, myCleanedUrl);

                    putAnchorsInTable(aContext, myCleanedUrl, myAnchors);

                    List<String> myUrls = extractUrls(new String(myContent));
                    List<String> myNormalizedUrls = normalizeAndFilterUrls(myUrls, myUrlParts);
                    List<String> myToTraverseUrls = new LinkedList<>();
                    for (String myNormalizedUrl : myNormalizedUrls) {
                        if (!alreadyTraversed(aContext, myNormalizedUrl)) {
                            myToTraverseUrls.add(myNormalizedUrl);
                        }
                    }
                    return myToTraverseUrls;
                } else {
                    putPageInTable(
                            aContext, myCleanedUrl, myHeadResponseCode, myContentType, myContentLength, null, null);
                }
                return new LinkedList<>();
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

    private static Map<String, List<String>> extractAnchors(
            String aContent, String[] aBaseUrl, RobotRuleFollower aRobotRuleFollower, Denylist aDenylist)
            throws Exception {
        String myAnchorPattern = "<a\\s+[^>]*href=\"([^\"]*)\"[^>]*>(.*?)</a>";
        Pattern myPattern = Pattern.compile(myAnchorPattern, Pattern.CASE_INSENSITIVE);
        Matcher myMatcher = myPattern.matcher(aContent);

        Map<String, List<String>> myAnchorsMap = new HashMap<>();

        while (myMatcher.find()) {
            String myHref = myMatcher.group(1).trim();
            String myAnchorText = myMatcher.group(2).trim();

            String myCleaned = URLNormalizer.normalizeAndFilterUrl(myHref, aBaseUrl);
            String[] myURLParts = URLNormalizer.cleanupUrl(myCleaned);
            if (myCleaned == null) {
                continue;
            }
            if (aRobotRuleFollower.isAllowed(myURLParts[3]) && !aDenylist.isBlocked(myCleaned)) {
                myAnchorsMap.computeIfAbsent(myCleaned, k -> new LinkedList<>()).add(myAnchorText);
            }
        }
        return myAnchorsMap;
    }

    private static boolean isRedirectCode(int aCode) {
        return aCode == 301 || aCode == 302 || aCode == 303 || aCode == 307 || aCode == 308;
    }

    private static boolean alreadyTraversed(FlameContext aContext, String aUrl) throws Exception {
        return aContext.getKVS().get(CRAWL_TABLE, Hasher.hash(aUrl), TableColumns.RESPONSE_CODE.value()) != null;
    }

    private static void putPageInTable(
            FlameContext aContext,
            String aUrl,
            int aResponseCode,
            String aContentType,
            int aContentLength,
            byte[] aBody,
            String aCanonicalUrl)
            throws Exception {
        String myHashedUrl = Hasher.hash(aUrl);
        aContext.getKVS().put(CRAWL_TABLE, myHashedUrl, TableColumns.URL.value(), aUrl);
        aContext.getKVS()
                .put(CRAWL_TABLE, myHashedUrl, TableColumns.RESPONSE_CODE.value(), String.valueOf(aResponseCode));
        if (aContentType != null) {
            aContext.getKVS().put(CRAWL_TABLE, myHashedUrl, TableColumns.CONTENT_TYPE.value(), aContentType);
        }
        if (aContentLength >= 0) {
            aContext.getKVS()
                    .put(CRAWL_TABLE, myHashedUrl, TableColumns.CONTENT_LENGTH.value(), String.valueOf(aContentLength));
        }
        if (aBody != null) {
            aContext.getKVS().put(CRAWL_TABLE, myHashedUrl, TableColumns.PAGE.value(), aBody);
        }
        if (aCanonicalUrl != null) {
            aContext.getKVS().put(CRAWL_TABLE, myHashedUrl, TableColumns.CANONICAL_URL.value(), aCanonicalUrl);
        }
    }

    private static void putPageContentInSeen(FlameContext aContext, byte[] aContent, String aCanonicalUrl)
            throws Exception {
        aContext.getKVS()
                .put(
                        CONTENT_TABLE,
                        Hasher.hash(new String(aContent)),
                        TableColumns.CANONICAL_URL.value(),
                        aCanonicalUrl);
    }

    private static void putAnchorsInTable(FlameContext aContext, String aBaseUrl, Map<String, List<String>> aAnchors)
            throws Exception {
        for (String myAnchorUrl : aAnchors.keySet()) {
            String myAnchorString = String.join(" ", aAnchors.get(myAnchorUrl));
            aContext.getKVS()
                    .put(
                            CRAWL_TABLE,
                            Hasher.hash(myAnchorUrl),
                            TableColumns.ANCHOR_PREFIX.value() + aBaseUrl,
                            myAnchorString);
        }
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

    private static RobotRuleFollower getRobotRuleFollower(FlameContext aContext, String[] aUrl) throws Exception {
        byte[] myRobotFile = aContext.getKVS().get(HOSTS_TABLE, Hasher.hash(aUrl[1]), TableColumns.ROBOTS.value());
        if (myRobotFile == null) {
            URL myHostUrl = new URL(aUrl[0] + "://" + aUrl[1] + ":" + aUrl[2] + "/robots.txt");
            HttpURLConnection myConnection = (HttpURLConnection) myHostUrl.openConnection();
            myConnection.setRequestMethod("GET");
            myConnection.setRequestProperty("User-Agent", CRAWLER_NAME);

            int myResponseCode = myConnection.getResponseCode();
            if (myResponseCode == 200) {
                myRobotFile = myConnection.getInputStream().readAllBytes();
                aContext.getKVS().put(HOSTS_TABLE, Hasher.hash(aUrl[1]), TableColumns.ROBOTS.value(), myRobotFile);
            } else {
                return RobotRuleFollower.fromRobotsTxt(null, CRAWLER_NAME);
            }
        }

        return RobotRuleFollower.fromRobotsTxt(new String(myRobotFile), CRAWLER_NAME);
    }

    private static String hasSeenContent(FlameContext aContext, byte[] aContent) throws Exception {
        if (aContent == null || aContent.length == 0) {
            return null;
        }
        byte[] myCanonicalUrlBytes = aContext.getKVS()
                .get(CONTENT_TABLE, Hasher.hash(new String(aContent)), TableColumns.CANONICAL_URL.value());
        if (myCanonicalUrlBytes == null || myCanonicalUrlBytes.length == 0) {
            return null;
        }
        return new String(myCanonicalUrlBytes);
    }
}
