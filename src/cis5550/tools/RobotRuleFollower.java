package cis5550.tools;

import java.util.LinkedHashMap;

public class RobotRuleFollower {
    private static final int DEFAULT_DELAY = 1000;

    private final LinkedHashMap<String, Boolean> theRules;
    private final int theCrawlDelay;

    private RobotRuleFollower(LinkedHashMap<String, Boolean> aRules, int aCrawlDelay) {
        theRules = aRules;
        theCrawlDelay = aCrawlDelay;
    }

    public static RobotRuleFollower fromRobotsTxt(String aRobotsTxt, String aCrawlerName) {
        if (aRobotsTxt == null) {
            return new RobotRuleFollower(new LinkedHashMap<>(), DEFAULT_DELAY);
        }

        String[] myLines = aRobotsTxt.split("\n");

        LinkedHashMap<String, Boolean> myRules = new LinkedHashMap<>();
        int myCrawlDelay = DEFAULT_DELAY;

        int i = 0;

        while (i < myLines.length) {
            if (myLines[i].startsWith("User-agent:")) {
                String myUserAgent = myLines[i].split(":")[1].trim();
                if (myUserAgent.equals("*") || myUserAgent.equals(aCrawlerName)) {
                    i++;
                    while (i < myLines.length && !myLines[i].startsWith("User-agent:")) {
                        if (myLines[i].startsWith("Disallow:")) {
                            myRules.put(myLines[i].split(":")[1].trim(), false);
                        } else if (myLines[i].startsWith("Allow:")) {
                            myRules.put(myLines[i].split(":")[1].trim(), true);
                        } else if (myLines[i].startsWith("Crawl-delay")) {
                            myCrawlDelay = (int) (1000 * Double.parseDouble(myLines[i].split(":")[1].trim()));
                        }
                        i++;
                    }
                }
                i++;
            } else {
                i++;
            }
        }

        return new RobotRuleFollower(myRules, myCrawlDelay);
    }

    public int getCrawlDelay() {
        return theCrawlDelay;
    }

    public boolean isAllowed(String aPath) {
        for (String myRule : theRules.keySet()) {
            if (aPath.startsWith(myRule)) {
                return theRules.get(myRule);
            }
        }
        return true;
    }

    public static void main(String[] args) {
        String robotsTxt = "User-agent: *\n" +
                "Disallow: /nocrawl\n" +
                "Allow: /";

        RobotRuleFollower robotRuleFollower = RobotRuleFollower.fromRobotsTxt(robotsTxt, "cis5550");
        System.out.println(robotRuleFollower.isAllowed("/nocrawl")); // false
    }
}
