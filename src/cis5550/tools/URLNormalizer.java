package cis5550.tools;

import java.util.LinkedList;
import java.util.List;

public class URLNormalizer {
    private static final String[] EXCLUDED_SUFFIX = new String[] {
            ".jpg", ".jpeg", ".gif", ".png", ".txt"
    };

    public static String[] cleanupUrl(String aUrl) {
        String[] myUrlParts = URLParser.parseURL(aUrl);
        if (myUrlParts[3] != null) {
            myUrlParts[3] = myUrlParts[3].split("#")[0];
        }
        if (myUrlParts[2] == null) {
            myUrlParts[2] = myUrlParts[0].compareTo("http") == 0 ? "80" : "443";
        }
        return myUrlParts;
    }

    public static boolean includeUrl(String[] aUrl) {
        if (aUrl[0].compareTo("http") != 0 && aUrl[0].compareTo("https") != 0) {
            return false;
        }
        for (String mySuffix : EXCLUDED_SUFFIX) {
            if (aUrl[3].endsWith(mySuffix)) {
                return false;
            }
        }
        return true;
    }

    public static List<String> normalizeAndFilterUrls(List<String> aUrls, String[] aBaseUrl) throws Exception {
        List<String> myNormalizedUrls = new LinkedList<>();
        for (String myUrl : aUrls) {
            String myNormalizedUrl = normalizeAndFilterUrl(myUrl, aBaseUrl);
            if (myNormalizedUrl != null) {
                myNormalizedUrls.add(myNormalizedUrl);
            }
        }
        return myNormalizedUrls;
    }

    public static String normalizeAndFilterUrl(String aUrl, String[] aBaseUrl) {
        String[] myUrlParts = URLParser.parseURL(aUrl);

        myUrlParts[3] = myUrlParts[3].split("#")[0];
        if (myUrlParts[3].isEmpty()) {
            myUrlParts[3] = aBaseUrl[3];
        }
        myUrlParts[3] = cleanupDotsAndSlashes(myUrlParts[3]);
        // Relative links
        if (!myUrlParts[3].isEmpty() && myUrlParts[3].charAt(0) != '/') {
            // Handle "back" links
            String currentPath = aBaseUrl[3];
            int myLastSlash = aBaseUrl[3].lastIndexOf('/');
            if (myLastSlash >= 0) {
                currentPath = currentPath.substring(0, myLastSlash);
            }
            while (myUrlParts[3].startsWith("../")) {
                myUrlParts[3] = myUrlParts[3].substring(3);
                myLastSlash = currentPath.lastIndexOf('/');
                if (myLastSlash > 0) {currentPath = currentPath.substring(0, myLastSlash);} else if (myLastSlash == 0) {currentPath = "";} else {return null;}
            }
            myUrlParts[3] = currentPath + "/" + myUrlParts[3];
        }
        // Add host if not present
        if (myUrlParts[1] == null) {
            myUrlParts[1] = aBaseUrl[1];
        }
        // Add protocol if not present
        if (myUrlParts[0] == null) {
            myUrlParts[0] = aBaseUrl[0];
        }
        // Add port if not present
        if (myUrlParts[2] == null) {
            if (myUrlParts[1].equals(aBaseUrl[1]) && myUrlParts[0].equals(aBaseUrl[0])) {myUrlParts[2] = aBaseUrl[2];} else {myUrlParts[2] = myUrlParts[0].compareTo("http") == 0 ? "80" : "443";}
        }
        if (includeUrl(myUrlParts)) {
            return (myUrlParts[0] + "://" + myUrlParts[1] + ":" + myUrlParts[2] + myUrlParts[3]);
        }
        return null;
    }
    private static String cleanupDotsAndSlashes(String aPath) {
        if (aPath == null || aPath.isEmpty()) {return aPath;}
        String[] mySegments = aPath.split("/+");
        List<String> stack = new LinkedList<>();
        for (String mySegment : mySegments) {if (mySegment.equals("..")) {if (!stack.isEmpty() && !stack.get(0).equals("..")) {stack.remove(0);} else {stack.add(mySegment);}} else if (!mySegment.isEmpty() && !mySegment.equals(".")) {stack.add(mySegment);}}
        return String.join("/", stack);}

    //    public static void main(String[] args) throws Exception {
//        List<String> testUrls = List.of(
//                "#abc", "blah.html#test", "../blubb/123.html", "/one/two.html", "http://elsewhere.com/some.html");
//        String[] baseUrl = URLParser.parseURL("https://foo.com:8000/bar/xyz.html");
//
//        for (String url : normalizeAndFilterUrls(testUrls, baseUrl)) {
//            System.out.println(url);
//        }
//    }
}