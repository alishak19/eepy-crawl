package cis5550.tools;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class URLHelper {

    private static final Logger LOGGER = Logger.getLogger(URLHelper.class);

    public static Map<String, List<String>> extractUrlsAndAnchors(byte[] contentBytes) {
        Map<String, List<String>> urlsAndAnchors = new HashMap<>();

        String pageContent = new String(contentBytes, StandardCharsets.UTF_8);
        Pattern pattern = Pattern.compile("<a\\s+[^>]*?href=[\"']?([^\"'\\s>]+)[\"']?[^>]*>(.*?)</a>", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
        Matcher matcher = pattern.matcher(pageContent);

        while (matcher.find()) {
            String url = matcher.group(1);
            String anchorText = matcher.group(2) != null ? matcher.group(2).trim() : "";

            if (!url.isEmpty()) {
                urlsAndAnchors.computeIfAbsent(url, k -> new ArrayList<>()).add(anchorText);
            }
        }

        return urlsAndAnchors;
    }

    public static List<String> extractUrls(byte[] contentBytes) {
        List<String> urls = new ArrayList<>();

        String pageContent;
        pageContent = new String(contentBytes, StandardCharsets.UTF_8);

        Pattern pattern = Pattern.compile("<a\\s+(?:[^>]*?\\s+)?href=[\"']?([^\"'\\s>]+)", Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(pageContent);

        while (matcher.find()) {
            String url = matcher.group(1);
            urls.add(url);
        }

        return urls;
    }

    public static String normalizeURL(String baseURL, String href) {
        try {
            boolean startsWith = href.startsWith("#");
            if (href.contains("#")) {
                href = href.substring(0, href.indexOf("#"));
            }

            String[] baseParts = URLParser.parseURL(baseURL);
            String baseProtocol = baseParts[0];
            String baseHost = baseParts[1];
            String basePort = baseParts[2] != null ? baseParts[2] : (baseProtocol.equals("https") ? "443" : "80");
            String basePath = baseParts[3] != null ? baseParts[3] : "/";

            String[] hrefParts = URLParser.parseURL(href);
            String hrefProtocol = hrefParts[0] != null ? hrefParts[0] : baseProtocol;
            String hrefHost = hrefParts[1] != null ? hrefParts[1] : baseHost;

            String hrefPort;
            if (hrefParts[2] != null) {
                hrefPort = hrefParts[2];
            } else if (hrefParts[0] != null) {
                hrefPort = hrefProtocol.equals("https") ? "443" : "80";
            } else {
                hrefPort = basePort;
            }

            String hrefPath;
            if (startsWith) {
                hrefPath = basePath;
            } else {
                hrefPath = hrefParts[3] != null ? hrefParts[3] : "/";
                hrefPath = hrefPath.replaceAll("/{2,}", "/");
                if (!hrefPath.startsWith("/") && !startsWith) {
                    if (!basePath.contains(".html")) {
                        hrefPath = resolveRelativePath(basePath, hrefPath);
                    } else {
                        String parentPath = basePath.endsWith("/") ? basePath : basePath.substring(0, basePath.lastIndexOf("/") + 1);
                        hrefPath = resolveRelativePath(parentPath, hrefPath);
                    }
                }
            }

            String normalizedURL = hrefProtocol + "://" + hrefHost + ":" + hrefPort + hrefPath;

            if (!hrefProtocol.equals("http") && !hrefProtocol.equals("https")) {
                return null;
            }
            if (normalizedURL.endsWith(".jpg") || normalizedURL.endsWith(".jpeg") || normalizedURL.endsWith(".png") ||
                    normalizedURL.endsWith(".gif") || normalizedURL.endsWith(".txt")
            ) {
                return null;
            }

            return normalizedURL;
        } catch (Exception e) {
            LOGGER.error(e.getMessage());
            return null;
        }
    }

    private static String resolveRelativePath(String basePath, String relativePath) {
        String[] baseSegments = basePath.split("/");
        String[] relativeSegments = relativePath.split("/");

        List<String> pathSegments = new ArrayList<>(Arrays.asList(baseSegments));

        for (String segment : relativeSegments) {
            if (segment.equals("..")) {
                if (!pathSegments.isEmpty()) {
                    pathSegments.remove(pathSegments.size() - 1);
                }
            } else if (!segment.equals(".")) {
                pathSegments.add(segment);
            }
        }

        String normalizedPath = String.join("/", pathSegments);

        if (!normalizedPath.startsWith("/")) {
            normalizedPath = "/" + normalizedPath;
        }

        return normalizedPath;
    }
}
