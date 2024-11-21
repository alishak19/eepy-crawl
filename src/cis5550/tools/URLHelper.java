package cis5550.tools;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class URLHelper {

    private static final Logger LOGGER = Logger.getLogger(URLHelper.class);
	private static final HashSet<String> IGNORE_EXTENSIONS = new HashSet<>(Arrays.asList("jpg", "jpeg", "gif", "txt", "png"));

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
    
    public static List<String> extractURL(String page) {
		List<String> tags = new ArrayList<>();
		
		boolean inTag = false;
		String currTag = "";
		for (int i = 0; i < page.length(); i++) {
			if (page.charAt(i) == '<' && page.charAt(i + 1) != '/') {
				inTag = true;
			}
			if (page.charAt(i) == '>') {
				inTag = false;
				tags.add(currTag);
				currTag = "";
			}
			if (inTag && page.charAt(i) != '<') {
				currTag += page.charAt(i);
			}
		}
		
		List<String> urls = new ArrayList<>();
		
		for (String tag : tags) {
			String[] vals = tag.split(" ");
			List<String> valList = Arrays.asList(vals);
			
			if (vals[0].equals("a") || vals[0].equals("A")) {
				for (String s : valList) {
					if (s.length() >= 4 && (s.substring(0, 4).equals("href") || s.substring(0, 4).equals("HREF"))) {
						String[] link = s.split("=");
						urls.add(link[1].substring(1, link[1].length() - 1));
					}
				}
			}
		}
		
		return urls;
	}
	
	public static String normalizeFilter(String base, String url) {
		int indexHash = url.indexOf('#');
		if (indexHash != -1) {
			url = url.substring(0, indexHash);
		}
		
		if (url.length() == 0) {
			return base;
		}
		
		String[] parsed = URLParser.parseURL(url);
		String[] parsedBase = URLParser.parseURL(base);
		
		String finalUrl = "";
		
		if (parsed[0] != null) {
			// not relative url
			if (parsed[2] == null) {
				int portNum = -1;
				if (parsed[0].contains("https")) {
					portNum = 443;
				} else {
					portNum = 80;
				}
				parsed[1] += ":" + portNum;
				finalUrl = parsed[0] + "://" + parsed[1];
			} else {
				finalUrl = parsed[0] + "://" + parsed[1] + ":" + parsed[2] + "/";
			}
			
			if (parsed[3] != null) {
				String path = removeDots(parsed[3]);
				finalUrl += path;
			}
			
		} else {
			// relative url
			if (parsed[1] != null) {
				if (parsed[2] == null) {
					int portNum = -1;
					if (parsed[0].contains("https")) {
						portNum = 443;
					} else {
						portNum = 80;
					}
					parsed[2] = portNum + "";
				}
				finalUrl = parsedBase[0] + "://" + parsed[1] + ":" + parsed[2] + "/";
				String basePath = parsedBase[3];
				basePath = basePath.substring(0, basePath.lastIndexOf("/"));
				basePath += parsed[3];
				
				String path = removeDots(basePath);
				finalUrl += path;
			} else {
				if (parsed[3].equals("") || parsedBase[3].equals("/")) {
					if (url.charAt(0) == '/' && base.charAt(base.length() - 1) == '/') {
						url = url.substring(1);
					}
					// System.out.println("it's me hi");
					return base + url;
				} else {
					if (parsed[3].charAt(0) == '/') {
						if (parsedBase[2] == null) {
							int portNum = -1;
							if (parsedBase[0].contains("https")) {
								portNum = 443;
							} else {
								portNum = 80;
							}
							parsedBase[2] = portNum + "";
						}
						finalUrl = parsedBase[0] + "://" + parsedBase[1] + ":" + parsedBase[2] + "/";
						String path = removeDots(parsed[3]);
						
						finalUrl += path;
					} else {
						if (parsedBase[2] == null) {
							int portNum = -1;
							if (parsedBase[0].contains("https")) {
								portNum = 443;
							} else {
								portNum = 80;
							}
							parsedBase[2] = portNum + "";
						}
						finalUrl = parsedBase[0] + "://" + parsedBase[1] + ":" + parsedBase[2] + "/";
						
						String path = "";
						if (parsedBase[1] != null && parsedBase[3] == null) {
							path = "/" + parsed[3];
						} else {
							String basePath = parsedBase[3];
							basePath = basePath.substring(0, basePath.lastIndexOf("/"));
							
							basePath += parsed[3];
							path = basePath;
						}
						
						String finPath = removeDots(path);
						finalUrl += finPath;
					}
				}
			}
			
		}
		
		if (filterHelper(finalUrl)) {
			return finalUrl;
		} else {
			return ""; 
		}
	}
	
	public static boolean filterHelper(String link) {
		if (!link.substring(0, 5).equals("https") && !link.substring(0, 4).equals("http")) {
			return false;
		}
		
		String[] sepLink = link.split("\\.");
		if (IGNORE_EXTENSIONS.contains(sepLink[sepLink.length - 1])) {
			return false;
		}
		
		return true;
	}
	
	public static int checkRules(String url, String rule) {
		int ans = 0;
		
		// ans = 1 = works, ans = -1 = fails
		
		if (rule.contains("Disallow")) {
			// disallow
			String rulePattern = rule.substring(10);
			String[] subUrlParse = URLParser.parseURL(url);
			if (subUrlParse[3] != null) {
				if (subUrlParse[3].length() >= rulePattern.length()) {
					String subUrl = subUrlParse[3].substring(0, rulePattern.length());
					if (subUrl.equals(rulePattern)) {
						ans = -1;
					}
				}
			}
		} else {
			// allow
			String rulePattern = rule.substring(7);
			String[] subUrlParse = URLParser.parseURL(url);
			if (subUrlParse[3] != null) {
				if (subUrlParse[3].length() >= rulePattern.length()) {
					String subUrl = subUrlParse[3].substring(0, rulePattern.length());
					if (subUrl.equals(rulePattern)) {
						ans = 1;
					}
				}
			}
			
		}
		return ans;
	}
	
	public static boolean ruleChecker(List<String> rules, String url) {
		if (rules.contains("User-agent: cis5550-crawler") && rules.indexOf("User-agent: *") - 2 != rules.indexOf("User-agent: cis5550-crawler")) {
			int lastIndex = rules.indexOf("User-agent: *");
			
			for (int i = 0; i < lastIndex; i++) {
				String rule = rules.get(i);
				int tCheck = checkRules(url, rule);
				if (tCheck != 0) {
					return tCheck == 1;
				}
			}
			
		} else {
			int startIndex = rules.indexOf("User-agent: *");
			
			for (int i = startIndex; i < rules.size(); i++) {
				String rule = rules.get(i);
				int tCheck = checkRules(url, rule);
				if (tCheck != 0) {
					return tCheck == 1;
				}
			}
		}
		return true;
	}
	
	public static String removeDots(String basePath) {
		String path = "";
		
		String[] baseRel = basePath.split("/");
		for (String s : baseRel) {
			if (s.equals("..")) {
				path = path.substring(0, path.lastIndexOf("/"));
			} else if (s.equals(".")) {
				path += "";
			} else {
				if (path.equals("")) {
					path += s;
				} else {
					path += "/" + s;
				}
				
			}
		}
		return path;
	}
}
