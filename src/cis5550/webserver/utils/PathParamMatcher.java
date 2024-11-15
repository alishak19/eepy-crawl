package cis5550.webserver.utils;

import java.util.HashMap;
import java.util.Map;

public class PathParamMatcher {
    public static Map<String, String> match(String aPath, String aRoute) {
        Map<String, String> myPathParams = new HashMap<>();
        String[] myPathParts = aPath.split("/");
        String[] myRouteParts = aRoute.split("/");
        if (myPathParts.length != myRouteParts.length) {
            return null;
        }
        for (int i = 0; i < myPathParts.length; i++) {
            if (myRouteParts[i].startsWith(":")) {
                myPathParams.put(myRouteParts[i].substring(1), myPathParts[i]);
            } else if (!myRouteParts[i].equals("*") && !myPathParts[i].equals(myRouteParts[i])) {
                return null;
            }
        }
        return myPathParams;
    }
}
