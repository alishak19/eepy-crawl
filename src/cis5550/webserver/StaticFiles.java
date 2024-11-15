package cis5550.webserver;

import java.util.HashMap;
import java.util.Map;

public class StaticFiles {
    private final Map<String, String> theRoots;

    private String theCurrentHost;
    private String theDefaultRoot;

    public StaticFiles() {
        theRoots = new HashMap<>();
    }

    public void location(String aPath) {
        if (theCurrentHost == null) {
            theDefaultRoot = aPath;
        } else {
            theRoots.put(theCurrentHost, aPath);
        }
    }

    public String getRoot(String aHost) {
        if (theRoots.containsKey(aHost)) {
            return theRoots.get(aHost);
        }
        return theDefaultRoot;
    }

    public String getFilePath(String aHost, String aPath) {
        return getRoot(aHost) + aPath;
    }

    public boolean isActive(String aHost) {
        return theRoots.containsKey(aHost) || theDefaultRoot != null;
    }

    public void host(String aHost) {
        theCurrentHost = aHost;
    }
}
