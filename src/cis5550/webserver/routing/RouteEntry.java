package cis5550.webserver.routing;

import cis5550.webserver.Route;
import cis5550.webserver.datamodels.RequestType;
import cis5550.webserver.utils.PathParamMatcher;

import java.util.Map;

public class RouteEntry implements PathMatchable {
    private final String thePath;
    private final Route theRoute;
    private final RequestType theRequestType;

    public RouteEntry(RequestType aRequestType, String aPath, Route aRoute) {
        theRequestType = aRequestType;
        thePath = aPath;
        theRoute = aRoute;
    }

    public RouteEntry() {
        theRequestType = null;
        thePath = null;
        theRoute = null;
    }

    public String getPath() {
        return thePath;
    }

    public Route getRoute() {
        return theRoute;
    }

    @Override
    public Map<String, String> match(RequestType aMethod, String aUrl) {
        if (thePath == null || theRequestType != aMethod) {
            return null;
        }
        return PathParamMatcher.match(aUrl, thePath);
    }

    public enum EntryType {
        ROUTE, BEFORE, AFTER
    }
}
