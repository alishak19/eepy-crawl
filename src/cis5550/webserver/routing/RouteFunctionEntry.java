package cis5550.webserver.routing;

import cis5550.webserver.datamodels.RequestType;
import cis5550.webserver.utils.PathParamMatcher;

import java.util.HashMap;
import java.util.Map;

public class RouteFunctionEntry implements PathMatchable {
    private final String thePath;
    private final RouteFunction theRouteFunction;

    public RouteFunctionEntry(String aPath, RouteFunction aRoute) {
        thePath = aPath;
        theRouteFunction = aRoute;
    }

    public RouteFunctionEntry(RouteFunction aRoute) {
        thePath = null;
        theRouteFunction = aRoute;
    }

    public String getPath() {
        return thePath;
    }

    public RouteFunction getRouteFunction() {
        return theRouteFunction;
    }

    @Override
    public Map<String, String> match(RequestType aMethod, String aUrl) {
        if (thePath == null) {
            return new HashMap<>();
        }
        return PathParamMatcher.match(aUrl, thePath);
    }
}
