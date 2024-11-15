package cis5550.webserver.routing;

import cis5550.webserver.RequestImpl;
import cis5550.webserver.ResponseImpl;
import cis5550.webserver.Route;

import java.util.LinkedList;
import java.util.List;

public class BeforeAfterRoute {
    private final Route theRoute;
    private final List<RouteFunction> theBefores;
    private final List<RouteFunction> theAfters;

    public BeforeAfterRoute(Route aRoute, List<RouteFunction> aBefores, List<RouteFunction> aAfters) {
        theRoute = aRoute;
        theBefores = aBefores;
        theAfters = aAfters;
    }

    public BeforeAfterRoute(Route aRoute) {
        theRoute = aRoute;
        theBefores = new LinkedList<>();
        theAfters = new LinkedList<>();
    }

    public boolean hasRoute() {
        return theRoute != null;
    }

    public Object handleRoute(RequestImpl aRequest, ResponseImpl aResponse) throws Exception {
        return theRoute.handle(aRequest, aResponse);
    }

    public boolean handleBefores(RequestImpl aRequest, ResponseImpl aResponse) throws Exception {
        for (RouteFunction myBefore : theBefores) {
            myBefore.handle(aRequest, aResponse);
            if (aResponse.hasHalted()) {
                return false;
            }
        }
        return true;
    }

    public void handleAfters(RequestImpl aRequest, ResponseImpl aResponse) throws Exception {
        for (RouteFunction myAfter : theAfters) {
            myAfter.handle(aRequest, aResponse);
        }
    }
}
