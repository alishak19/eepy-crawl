package cis5550.webserver.routing;

import cis5550.webserver.RequestImpl;
import cis5550.webserver.Route;
import cis5550.webserver.datamodels.*;

import java.util.*;
import java.util.stream.Collectors;

public class RoutesContainer {
    private final RouteEntryTable<RouteEntry> theRoutes;
    private final RouteEntryTable<RouteFunctionEntry> theBefores;
    private final RouteEntryTable<RouteFunctionEntry> theAfters;

    public RoutesContainer() {
        theRoutes = new RouteEntryTable<>();
        theBefores = new RouteEntryTable<>();
        theAfters = new RouteEntryTable<>();
    }

    public void addRoute(String aHost, RequestType aMethod, String aPath, Route aRoute) {
        RouteEntry myRouteEntry = new RouteEntry(aMethod, aPath, aRoute);
        theRoutes.addEntry(aHost, myRouteEntry);
    }

    public void addBefore(String aHost, RouteFunction aBefore) {
        RouteFunctionEntry myRouteFunctionEntry = new RouteFunctionEntry(aBefore);
        theBefores.addEntry(aHost, myRouteFunctionEntry);
    }

    public void addBefore(String aHost, String aPath, RouteFunction aBefore) {
        RouteFunctionEntry myRouteFunctionEntry = new RouteFunctionEntry(aPath, aBefore);
        theBefores.addEntry(aHost, myRouteFunctionEntry);
    }

    public void addAfter(String aHost, RouteFunction aAfter) {
        RouteFunctionEntry myRouteFunctionEntry = new RouteFunctionEntry(aAfter);
        theAfters.addEntry(aHost, myRouteFunctionEntry);
    }

    public void addAfter(String aHost, String aPath, RouteFunction aAfter) {
        RouteFunctionEntry myRouteFunctionEntry = new RouteFunctionEntry(aPath, aAfter);
        theAfters.addEntry(aHost, myRouteFunctionEntry);
    }

    public BeforeAfterRoute getRouteAndBuildParams(RequestImpl aRequest, String aPath) {
        String myHost = aRequest.getHost();

        RouteEntry myRouteEntry =
                theRoutes.getFirstMatchingEntry(aRequest);

        List<RouteFunctionEntry> myBeforeEntries =
                theBefores.getMatchingEntries(RequestType.fromString(aRequest.requestMethod()), aPath, myHost);

        List<RouteFunctionEntry> myAfterEntries =
                theAfters.getMatchingEntries(RequestType.fromString(aRequest.requestMethod()), aPath, myHost);

        List<RouteFunction> myBefores = myBeforeEntries.stream().map(RouteFunctionEntry::getRouteFunction).collect(Collectors.toList());
        List<RouteFunction> myAfters = myAfterEntries.stream().map(RouteFunctionEntry::getRouteFunction).collect(Collectors.toList());

        return new BeforeAfterRoute(myRouteEntry != null ? myRouteEntry.getRoute() : null, myBefores, myAfters);
    }
}
