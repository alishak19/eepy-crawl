package cis5550.webserver.routing;

import cis5550.webserver.RequestImpl;
import cis5550.webserver.datamodels.RequestType;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class RouteEntryTable<T extends PathMatchable> {
    private final List<T> theDefaultEntries;
    private final Map<String, List<T>> theEntriesByHost;

    public RouteEntryTable() {
        theDefaultEntries = new LinkedList<>();
        theEntriesByHost = new HashMap<>();
    }

    public void addEntry(String aHost, T anEntry) {
        if (aHost == null) {
            theDefaultEntries.add(anEntry);
        } else {
            theEntriesByHost.computeIfAbsent(aHost, myK -> new LinkedList<>());
            theEntriesByHost.get(aHost).add(anEntry);
        }
    }

    public List<T> getMatchingEntries(RequestType aMethod, String aUrl, String aHost) {
        List<T> myEntriesToSearch;

        if (aHost == null || !theEntriesByHost.containsKey(aHost)) {
            myEntriesToSearch = theDefaultEntries;
        } else {
            myEntriesToSearch = theEntriesByHost.get(aHost);
        }

        List<T> myMatchingEntries = new LinkedList<>();

        if (myEntriesToSearch != null) {
            for (T myEntry : myEntriesToSearch) {
                if (myEntry.match(aMethod, aUrl) != null) {
                    myMatchingEntries.add(myEntry);
                }
            }
        }

        return myMatchingEntries;
    }

    public T getFirstMatchingEntry(RequestImpl aRequest) {
        List<T> myEntriesToSearch;
        String myHost = aRequest.getHost();
        RequestType myMethod = RequestType.fromString(aRequest.requestMethod());
        String myUrl = aRequest.url();

        if (myHost == null || !theEntriesByHost.containsKey(myHost)) {
            myEntriesToSearch = theDefaultEntries;
        } else {
            myEntriesToSearch = theEntriesByHost.get(myHost);
        }

        if (myEntriesToSearch != null) {
            for (T myEntry : myEntriesToSearch) {
                Map<String, String> myPathParams = myEntry.match(myMethod, myUrl);
                if (myPathParams != null) {
                    aRequest.setParams(myPathParams);
                    return myEntry;
                }
            }
        }
        return null;
    }

    public boolean containsHost(String aHost) {
        return theEntriesByHost.containsKey(aHost);
    }
}
