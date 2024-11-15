package cis5550.webserver;

import cis5550.webserver.datamodels.RequestType;
import cis5550.webserver.routing.RouteFunction;
import cis5550.webserver.routing.RoutesContainer;
import cis5550.webserver.sessions.SessionsContainer;
import cis5550.webserver.utils.HostsContainer;

public class Server {
    public static final StaticFiles theStaticFiles = new StaticFiles();

    private static final RoutesContainer theRoutesContainer = new RoutesContainer();
    private static final SessionsContainer theSessionsContainer = new SessionsContainer();
    private static HostsContainer theHostsContainer;

    private static final int NUM_THREADS = 100;
    private static int thePort = 80;
    private static int theSecurePort = Integer.MIN_VALUE;
    private static ServerImpl theServer = null;
    private static ServerImpl theSecureServer = null;
    private static String theHost = null;

    public static void get(String aPath, Route aRoute) {
        startServerIfNotStarted();
        theRoutesContainer.addRoute(theHost, RequestType.GET, aPath, aRoute);
    }

    public static void post(String aPath, Route aRoute) {
        startServerIfNotStarted();
        theRoutesContainer.addRoute(theHost, RequestType.POST, aPath, aRoute);
    }

    public static void put(String aPath, Route aRoute) {
        startServerIfNotStarted();
        theRoutesContainer.addRoute(theHost, RequestType.PUT, aPath, aRoute);
    }

    public static void port(int aPort) {
        thePort = aPort;
    }

    public static void securePort(int aPort) {
        theSecurePort = aPort;
        theHostsContainer = new HostsContainer(aPort);
    }

    public static void host(String aHost) {
        theHost = aHost;
        theStaticFiles.host(aHost);
    }

    public static void host(String aHost, String aKeyStorePath, String aKeyStorePassword) {
        theHost = aHost;
        theHostsContainer.addHost(aHost, aKeyStorePath, aKeyStorePassword);
        theStaticFiles.host(aHost);
    }

    public static void before(RouteFunction aBefore) {
        theRoutesContainer.addBefore(theHost, aBefore);
    }

    public static void before(String aHost, RouteFunction aBefore) {
        theRoutesContainer.addBefore(aHost, aBefore);
    }

    public static void after(RouteFunction aAfter) {
        theRoutesContainer.addAfter(theHost, aAfter);
    }

    public static void after(String aHost, RouteFunction aAfter) {
        theRoutesContainer.addAfter(aHost, aAfter);
    }

    private static void startServerIfNotStarted() {
        if (!theSessionsContainer.hasStarted()) {
            new Thread(theSessionsContainer).start();
        }
        if (theServer == null) {
            theServer =
                    new ServerImpl(thePort, theStaticFiles, theRoutesContainer, theSessionsContainer, theHostsContainer, NUM_THREADS, false);
            new Thread(theServer).start();
        }
        if (theSecurePort != Integer.MIN_VALUE && theSecureServer == null) {
            theSecureServer = new ServerImpl(
                    theSecurePort, theStaticFiles, theRoutesContainer, theSessionsContainer, theHostsContainer ,NUM_THREADS, true);
            new Thread(theSecureServer).start();
        }
    }
}
