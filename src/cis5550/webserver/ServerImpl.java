package cis5550.webserver;

import cis5550.tools.Logger;
import cis5550.webserver.routing.RoutesContainer;
import cis5550.webserver.sessions.SessionsContainer;
import cis5550.webserver.utils.HostsContainer;

import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class ServerImpl implements Runnable {
    private static final Logger LOGGER = Logger.getLogger(ServerImpl.class);
    private static final String PWD = "secret";
    private static final String KEYSTORE = "keystore.jks";

    private int thePort;
    private ServerSocket theSocket;

    private final StaticFiles theStaticFiles;
    private final RoutesContainer theRoutesContainer;
    private final SessionsContainer theSessionsContainer;
    private final HostsContainer theHostsContainer;
    private final BlockingQueue<Runnable> theTaskQueue;
    private final ServerWorkerThread[] theWorkerThreads;
    private final boolean theSecure;
    private volatile boolean theIsStopped = false;

    public ServerImpl(
            int aPort,
            StaticFiles aStaticFiles,
            RoutesContainer aRoutesContainer,
            SessionsContainer aSessionsContainer,
            HostsContainer aHostsContainer,
            int aNumThreads,
            boolean aSecure) {
        thePort = aPort;
        theStaticFiles = aStaticFiles;
        theRoutesContainer = aRoutesContainer;
        theSessionsContainer = aSessionsContainer;
        theSecure = aSecure;

        theHostsContainer = aHostsContainer;

        theTaskQueue = new LinkedBlockingQueue<>();
        theWorkerThreads = new ServerWorkerThread[aNumThreads];

        try {
            LOGGER.info("Starting " + (theSecure ? "HTTPS" : "HTTP") + " server on port " + thePort);
            theSocket = new ServerSocket(thePort);
        } catch (Exception e) {
            LOGGER.error("Failed to create server socket", e);
        }
    }

    @Override
    public void run() {
        for (int i = 0; i < theWorkerThreads.length; i++) {
            theWorkerThreads[i] = new ServerWorkerThread(theTaskQueue);
            theWorkerThreads[i].start();
        }

        while (true) {
            try {
                Socket mySocket = theSecure ? theHostsContainer.getHostSocket(theSocket.accept()) : theSocket.accept();

                theTaskQueue.put(new ServerConnection(
                        mySocket, theRoutesContainer, theStaticFiles, theSessionsContainer, this));
            } catch (Exception e) {
                LOGGER.error("Failed to accept connection", e);
            }
        }
    }

    public synchronized void stopServer() {
        theIsStopped = true;
        for (ServerWorkerThread myWorkerThread : theWorkerThreads) {
            myWorkerThread.stopThread();
        }
    }

    public boolean isSecure() {
        return theSecure;
    }
}
