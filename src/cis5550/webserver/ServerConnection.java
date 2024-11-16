package cis5550.webserver;

import cis5550.tools.Logger;
import cis5550.webserver.parsers.HTTPRequestParser;
import cis5550.webserver.routing.RoutesContainer;
import cis5550.webserver.sessions.SessionsContainer;

import java.io.IOException;
import java.net.Socket;
import java.util.Objects;

public class ServerConnection implements Runnable {
    private static final Logger LOGGER = Logger.getLogger(ServerConnection.class);
    private Socket theSocket;
    private final StaticFiles theStaticFiles;
    private final RoutesContainer theRoutesContainer;
    private final SessionsContainer theSessionsContainer;
    private final ServerImpl theServer;
    private final HTTPRequestParser theHTTPRequestParser;

    public ServerConnection(
            Socket aSocket,
            RoutesContainer aRoutesContainer,
            StaticFiles aStaticFiles,
            SessionsContainer aSessionsContainer,
            ServerImpl aServer) {
        theSocket = aSocket;
        theStaticFiles = aStaticFiles;
        theRoutesContainer = aRoutesContainer;
        theSessionsContainer = aSessionsContainer;
        theServer = aServer;
        theHTTPRequestParser = new HTTPRequestParser(aServer);
    }

    @Override
    public void run() {
        LOGGER.info("Accepted connection from " + theSocket.getInetAddress());
        while (true) {
            try {
                RequestImpl myRequest = theHTTPRequestParser.parseRequest(theSocket, theSessionsContainer);

                if (Objects.isNull(myRequest)) {
                    LOGGER.info("Closing connection " + theSocket.getInetAddress());
                    break;
                }

                LOGGER.info("Received request " + myRequest);

                if (!HTTPRequestResponder.generateAndSendResponse(myRequest, theRoutesContainer, theStaticFiles, theSocket)) {
                    LOGGER.debug("Used write(): closing connection " + theSocket.getInetAddress());
                    break;
                }
            } catch (IOException e) {
                LOGGER.error("Failed to process request", e);
                break;
            }
        }
        try {
            theSocket.close();
        } catch (IOException e) {
            LOGGER.error("Failed to close socket", e);
        }
    }
}
