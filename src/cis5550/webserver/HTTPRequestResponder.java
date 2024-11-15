package cis5550.webserver;

import cis5550.tools.Logger;
import cis5550.webserver.datamodels.*;
import cis5550.webserver.parsers.DateParser;
import cis5550.webserver.parsers.RangeParser;
import cis5550.webserver.routing.BeforeAfterRoute;
import cis5550.webserver.utils.FileReader;
import cis5550.webserver.utils.RangeSplicer;
import cis5550.webserver.routing.RoutesContainer;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Date;
import java.util.Objects;

public class HTTPRequestResponder {
    private static final Logger LOGGER = Logger.getLogger(HTTPRequestResponder.class);
    private static final String HTTP_VERSION = "HTTP/1.1";
    private static final String SERVER_NAME = "CIS5550-WebServer";

    public static boolean generateResponse(
            RequestImpl aRequest, RoutesContainer aRoutesContainer, StaticFiles aStaticFiles, Socket aSocket) throws IOException {
        boolean myKeepAlive = true;

        ResponseImpl myResponse = new ResponseImpl(aSocket);
        aRequest.setResponse(myResponse);
        myResponse.header(Header.SERVER.getHeaderString(), SERVER_NAME);
        myResponse.header(Header.DATE.getHeaderString(), DateParser.formatDate(new Date().getTime()));

        if (!aRequest.isValid()) {
            myResponse.errorStatus(HTTPStatus.BAD_REQUEST);
            sendResponse(myResponse, aSocket);
        } else if (Objects.isNull(aRequest.requestMethod())
                || (RequestType.fromString(aRequest.requestMethod()) == RequestType.INVALID)) {
            myResponse.errorStatus(HTTPStatus.NOT_IMPLEMENTED);
            sendResponse(myResponse, aSocket);
        } else if (Objects.isNull(aRequest.protocol()) || (aRequest.protocol().compareTo(HTTP_VERSION) != 0)) {
            myResponse.errorStatus(HTTPStatus.VERSION_NOT_SUPPORTED);
            sendResponse(myResponse, aSocket);
        } else if (Objects.isNull(aRequest.headers(Header.HOST.getHeaderString()))) {
            myResponse.errorStatus(HTTPStatus.BAD_REQUEST);
            sendResponse(myResponse, aSocket);
        } else {
            BeforeAfterRoute myBeforeAfterRoute = aRoutesContainer.getRouteAndBuildParams(aRequest, aRequest.url());
            boolean myNotHalted = true;

            try {
                myNotHalted = myBeforeAfterRoute.handleBefores(aRequest, myResponse);
            } catch (Exception e) {
                LOGGER.error("Error while handling request befores", e);
                myResponse.errorStatus(HTTPStatus.INTERNAL_SERVER_ERROR);
                myResponse.clearBody();
                return myKeepAlive;
            }

            if (myBeforeAfterRoute.hasRoute()) {
                try {
                    if (!myNotHalted) {
                        sendResponse(myResponse, aSocket);
                        return myKeepAlive;
                    }

                    myResponse.status(HTTPStatus.OK.getCode(), HTTPStatus.OK.getMessage());
                    myResponse.header(Header.CONTENT_TYPE.getHeaderString(), ContentType.HTML.getTypeString());

                    Object myRouteResult = myBeforeAfterRoute.handleRoute(aRequest, myResponse);

                    if (!myResponse.hasWritten() && !myResponse.hasRedirected()) {
                        if (myRouteResult != null) {
                            myResponse.body(myRouteResult.toString());
                            myResponse.header(
                                    Header.CONTENT_LENGTH.getHeaderString(),
                                    String.valueOf(myResponse.getBody().length));
                        } else if (myResponse.getBody() != null) {
                            myResponse.header(
                                    Header.CONTENT_LENGTH.getHeaderString(),
                                    String.valueOf(myResponse.getBody().length));
                        } else {
                            myResponse.header(Header.CONTENT_LENGTH.getHeaderString(), "0");
                        }
                    }

                    myBeforeAfterRoute.handleAfters(aRequest, myResponse);
                } catch (Exception e) {
                    LOGGER.error("Error while handling request", e);
                    myResponse.errorStatus(HTTPStatus.INTERNAL_SERVER_ERROR);
                    myResponse.clearBody();
                } finally {
                    if (!myResponse.hasWritten() && !myResponse.hasRedirected()) {
                        sendResponse(myResponse, aSocket);
                    } else if (!myResponse.hasRedirected()) {
                        myKeepAlive = false;
                    }
                }
            } else if ((RequestType.fromString(aRequest.requestMethod()) == RequestType.GET
                            || RequestType.fromString(aRequest.requestMethod()) == RequestType.HEAD)
                    && aStaticFiles.isActive(aRequest.getHost())) {

                myResponse = generateResponseForFile(
                        aStaticFiles.getFilePath(aRequest.getHost(), aRequest.url()), aRequest, aSocket);
                try {
                    myBeforeAfterRoute.handleAfters(aRequest, myResponse);
                } catch (Exception e) {
                    LOGGER.error("Error while handling request afters", e);
                }
                sendResponse(myResponse, aSocket);
            } else {
                try {
                    myBeforeAfterRoute.handleAfters(aRequest, myResponse);
                } catch (Exception e) {
                    LOGGER.info("Error while handling request afters", e);
                }
                myResponse.errorStatus(HTTPStatus.NOT_FOUND);
                sendResponse(myResponse, aSocket);
            }
        }
        return myKeepAlive;
    }

    public static ResponseImpl generateResponseForFile(String aPath, Request aRequest, Socket aSocket)
            throws IOException {
        ResponseImpl myResponse = new ResponseImpl();
        File myFile = new File(aPath);
        if (aPath.contains("..")) {
            myResponse.errorStatus(HTTPStatus.FORBIDDEN);
        } else if (!myFile.exists() || myFile.isDirectory()) {
            myResponse.errorStatus(HTTPStatus.NOT_FOUND);
        } else if (!Objects.isNull(aRequest.headers(Header.IF_MODIFIED_SINCE.getHeaderString()))
                && myFile.lastModified()
                        <= DateParser.parseDate(aRequest.headers(Header.IF_MODIFIED_SINCE.getHeaderString()))) {
            myResponse.errorStatus(HTTPStatus.NOT_MODIFIED);
        } else {
            String aExtension = aPath.substring(aPath.lastIndexOf(".") + 1);

            myResponse.status(HTTPStatus.OK.getCode(), HTTPStatus.OK.getMessage());
            byte[] myBody = FileReader.readFile(aPath);
            if (!Objects.isNull(myBody) && aRequest.requestMethod().compareTo(RequestType.GET.toString()) == 0) {
                Range myRange = RangeParser.parseRange(aRequest.headers(Header.RANGE.getHeaderString()));

                if (!Objects.isNull(myRange) && !RangeParser.isValidRange(myRange, myBody.length)) {
                    myResponse.errorStatus(HTTPStatus.INVALID_RANGE);
                } else {
                    myBody = RangeSplicer.getRange(myRange, myBody);
                    myResponse.header(Header.CONTENT_LENGTH.getHeaderString(), String.valueOf(myBody.length));
                    myResponse.header(
                            Header.CONTENT_TYPE.getHeaderString(),
                            ContentType.fromExtension(aExtension).getTypeString());
                    myResponse.bodyAsBytes(myBody);
                }
            } else if (Objects.isNull(myBody)) {
                myResponse.errorStatus(HTTPStatus.FORBIDDEN);
            } else if (aRequest.requestMethod().compareTo(RequestType.HEAD.toString()) == 0) {
                myResponse.header(Header.CONTENT_LENGTH.getHeaderString(), String.valueOf(myBody.length));
                myResponse.header(
                        Header.CONTENT_TYPE.getHeaderString(),
                        ContentType.fromExtension(aExtension).getTypeString());
            }
        }
        return myResponse;
    }

    public static void sendResponse(ResponseImpl aResponse, Socket aSocket) throws IOException {
        LOGGER.info("Sending response " + aResponse.toString());
        OutputStream myOutputStream = aSocket.getOutputStream();
        sendHeaders(aResponse, aSocket);
        if (aResponse.getBody() != null) {
            myOutputStream.write(aResponse.getBody());
        }
        myOutputStream.flush();
    }

    public static void sendHeaders(ResponseImpl aResponse, Socket aSocket) throws IOException {
        if (aSocket == null) {
            throw new IOException("Socket is null");
        }
        LOGGER.info("Sending headers for response " + aResponse.toString());
        OutputStream myOutputStream = aSocket.getOutputStream();
        myOutputStream.write(
                (HTTP_VERSION + " " + aResponse.getStatusCode() + " " + aResponse.getReasonPhrase() + "\r\n")
                        .getBytes());
        for (String myKey : aResponse.getHeaders().keySet()) {
            for (String myValue : aResponse.getHeader(myKey)) {
                myOutputStream.write((myKey + ": " + myValue + "\r\n").getBytes());
            }
        }
        myOutputStream.write("\r\n".getBytes());
        myOutputStream.flush();
    }
}
