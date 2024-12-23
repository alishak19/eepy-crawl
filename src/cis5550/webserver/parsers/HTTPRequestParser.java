package cis5550.webserver.parsers;

import cis5550.tools.Logger;
import cis5550.webserver.ServerImpl;
import cis5550.webserver.datamodels.ContentType;
import cis5550.webserver.datamodels.Header;
import cis5550.webserver.RequestImpl;
import cis5550.webserver.datamodels.RequestType;
import cis5550.webserver.sessions.SessionsContainer;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class HTTPRequestParser {
    private static final Logger LOGGER = Logger.getLogger(HTTPRequestParser.class);
    private static final String ENCODING = "UTF-8";
    private static final byte[] END_SEQUENCE = new byte[]{13, 10, 13, 10};

    private final ServerImpl theServer;

    public HTTPRequestParser(ServerImpl aServer) {
        theServer = aServer;
    }

    public RequestImpl parseRequest(Socket aSocket, SessionsContainer aSessionsContainer) throws IOException {
        byte[] myRequestBytes = readBytes(aSocket.getInputStream());
        if (Objects.isNull(myRequestBytes)) {
            LOGGER.debug("Request is null");
            return null;
        }

        BufferedReader myBufferedReader =
                new BufferedReader(new InputStreamReader(new ByteArrayInputStream(myRequestBytes)));

        return buildRequestObject(myBufferedReader, aSocket, aSessionsContainer);
    }

    private byte[] readBytes(InputStream aInputStream) throws IOException {
        long myStartTime = System.nanoTime();
        ByteArrayOutputStream myByteArrayOutputStream = new ByteArrayOutputStream();

        byte myByte;
        int myEndSequenceCounter = 0;

        while ((myByte = (byte) aInputStream.read()) >= 0) {
            myByteArrayOutputStream.write(myByte);
            if (myByte == END_SEQUENCE[myEndSequenceCounter]) {
                myEndSequenceCounter++;
                if (myEndSequenceCounter == END_SEQUENCE.length) {
                    long myEndTime = System.nanoTime();
                    LOGGER.info("Time taken to read bytes: " + (myEndTime - myStartTime) + "ns");
                    return myByteArrayOutputStream.toByteArray();
                }
            } else {
                myEndSequenceCounter = (myByte == END_SEQUENCE[0]) ? 1 : 0;
            }
        }
        return null;
    }

    private RequestImpl buildRequestObject(
            BufferedReader aRequestReader, Socket aSocket, SessionsContainer aSessionsContainer) throws IOException {
        InputStream myInputStream = aSocket.getInputStream();
        String myFirstLine = aRequestReader.readLine();

        boolean myIsValid = true;
        RequestType myMethod = RequestType.INVALID;
        String myVersion = "";
        InetSocketAddress myRemoteAddress = (InetSocketAddress) aSocket.getRemoteSocketAddress();
        String myUrl = "";
        Map<String, String> myHeaders = new HashMap<>();
        Map<String, String> myQueryParams = new HashMap<>();
        Map<String, String> myParams = new HashMap<>();
        byte[] myBody = new byte[0];

        int myTypeIndex = myFirstLine.indexOf(" ");
        if (myTypeIndex != -1) {
            myMethod = RequestType.fromString(myFirstLine.substring(0, myTypeIndex));
        } else {
            LOGGER.error("Failed to parse method");
            myIsValid = false;
        }

        int myVersionIndex = myFirstLine.lastIndexOf(" ");
        if (myVersionIndex != -1 && myFirstLine.substring(myVersionIndex + 1).startsWith("HTTP/")) {
            myVersion = myFirstLine.substring(myVersionIndex + 1);
        } else {
            LOGGER.error("Failed to parse version");
            myIsValid = false;
        }

        int myPathIndex = myFirstLine.indexOf(" ", myTypeIndex + 1);
        if (myPathIndex != -1) {
            int myQueryIndex = myFirstLine.indexOf("?");
            if (myQueryIndex != -1) {
                myUrl = myFirstLine.substring(myTypeIndex + 1, myQueryIndex);
                String myQuery = myFirstLine.substring(myQueryIndex + 1, myPathIndex);
                if (!addQueryParamsEntries(myQueryParams, myQuery)) {
                    LOGGER.error("Failed to add query params");
                    myIsValid = false;
                }
            } else {
                myUrl = myFirstLine.substring(myTypeIndex + 1, myPathIndex);
            }
        } else {
            LOGGER.error("Failed to parse path");
            myIsValid = false;
        }

        String myCurrLine;
        while ((myCurrLine = aRequestReader.readLine()) != null && !myCurrLine.isEmpty()) {
            int myColonIndex = myCurrLine.indexOf(": ");
            if (myColonIndex != -1) {
                String myKey = myCurrLine.substring(0, myColonIndex);
                String myValue = myCurrLine.substring(myColonIndex + 2);
                if (myHeaders.containsKey(myKey)) {
                    myHeaders.put(myKey.toLowerCase(), myHeaders.get(myKey.toLowerCase()) + ", " + myValue);
                } else {
                    myHeaders.put(myKey.toLowerCase(), myValue);
                }
            } else {
                LOGGER.error("Failed to parse header");
                myIsValid = false;
            }
        }

        String myContentLength =
                myHeaders.get(Header.CONTENT_LENGTH.getHeaderString().toLowerCase());
        if (Objects.nonNull(myContentLength)) {
            int myContentLengthInt = Integer.parseInt(myContentLength);
            myBody = new byte[myContentLengthInt];
            int totalBytesRead = readInputStream(myInputStream, myContentLengthInt, myBody);
            if (totalBytesRead != myContentLengthInt) {
                LOGGER.debug("Failed to read body with content length " + myContentLength + " and read length " + totalBytesRead + "." + "Read content is " + new String(myBody));
                LOGGER.error("Failed to read body");
                myIsValid = false;
            }
        }

        String myContentType =
                myHeaders.get(Header.CONTENT_TYPE.getHeaderString().toLowerCase());
        if (Objects.nonNull(myContentType) && myContentType.equals(ContentType.FORM.getTypeString())) {
            String myBodyString = new String(myBody);
            if (!addQueryParamsEntries(myQueryParams, myBodyString)) {
                LOGGER.error("Failed to add query params from body");
                myIsValid = false;
            }
        }

        RequestImpl myResultRequest = new RequestImpl(
                myMethod.toString(),
                myUrl,
                myVersion,
                myHeaders,
                myQueryParams,
                myParams,
                myRemoteAddress,
                myBody,
                aSessionsContainer,
                theServer.isSecure());

        if (myResultRequest.headers(Header.COOKIE.getHeaderString()) != null) {
            String myCookieHeader = myResultRequest.headers(Header.COOKIE.getHeaderString());
            String[] myCookies = myCookieHeader.split("=");
            if (myCookies.length == 2 && myCookies[0].equals(Header.SESSION_ID.getHeaderString())) {
                myResultRequest.setSession(
                        aSessionsContainer.getSession(myCookies[1]));
            }
        }

        if (!myIsValid) {
            myResultRequest.setValid(false);
        }

        return myResultRequest;
    }

    private boolean addQueryParamsEntries(Map<String, String> aQueryParams, String aQuery) throws IOException {
        String[] myQueryParts = aQuery.split("&");
        for (String myQueryPart : myQueryParts) {
            String[] myQueryPair = myQueryPart.split("=");
            if (myQueryPair.length == 2) {
                aQueryParams.put(
                        URLDecoder.decode(myQueryPair[0], ENCODING), URLDecoder.decode(myQueryPair[1], ENCODING));
            } else if (myQueryPair.length == 1) {
                aQueryParams.put(URLDecoder.decode(myQueryPair[0], ENCODING), "");
            } else {
                LOGGER.error("Failed to parse query param: " + aQuery);
                return false;
            }
        }
        return true;
    }

    private int readInputStream(InputStream aInputStream, int aContentLength, byte[] aDest) throws IOException {
        byte[] myResBuffer = new byte[1024];
        int myTotalBytesRead = 0;
        while (myTotalBytesRead < aContentLength) {
            int myBytesRead = aInputStream.read(myResBuffer, 0, Math.min(myResBuffer.length, aContentLength - myTotalBytesRead));
            if (myBytesRead == -1) {
                throw new IOException("End of stream reached prematurely");
            }
            System.arraycopy(myResBuffer, 0, aDest, myTotalBytesRead, myBytesRead);
            myTotalBytesRead += myBytesRead;
            if (myBytesRead == 0) {
                LOGGER.error("Failed to read bytes");
                return myTotalBytesRead;
            }
        }
        return myTotalBytesRead;
    }
}
