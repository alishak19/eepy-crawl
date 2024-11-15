package cis5550.webserver;

import cis5550.webserver.datamodels.HTTPStatus;
import cis5550.webserver.datamodels.Header;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class ResponseImpl implements Response {
    private int theStatusCode;
    private String theReasonPhrase;
    private byte[] theBody;
    private final Map<String, List<String>> theHeaders;
    private final Socket theSocket;

    private boolean theHasWritten = false;
    private boolean theHasRedirected = false;
    private boolean theHasHalted = false;

    public ResponseImpl() {
        theHeaders = new HashMap<>();
        theSocket = null;
    }

    public ResponseImpl(Socket aSocket) {
        theHeaders = new HashMap<>();
        theSocket = aSocket;
    }

    @Override
    public void body(String body) {
        theBody = body.getBytes();
    }

    public void clearBody() {
        theBody = null;
    }

    @Override
    public void bodyAsBytes(byte[] bodyArg) {
        theBody = bodyArg;
    }

    @Override
    public void header(String name, String value) {
        if (!theHasWritten) {
            theHeaders.computeIfAbsent(name, myK -> new LinkedList<>());
            theHeaders.get(name).add(value);
        }
    }

    public void errorStatus(HTTPStatus aStatus) {
        theStatusCode = aStatus.getCode();
        theReasonPhrase = aStatus.getMessage();
        clearBody();
        header(Header.CONTENT_LENGTH.getHeaderString(), "0");
    }

    @Override
    public void type(String contentType) {
        theHeaders.get(Header.CONTENT_TYPE.getHeaderString()).clear();
        header(Header.CONTENT_TYPE.getHeaderString(), contentType);
    }

    @Override
    public void status(int statusCode, String reasonPhrase) {
        theStatusCode = statusCode;
        theReasonPhrase = reasonPhrase;
    }

    @Override
    public void write(byte[] b) throws Exception {
        if (theSocket == null) {
            throw new Exception("Socket is null");
        }
        OutputStream myOutputStream = theSocket.getOutputStream();

        if (!theHasWritten) {
            theHasWritten = true;
            header(Header.CONNECTION.getHeaderString(), "close");
            HTTPRequestResponder.sendHeaders(this, theSocket);
        }
        myOutputStream.write(b);
        myOutputStream.flush();
    }

    @Override
    public void redirect(String url, int responseCode) {
        if (theHasWritten) {
            throw new RuntimeException("Cannot redirect after writing to the response");
        }
        theHasRedirected = true;
        clearBody();
        status(responseCode, HTTPStatus.fromCode(responseCode).getMessage());
        header(Header.LOCATION.getHeaderString(), url);
        header(Header.CONTENT_LENGTH.getHeaderString(), "0");
        try {
            HTTPRequestResponder.sendHeaders(this, theSocket);
        } catch (IOException e) {
        }
    }

    @Override
    public void halt(int statusCode, String reasonPhrase) {
        theHasHalted = true;
        status(statusCode, reasonPhrase);
        clearBody();
        header(Header.CONTENT_LENGTH.getHeaderString(), "0");
    }

    public int getStatusCode() {
        return theStatusCode;
    }

    public String getReasonPhrase() {
        return theReasonPhrase;
    }

    public String getContentType() {
        return getFirstHeader(Header.CONTENT_TYPE.getHeaderString());
    }

    public byte[] getBody() {
        return theBody;
    }

    public Map<String, List<String>> getHeaders() {
        return theHeaders;
    }

    public List<String> getHeader(String aKey) {
        return theHeaders.get(aKey);
    }

    public String getFirstHeader(String aKey) {
        List<String> myHeader = theHeaders.get(aKey);
        if (myHeader == null || myHeader.isEmpty()) {
            return null;
        }
        return myHeader.get(0);
    }

    public boolean hasWritten() {
        return theHasWritten;
    }

    public boolean hasRedirected() {
        return theHasRedirected;
    }

    public boolean hasHalted() {
        return theHasHalted;
    }

    @Override
    public String toString() {
        return "ResponseImpl{" +
                "statusCode=" + theStatusCode +
                ", reasonPhrase='" + theReasonPhrase + '\'' +
                ", body='" + (Objects.isNull(theBody) ? null : new String(theBody, StandardCharsets.UTF_8)) + "'" +
                ", headers=" + theHeaders +
                '}';
    }
}
