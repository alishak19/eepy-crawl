package cis5550.webserver;

import cis5550.webserver.datamodels.Header;
import cis5550.webserver.sessions.SessionsContainer;

import java.util.*;
import java.net.*;
import java.nio.charset.*;

import static cis5550.webserver.datamodels.Header.SESSION_ID;

// Provided as part of the framework code

public class RequestImpl implements Request {
    String method;
    String url;
    String protocol;
    InetSocketAddress remoteAddr;
    Map<String, String> headers;
    Map<String, String> queryParams;
    Map<String, String> params;
    byte bodyRaw[];
    boolean isValid = true;
    boolean hasCalledSession = false;
    SessionImpl session;
    ResponseImpl theResponse;
    boolean isHTTPS = false;

    private final SessionsContainer theSessionsContainer;

    //  Server server;

    public RequestImpl(
            String methodArg,
            String urlArg,
            String protocolArg,
            Map<String, String> headersArg,
            Map<String, String> queryParamsArg,
            Map<String, String> paramsArg,
            InetSocketAddress remoteAddrArg,
            byte bodyRawArg[],
            SessionsContainer aSessionsContainer,
            boolean isHTTPSArg) {
        method = methodArg;
        url = urlArg;
        remoteAddr = remoteAddrArg;
        protocol = protocolArg;
        headers = headersArg;
        queryParams = queryParamsArg;
        params = paramsArg;
        bodyRaw = bodyRawArg;
        theSessionsContainer = aSessionsContainer;
        isHTTPS = isHTTPSArg;
        //    server = serverArg;
    }

    public String requestMethod() {
        return method;
    }

    public void setParams(Map<String, String> paramsArg) {
        params = paramsArg;
    }

    public int port() {
        return remoteAddr.getPort();
    }

    public String url() {
        return url;
    }

    public String protocol() {
        return protocol;
    }

    public String contentType() {
        return headers.get("content-type");
    }

    public String ip() {
        return remoteAddr.getAddress().getHostAddress();
    }

    public String body() {
        return new String(bodyRaw, StandardCharsets.UTF_8);
    }

    public byte[] bodyAsBytes() {
        return bodyRaw;
    }

    public int contentLength() {
        return bodyRaw.length;
    }

    public String headers(String name) {
        return headers.get(name.toLowerCase());
    }

    public Set<String> headers() {
        return headers.keySet();
    }

    public String queryParams(String param) {
        return queryParams.get(param);
    }

    public Set<String> queryParams() {
        return queryParams.keySet();
    }

    public String params(String param) {
        return params.get(param);
    }

    public void setSession(SessionImpl aSession) {
        session = aSession;
        if (session != null) {
            hasCalledSession = true;
        }
    }

    @Override
    public Session session() {
        if (session == null) {
            session = theSessionsContainer.createSession();
        }
        if (!hasCalledSession && theResponse != null) {
            theResponse.header(
                    Header.SET_COOKIE.getHeaderString(),
                    SESSION_ID.getHeaderString() + "=" + session.id() + "; SameSite=Strict; HttpOnly"
                            + (isHTTPS ? "; Secure" : ""));
        }
        hasCalledSession = true;
        return session;
    }

    public Map<String, String> params() {
        return params;
    }

    public String getHost() {

        return headers.get(Header.HOST.getHeaderString().toLowerCase()).split(":")[0];
    }

    public boolean isValid() {
        return isValid;
    }

    public void setValid(boolean isValidArg) {
        isValid = isValidArg;
    }

    public boolean hasCalledSession() {
        return hasCalledSession;
    }

    public void setResponse(ResponseImpl aResponse) {
        theResponse = aResponse;
    }

    @Override
    public String toString() {
        return "RequestImpl{" + "method='"
                + method + '\'' + ", url='"
                + url + '\'' + ", protocol='"
                + protocol + '\'' + ", remoteAddr="
                + remoteAddr + ", headers="
                + headers + ", queryParams="
                + queryParams + ", params="
                + params + ", bodyRaw="
                + Arrays.toString(bodyRaw) + '}';
    }
}
