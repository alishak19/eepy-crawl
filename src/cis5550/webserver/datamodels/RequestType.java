package cis5550.webserver.datamodels;

public enum RequestType {
    GET, POST, PUT, DELETE, HEAD, OPTIONS, TRACE, CONNECT, INVALID;

    public static RequestType fromString(String aString) {
        return switch (aString) {
            case "GET" -> GET;
            case "POST" -> POST;
            case "PUT" -> PUT;
            case "DELETE" -> DELETE;
            case "HEAD" -> HEAD;
            case "OPTIONS" -> OPTIONS;
            case "TRACE" -> TRACE;
            case "CONNECT" -> CONNECT;
            default -> INVALID;
        };
    }
}
