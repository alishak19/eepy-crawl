package cis5550.webserver.datamodels;

public enum Header {
    CONTENT_LENGTH("Content-Length"),
    CONTENT_TYPE("Content-Type"),
    CONNECTION("Connection"),
    HOST("Host"),
    IF_MODIFIED_SINCE("If-Modified-Since"),
    RANGE("Range"),
    SERVER("Server"),
    DATE("Date"),
    LOCATION("Location"),
    COOKIE("Cookie"),
    SET_COOKIE("Set-Cookie"),
    SESSION_ID("SessionID"),
    ;

    private final String theHeaderString;

    Header(String aHeaderString) {
        theHeaderString = aHeaderString;
    }

    public String getHeaderString() {
        return theHeaderString;
    }

    public static Header fromString(String aString) {
        return switch (aString) {
            case "Content-Length" -> CONTENT_LENGTH;
            case "Content-Type" -> CONTENT_TYPE;
            case "Connection" -> CONNECTION;
            case "Host" -> HOST;
            case "If-Modified-Since" -> IF_MODIFIED_SINCE;
            case "Range" -> RANGE;
            case "Server" -> SERVER;
            case "Date" -> DATE;
            default -> null;
        };
    }
}
