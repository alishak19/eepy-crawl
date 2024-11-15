package cis5550.webserver.datamodels;

public enum HTTPStatus {
    OK(200, "OK"),
    NOT_MODIFIED(304, "Not Modified"),
    MOVED_PERMANENTLY(301, "Moved Permanently"),
    FOUND(302, "Found"),
    SEE_OTHER(303, "See Other"),
    TEMPORARY_REDIRECT(307, "Temporary Redirect"),
    PERMANENT_REDIRECT(308, "Permanent Redirect"),
    BAD_REQUEST(400, "Bad Request"),
    FORBIDDEN(403, "Forbidden"),
    NOT_FOUND(404, "Not Found"),
    NOT_ALLOWED(405, "Method Not Allowed"),
    INVALID_RANGE(416, "Requested Range Not Satisfiable"),
    INTERNAL_SERVER_ERROR(500, "Internal Server Error"),
    NOT_IMPLEMENTED(501, "Not Implemented"),
    VERSION_NOT_SUPPORTED(505, "HTTP Version Not Supported"),
    NULL(0, "Null")
    ;

    private final int theCode;
    private final String theMessage;

    HTTPStatus(int aCode, String aMessage) {
        theCode = aCode;
        theMessage = aMessage;
    }

    public int getCode() {
        return theCode;
    }

    public String getMessage() {
        return theMessage;
    }

    public static HTTPStatus fromCode(int aCode) {
        return switch (aCode) {
            case 200 -> OK;
            case 304 -> NOT_MODIFIED;
            case 301 -> MOVED_PERMANENTLY;
            case 302 -> FOUND;
            case 303 -> SEE_OTHER;
            case 307 -> TEMPORARY_REDIRECT;
            case 308 -> PERMANENT_REDIRECT;
            case 400 -> BAD_REQUEST;
            case 403 -> FORBIDDEN;
            case 404 -> NOT_FOUND;
            case 405 -> NOT_ALLOWED;
            case 416 -> INVALID_RANGE;
            case 500 -> INTERNAL_SERVER_ERROR;
            case 501 -> NOT_IMPLEMENTED;
            case 505 -> VERSION_NOT_SUPPORTED;
            default -> NULL;
        };
    }
}
