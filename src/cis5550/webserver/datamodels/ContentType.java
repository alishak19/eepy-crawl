package cis5550.webserver.datamodels;

public enum ContentType {
    JPEG("image/jpeg"),
    PNG("image/png"),
    HTML("text/html"),
    CSS("text/css"),
    JS("application/javascript"),
    TXT("text/plain"),
    OCTET("application/octet-stream"),
    JSON("application/json"),
    FORM("application/x-www-form-urlencoded"),
    ;

    private final String theTypeString;

    ContentType(String aTypeString) {
        theTypeString = aTypeString;
    }

    public String getTypeString() {
        return theTypeString;
    }

    public static ContentType fromExtension(String aExtension) {
        return switch (aExtension) {
            case "jpeg", "jpg" -> JPEG;
            case "html" -> HTML;
            case "txt" -> TXT;
            default -> OCTET;
        };
    }
}
