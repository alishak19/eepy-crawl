package cis5550.jobs.datamodels;

public enum TableColumns {
    URL("url"),
    PAGE("page"),
    RESPONSE_CODE("responseCode"),
    CONTENT_TYPE("contentType"),
    CONTENT_LENGTH("length"),
    TIMESTAMP("timestamp"),
    ROBOTS("robots"),
    CANONICAL_URL("canonicalUrl"),
    ANCHOR_PREFIX("anchors:"),
    RANK("rank"),
    ;

    private final String columnName;

    TableColumns(String aColumnName) {
        columnName = aColumnName;
    }

    public String value() {
        return columnName;
    }
}
