package cis5550.frontend;

public record SearchResult(String title, String url, String snippet) {
    private static final String UNIQUE_SEPARATOR = "%*&^%*^";
    public String string() {
        return title + UNIQUE_SEPARATOR + url + UNIQUE_SEPARATOR + snippet;
    }
    public static SearchResult fromString(String aString) {
        String[] myParts = aString.split(UNIQUE_SEPARATOR);
        return new SearchResult(myParts[0], myParts[1], myParts[2]);
    }
}