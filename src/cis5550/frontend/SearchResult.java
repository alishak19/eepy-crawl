package cis5550.frontend;

public record SearchResult(String title, String url, String snippet, double score) {
    private static final String UNIQUE_SEPARATOR = "&#&";
    public String string() {
        return title + UNIQUE_SEPARATOR + (url.isEmpty() ? " " : url) + UNIQUE_SEPARATOR + (snippet.isEmpty() ? " " : snippet) + UNIQUE_SEPARATOR + score;
    }
    public static SearchResult fromString(String aString) {
        String[] myParts = aString.split(UNIQUE_SEPARATOR);
        return new SearchResult(myParts[0], myParts[1], myParts[2], Double.parseDouble(myParts[3]));
    }
}