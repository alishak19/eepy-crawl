package cis5550.frontend;

public record UrlInfo(String title, String snippet) {
    public String getTitle() {
        return title;
    }

    public String getSnippet() {
        return snippet;
    }
}
