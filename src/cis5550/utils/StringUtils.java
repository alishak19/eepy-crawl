package cis5550.utils;

public class StringUtils {
    public static boolean isNullOrEmpty(String aString) {
        return aString == null || aString.isEmpty();
    }

    public static boolean isNullOrEmpty(byte[] aBytes) {
        return aBytes == null || aBytes.length == 0;
    }

    public static String escapeHtml(String aInput) {
        if (aInput == null) return "";
        return aInput.replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;")
                .replace("\"", "&quot;")
                .replace("'", "&#39;");
    }
}
