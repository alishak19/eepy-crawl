package cis5550.webserver.parsers;

import java.text.SimpleDateFormat;
import java.util.Locale;

public class DateParser {
    private static final SimpleDateFormat rfc1123Format = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss z", Locale.US);

    public static long parseDate(String aDateString) {
        try {
            return rfc1123Format.parse(aDateString).getTime();
        } catch (Exception e) {
            return -1;
        }
    }

    public static String formatDate(long aDate) {
        return rfc1123Format.format(aDate);
    }
}
