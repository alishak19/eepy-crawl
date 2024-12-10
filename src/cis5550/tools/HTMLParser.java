package cis5550.tools;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HTMLParser {
    private static final Map<String, String> HTML_ENTITIES = new HashMap<>();

    static {
        HTML_ENTITIES.put("&lt;", "<");
        HTML_ENTITIES.put("&gt;", ">");
        HTML_ENTITIES.put("&amp;", "&");
        // HTML_ENTITIES.put("&quot;", "\"");
        HTML_ENTITIES.put("&apos;", "'");
        HTML_ENTITIES.put("&#39;", "'");
        HTML_ENTITIES.put("&nbsp;", " ");
        HTML_ENTITIES.put("&cent;", "¢");
        HTML_ENTITIES.put("&pound;", "£");
        HTML_ENTITIES.put("&yen;", "¥");
        HTML_ENTITIES.put("&euro;", "€");
        HTML_ENTITIES.put("&copy;", "©");
        HTML_ENTITIES.put("&reg;", "®");
        HTML_ENTITIES.put("&trade;", "™");
    }

    private static final Pattern DECIMAL_PATTERN = Pattern.compile("&#(\\d+);");
    private static final Pattern HEX_PATTERN = Pattern.compile("&#x([0-9a-fA-F]+);");

    public static String unescapeHtml(String input) {
        if (input == null || input.isEmpty()) {
            return input;
        }

        for (Map.Entry<String, String> entry : HTML_ENTITIES.entrySet()) {
            input = input.replace(entry.getKey(), entry.getValue());
        }

        Matcher decimalMatcher = DECIMAL_PATTERN.matcher(input);
        StringBuffer result = new StringBuffer();
        while (decimalMatcher.find()) {
            int codePoint = Integer.parseInt(decimalMatcher.group(1));
            decimalMatcher.appendReplacement(result, new String(Character.toChars(codePoint)));
        }
        decimalMatcher.appendTail(result);
        input = result.toString();

        Matcher hexMatcher = HEX_PATTERN.matcher(input);
        result = new StringBuffer();
        while (hexMatcher.find()) {
            int codePoint = Integer.parseInt(hexMatcher.group(1), 16);
            hexMatcher.appendReplacement(result, Matcher.quoteReplacement(new String(Character.toChars(codePoint))));
        }
        hexMatcher.appendTail(result);

        String res = result.toString();

        return res;
    }
}
