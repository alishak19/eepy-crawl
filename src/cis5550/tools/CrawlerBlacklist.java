package cis5550.tools;

import cis5550.jobs.datamodels.TableColumns;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

public class CrawlerBlacklist {

    public static boolean isBlacklisted(String url, List<Pattern> blacklistPatterns) {
        for (Pattern pattern : blacklistPatterns) {
            if (pattern.matcher(url).matches()) {
                return true;
            }
        }
        return false;
    }

    public static List<Pattern> loadBlacklistPatterns(KVSClient client, String tableName) throws IOException {
        List<Pattern> patterns = new ArrayList<>();
        Iterator<Row> rows = client.scan(tableName);
        while (rows.hasNext()) {
            Row row = rows.next();
            String pattern = row.get(TableColumns.PATTERNS.value());
            if (pattern != null) {
                String regexPattern = pattern
                        .replace(".", "\\.")
                        .replace("*", ".*");
                patterns.add(Pattern.compile(regexPattern));
            }
        }
        return patterns;
    }
}
