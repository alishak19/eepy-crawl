package cis5550.jobs.datamodels;

public enum TableName {
    CRAWL_TABLE("pt-crawl"),
    INDEX_TABLE("at-index"),
    PAGERANK_TABLE("pt-pagerank"),
    CACHE_TABLE("pt-cache"),
    ;

    private final String tableName;

    TableName(String aTableName) {
        tableName = aTableName;
    }

    public String getName() {
        return tableName;
    }

}
