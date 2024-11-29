package cis5550.flame;

public enum FlameOperation {
    FLATMAP("/rdd/flatMap"),
    MAP_TO_PAIR("/rdd/mapToPair"),
    FOLD_BY_KEY("/pairRDD/foldByKey"),
    GROUP_BY("/rdd/groupBy"),
    SAMPLE("/rdd/sample"),
    INTERSECTION("/rdd/intersection"),
    DISTINCT("/rdd/distinct"),
    FROM_TABLE("/rdd/fromTable"),
    FLATMAP_TO_PAIR("/rdd/flatMapToPair"),
    PAIR_FLATMAP("/pairRDD/flatMap"),
    PAIR_FLATMAP_TO_PAIR("/pairRDD/flatMapToPair"),
    JOIN("/pairRDD/join"),
    FOLD("/rdd/fold"),
    FILTER("/rdd/filter"),
    MAP_PARTITIONS("/rdd/mapPartitions"),
    COGROUP("/pairRDD/cogroup"),
    PAIR_FROM_TABLE("/pairRDD/pairFromTable"),
    PAIR_FOLD("/pairRDD/fold")
    ;

    private String thePath;

    FlameOperation(String aPath) {
        thePath = aPath;
    }

    public String getPath() {
        return thePath;
    }
}
