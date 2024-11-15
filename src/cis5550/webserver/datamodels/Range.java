package cis5550.webserver.datamodels;

public class Range {
    public static final long DISABLED_START = Long.MIN_VALUE;
    public static final long DISABLED_END = Long.MAX_VALUE;

    private final long theStart;
    private final long theEnd;

    public Range(long aStart, long aEnd) {
        theStart = aStart;
        theEnd = aEnd;
    }

    public long getStart() {
        return theStart;
    }

    public long getEnd() {
        return theEnd;
    }
}
