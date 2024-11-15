package cis5550.webserver.parsers;

import cis5550.webserver.datamodels.Range;

import java.util.Objects;

public class RangeParser {
    public static boolean isValidRange(Range aRange, long aFileSize) {
        if (aRange == null) {
            return false;
        }

        long myStart = aRange.getStart();
        long myEnd = aRange.getEnd();
        if (myStart == Range.DISABLED_START && myEnd == Range.DISABLED_END) {
            return false;
        }

        if (myStart == Range.DISABLED_START) {
            return myEnd >= 0 && myEnd < aFileSize;
        }

        if (myEnd == Range.DISABLED_END) {
            return myStart >= 0 && myStart < aFileSize;
        }

        return myStart >= 0 && myEnd >= 0 && myStart < aFileSize && myEnd < aFileSize && myStart <= myEnd;
    }

    public static Range parseRange(String aRange) {
        if (Objects.isNull(aRange)) {
            return null;
        }
        String[] myRangeParts = aRange.split("=");
        if (myRangeParts.length != 2) {
            return null;
        }
        String myRange = myRangeParts[1];
        String[] myRangeValues = myRange.split("-");
        if (myRangeValues.length > 2 || myRangeValues.length == 0) {
            return null;
        }
        try {
            if (myRangeValues.length != 2) {
                long myStart = Long.parseLong(myRangeValues[0]);
                return new Range(myStart, Range.DISABLED_END);
            }
            if (!myRangeValues[0].isEmpty() && myRangeValues[1].isEmpty()) {
                long myStart = Long.parseLong(myRangeValues[0]);
                return new Range(myStart, Range.DISABLED_END);
            }
            if (myRangeValues[0].isEmpty() && !myRangeValues[1].isEmpty()) {
                long myEnd = Long.parseLong(myRangeValues[1]);
                return new Range(Range.DISABLED_START, myEnd);
            }
            long myStart = Long.parseLong(myRangeValues[0]);
            long myEnd = Long.parseLong(myRangeValues[1]);
            return new Range(myStart, myEnd);
        } catch (NumberFormatException e) {
            return null;
        }
    }
}
