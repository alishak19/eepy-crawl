package cis5550.webserver.utils;

import cis5550.webserver.datamodels.Range;

public class RangeSplicer {
    public static byte[] getRange(Range aRange, byte[] aBytes) {
        if (aRange == null || (aRange.getStart() == Range.DISABLED_START && aRange.getEnd() == Range.DISABLED_END)) {
            return aBytes;
        }

        long myStart = aRange.getStart();
        long myEnd = aRange.getEnd();

        if (myStart == Range.DISABLED_START) {
            byte[] myEndBytes = new byte[(int) myEnd];
            System.arraycopy(aBytes, 0, myEndBytes, 0, (int) myEnd);
            return myEndBytes;
        }
        if (myEnd == Range.DISABLED_END) {
            byte[] myStartBytes = new byte[(int) (aBytes.length - myStart)];
            System.arraycopy(aBytes, (int) myStart, myStartBytes, 0, aBytes.length - (int) myStart);
            return myStartBytes;
        }

        byte[] myRangeBytes = new byte[(int) (myEnd - myStart + 1)];
        System.arraycopy(aBytes, (int) myStart, myRangeBytes, 0, (int) (myEnd - myStart + 1));
        return myRangeBytes;
    }
}
