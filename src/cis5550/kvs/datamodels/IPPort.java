package cis5550.kvs.datamodels;

public record IPPort(String ip, int port) {
    public static IPPort fromString(String aString) {
        String[] myParts = aString.split(":");
        return new IPPort(myParts[0], Integer.parseInt(myParts[1]));
    }
}
