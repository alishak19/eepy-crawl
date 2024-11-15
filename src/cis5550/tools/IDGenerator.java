package cis5550.tools;

public class IDGenerator {
    public static String generateLowerCaseID(int aLength) {
        StringBuilder myIDBuilder = new StringBuilder();
        for (int i = 0; i < aLength; i++) {
            int myRandom = (int) (Math.random() * 26 + 97);
            myIDBuilder.append((char) myRandom);
        }
        return myIDBuilder.toString();
    }
}
