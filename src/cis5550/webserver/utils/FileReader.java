package cis5550.webserver.utils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class FileReader {
    public static byte[] readFile(String aPath) {
        try {
            return Files.readAllBytes(Paths.get(aPath));
        } catch (IOException e) {
            return null;
        }
    }
}
