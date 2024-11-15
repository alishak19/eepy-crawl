package cis5550.test;

import static cis5550.webserver.Server.*;

public class HW3LocalTest {
    public static void main(String args[]) throws Exception {
        securePort(443);
        get("/",
            (req, res) -> {
                return "Hello, World!";
            }
        );
    }
}
