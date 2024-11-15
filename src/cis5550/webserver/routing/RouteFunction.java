package cis5550.webserver.routing;

import cis5550.webserver.Request;
import cis5550.webserver.Response;

@FunctionalInterface
public interface RouteFunction {
    void handle(Request aRequest, Response aResponse) throws Exception;
}

