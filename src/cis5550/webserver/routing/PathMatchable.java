package cis5550.webserver.routing;

import cis5550.webserver.datamodels.RequestType;

import java.util.Map;

public interface PathMatchable {
    Map<String, String> match(RequestType aMethod, String aUrl);
}
