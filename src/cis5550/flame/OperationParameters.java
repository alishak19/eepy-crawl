package cis5550.flame;

import cis5550.tools.Serializer;
import cis5550.webserver.Request;

import java.io.File;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;

public record OperationParameters(
        String inputTable,
        String outputTable,
        String fromKey,
        String toKeyExclusive,
        String kvsCoordinator,
        Object lambda,
        String zeroElement) {
    public static OperationParameters fromRequest(Request request, File aJarFile) {
        String myInputTable = request.queryParams("inputTable");
        String myOutputTable = request.queryParams("outputTable");
        String myFromKey = request.queryParams("fromKey");
        String myToKeyExclusive = request.queryParams("toKeyExclusive");
        String myKvsCoordinator = request.queryParams("kvsCoordinator");
        String myZeroElement = request.queryParams("zeroElement");

        return new OperationParameters(
                myInputTable != null ? URLDecoder.decode(myInputTable, StandardCharsets.UTF_8) : null,
                myOutputTable != null ? URLDecoder.decode(myOutputTable, StandardCharsets.UTF_8) : null,
                myFromKey != null ? URLDecoder.decode(myFromKey, StandardCharsets.UTF_8) : null,
                myToKeyExclusive != null ? URLDecoder.decode(myToKeyExclusive, StandardCharsets.UTF_8) : null,
                myKvsCoordinator != null ? URLDecoder.decode(myKvsCoordinator, StandardCharsets.UTF_8) : null,
                request.bodyAsBytes() != null ? Serializer.byteArrayToObject(request.bodyAsBytes(), aJarFile) : null,
                myZeroElement != null ? URLDecoder.decode(myZeroElement, StandardCharsets.UTF_8) : null);
    }
}
