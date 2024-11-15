package cis5550.flame;

import cis5550.tools.Serializer;
import cis5550.webserver.Request;

import static cis5550.utils.StringUtils.isNullOrEmpty;

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
                !isNullOrEmpty(myInputTable) ? URLDecoder.decode(myInputTable, StandardCharsets.UTF_8) : null,
                !isNullOrEmpty(myOutputTable) ? URLDecoder.decode(myOutputTable, StandardCharsets.UTF_8) : null,
                !isNullOrEmpty(myFromKey) ? URLDecoder.decode(myFromKey, StandardCharsets.UTF_8) : null,
                !isNullOrEmpty(myToKeyExclusive) ? URLDecoder.decode(myToKeyExclusive, StandardCharsets.UTF_8) : null,
                !isNullOrEmpty(myKvsCoordinator) ? URLDecoder.decode(myKvsCoordinator, StandardCharsets.UTF_8) : null,
                !isNullOrEmpty(request.bodyAsBytes()) ? Serializer.byteArrayToObject(request.bodyAsBytes(), aJarFile) : null,
                myZeroElement != null ? URLDecoder.decode(myZeroElement, StandardCharsets.UTF_8) : null);
    }
}
