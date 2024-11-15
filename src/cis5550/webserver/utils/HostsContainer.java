package cis5550.webserver.utils;

import cis5550.tools.Logger;
import cis5550.tools.SNIInspector;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SNIHostName;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import java.io.FileInputStream;
import java.net.Socket;
import java.security.KeyStore;
import java.util.HashMap;
import java.util.Map;

public class HostsContainer {
    public static final String DEFAULT_HOST_PATH = "keystore.jks";
    public static final String DEFAULT_HOST_PASSWORD = "secret";

    private static final Logger LOGGER = Logger.getLogger(HostsContainer.class);
    private final int thePort;
    private final Map<String, String> theHostPaths;
    private final Map<String, String> theHostPasswords;

    public HostsContainer(int aPort) {
        theHostPaths = new HashMap<>();
        theHostPasswords = new HashMap<>();
        thePort = aPort;
    }

    public void addHost(String aHost, String aPath, String aPassword) {
        theHostPaths.put(aHost, aPath);
        theHostPasswords.put(aHost, aPassword);
    }

    public Socket getHostSocket(Socket aSocket) throws Exception {
        SNIInspector mySNIInspector = new SNIInspector();
        mySNIInspector.parseConnection(aSocket);

        SNIHostName myHost = mySNIInspector.getHostName();
        if (myHost == null) {
            return createHTTPSSocket(DEFAULT_HOST_PATH, DEFAULT_HOST_PASSWORD, aSocket, mySNIInspector);
        }
        String myHostString = myHost.getAsciiName();
        if (theHostPaths.containsKey(myHostString)) {
            return createHTTPSSocket(
                    theHostPaths.get(myHostString), theHostPasswords.get(myHostString), aSocket, mySNIInspector);
        } else {
            return createHTTPSSocket(DEFAULT_HOST_PATH, DEFAULT_HOST_PASSWORD, aSocket, mySNIInspector);
        }
    }

    private Socket createHTTPSSocket(String aPath, String aPassword, Socket aSocket, SNIInspector aSNIInspector)
            throws Exception {
        KeyStore keyStore = KeyStore.getInstance("JKS");
        keyStore.load(new FileInputStream(aPath), aPassword.toCharArray());
        KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance("SunX509");
        keyManagerFactory.init(keyStore, aPassword.toCharArray());
        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(keyManagerFactory.getKeyManagers(), null, null);
        SSLSocketFactory factory = sslContext.getSocketFactory();
        return factory.createSocket(aSocket, aSNIInspector.getInputStream(), true);
    }
}
