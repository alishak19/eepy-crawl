package cis5550.webserver.sessions;

import cis5550.webserver.SessionImpl;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class SessionsContainer implements Runnable {
    public static final int DEFAULT_MAX_ACTIVE_INTERVAL = 300;
    private final Map<String, SessionImpl> theSessions;

    private boolean theStarted = false;

    public SessionsContainer() {
        theSessions = new ConcurrentHashMap<>();
    }

    public SessionImpl getSession(String aSessionId) {
        SessionImpl mySession = theSessions.getOrDefault(aSessionId, null);
        if (mySession != null) {
            if (mySession.hasExpired()) {
                theSessions.remove(aSessionId);
                return null;
            }

            mySession.updateLastAccessedTime();
            return mySession;
        }
        return null;
    }

    public SessionImpl createSession() {
        while (true) {
            String mySessionId = UUID.randomUUID().toString();
            if (!theSessions.containsKey(mySessionId)) {
                SessionImpl mySession =
                        new SessionImpl(mySessionId, System.currentTimeMillis(), DEFAULT_MAX_ACTIVE_INTERVAL);
                theSessions.put(mySessionId, mySession);
                return mySession;
            }
        }
    }

    public boolean hasStarted() {
        return theStarted;
    }

    @Override
    public void run() {
        theStarted = true;
        while (true) {
            try {
                Thread.sleep(DEFAULT_MAX_ACTIVE_INTERVAL);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            for (Map.Entry<String, SessionImpl> myEntry : theSessions.entrySet()) {
                if (myEntry.getValue().hasExpired()) {
                    theSessions.remove(myEntry.getKey());
                }
            }
        }
    }
}
