package cis5550.webserver;

import java.util.HashMap;
import java.util.Map;

public class SessionImpl implements Session {
    private String theId;
    private long theCreationTime;
    private long theLastAccessedTime;
    private int theMaxActiveInterval;
    private boolean theIsValid;
    private Map<String, Object> theAttributes;

    public SessionImpl(String aId, long aCreationTime, int aMaxActiveInterval) {
        theId = aId;
        theCreationTime = aCreationTime;
        theLastAccessedTime = aCreationTime;
        theMaxActiveInterval = aMaxActiveInterval;
        theAttributes = new HashMap<>();
        theIsValid = true;
    }

    @Override
    public String id() {
        return theId;
    }

    @Override
    public long creationTime() {
        return theCreationTime;
    }

    @Override
    public long lastAccessedTime() {
        return theLastAccessedTime;
    }

    @Override
    public void maxActiveInterval(int seconds) {
        theMaxActiveInterval = seconds;
    }

    @Override
    public void invalidate() {
        theIsValid = false;
    }

    @Override
    public Object attribute(String name) {
        return theAttributes.getOrDefault(name, null);
    }

    @Override
    public void attribute(String name, Object value) {
        theAttributes.put(name, value);
    }

    public void updateLastAccessedTime() {
        theLastAccessedTime = System.currentTimeMillis();
    }

    public boolean hasExpired() {
        return !theIsValid || System.currentTimeMillis() - theLastAccessedTime > (long) theMaxActiveInterval * 1000;
    }
}
