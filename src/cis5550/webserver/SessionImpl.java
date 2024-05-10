package cis5550.webserver;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

class SessionImpl implements Session {
    private String sessionId;
    private long creationTime;
    private long lastAccessedTime;
    private int maxActiveInterval;
    private boolean isNewSession = true;
    private Map<String, Object> attributes;

    public SessionImpl(String sessionId) {
        this.sessionId = sessionId;
        this.creationTime = System.currentTimeMillis();
        this.lastAccessedTime = System.currentTimeMillis();
        this.attributes = new ConcurrentHashMap<>();
        this.maxActiveInterval = 300;
        
        // Thread for individual session expiration
        new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                for (Session session : Server.sessionHashMap.values()) {
                    checkSessionExpiration(session);
                }
            }
        }).start();

        // Thread for global session expiration
        new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                synchronized (Server.sessionHashMap) {
                    for (Session session : Server.sessionHashMap.values()) {
                        checkSessionExpiration(session);
                    }
                }
            }
        }).start();
    }

    // Method to check session expiration
    private void checkSessionExpiration(Session session) {
        long currentTime = System.currentTimeMillis();
        if (currentTime - lastAccessedTime > maxActiveInterval * 1000) {
            session.invalidate();
        }
    }

    @Override
    public String id() {
        return this.sessionId;
    }

    @Override
    public long creationTime() {
        return creationTime;
    }

    @Override
    public long lastAccessedTime() {
        return lastAccessedTime;
    }

    @Override
    public void maxActiveInterval(int seconds) {
        this.maxActiveInterval = seconds;
    }

    @Override
    public void invalidate() {
        this.creationTime = -1;
        this.lastAccessedTime = -1;
        this.maxActiveInterval = -1;
        this.sessionId = null;
        attributes.clear();
    }

    @Override
    public Object attribute(String name) {
        return this.attributes.get(name);
    }

    @Override
    public void attribute(String name, Object value) {
        this.attributes.put(name, value);
    }
    
    public void setLastAccessedTime(long lastAccessedTime) {
        if (System.currentTimeMillis() - lastAccessedTime <= maxActiveInterval * 1000) {
            this.lastAccessedTime = lastAccessedTime;
        }
    }

    public boolean isNewSession() {
        return isNewSession;
    }

    public void setSessionStateAsOld() {
        this.isNewSession = false;
    }
    
    public int getMaxActiveInterval() {
        return maxActiveInterval;
    }
}
