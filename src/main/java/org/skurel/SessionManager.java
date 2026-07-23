package org.skurel;

import java.util.concurrent.ConcurrentHashMap;

public class SessionManager {
    // Thread-safe map: Key = MSISDN (String), Value = SessionID (String)
    private static final ConcurrentHashMap<String, String> sessionMap = new ConcurrentHashMap<>();

    // 1. Store or update a pair
    public static void saveSession(String msisdn, String sessionId) {
        sessionMap.put(msisdn, sessionId);
    }
    public static void save(String msisdn, String sessionId) {
        if (msisdn != null && sessionId != null) {
        	sessionMap.put(msisdn, sessionId);
        }
    }
    public static String retrieve(String msisdn) {
        if (msisdn == null) {
            return null;
        }
        return sessionMap.get(msisdn);
    }
    // 2. Retrieve a SessionID using MSISDN
    public String getSession(String msisdn) {
        return sessionMap.get(msisdn);
    }

    // 3. Remove a pair when a session expires
    public void removeSession(String msisdn) {
        sessionMap.remove(msisdn);
    }
}
