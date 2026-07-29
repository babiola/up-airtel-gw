package org.skurel;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.util.concurrent.TimeUnit;

public class SessionManager {
    
    // Automatically removes sessions 5 minutes after they are created/updated
    private static final Cache<String, String> sessionCache = Caffeine.newBuilder()
            .expireAfterWrite(2, TimeUnit.MINUTES) 
            .maximumSize(100_000) // Safety ceiling to prevent OOM under heavy load
            .build();

    public static void save(String msisdn, String sessionId) {
        if (msisdn != null && sessionId != null) {
            sessionCache.put(msisdn, sessionId);
        }
    }

    public static String retrieve(String msisdn) {
        if (msisdn == null) {
            return null;
        }
        return sessionCache.getIfPresent(msisdn);
    }

    public static void remove(String msisdn) {
        if (msisdn != null) {
            sessionCache.invalidate(msisdn);
        }
    }
}
