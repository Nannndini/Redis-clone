package com.rediscone;

import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory key-value store with optional TTL-based expiry.
 * Uses ConcurrentHashMap for thread-safe access and lazy expiry on read.
 */
public class DataStore {

    /**
     * Represents a stored value with an optional expiration timestamp.
     */
    private static class StoreEntry {
        final String value;
        final long expiresAt; // epoch millis, -1 means no expiry

        StoreEntry(String value, long expiresAt) {
            this.value = value;
            this.expiresAt = expiresAt;
        }

        boolean isExpired() {
            return expiresAt != -1 && System.currentTimeMillis() > expiresAt;
        }
    }

    private final ConcurrentHashMap<String, StoreEntry> store = new ConcurrentHashMap<>();

    /**
     * Set a key-value pair with an optional expiry.
     * @param key the key
     * @param value the value
     * @param expiryMs expiry duration in milliseconds; -1 means no expiry
     */
    public void set(String key, String value, long expiryMs) {
        long expiresAt = (expiryMs > 0) ? System.currentTimeMillis() + expiryMs : -1;
        store.put(key, new StoreEntry(value, expiresAt));
    }

    /**
     * Get a value by key. Returns null if the key doesn't exist or has expired.
     * Performs lazy eviction: expired keys are removed on access.
     */
    public String get(String key) {
        StoreEntry entry = store.get(key);
        if (entry == null) {
            return null;
        }
        if (entry.isExpired()) {
            store.remove(key);
            return null;
        }
        return entry.value;
    }

    /**
     * Check if a key exists (and is not expired).
     */
    public boolean exists(String key) {
        return get(key) != null;
    }

    /**
     * Delete a key.
     * @return true if the key existed (and was not expired).
     */
    public boolean delete(String key) {
        StoreEntry entry = store.remove(key);
        return entry != null && !entry.isExpired();
    }

    /**
     * Get all non-expired keys.
     */
    public java.util.Set<String> keys() {
        // Clean up expired keys lazily during iteration
        store.entrySet().removeIf(e -> e.getValue().isExpired());
        return store.keySet();
    }

    /**
     * Set a key with an absolute expiry timestamp (for RDB loading).
     * @param key the key
     * @param value the value
     * @param expiresAtMs absolute expiry timestamp in epoch millis; -1 means no expiry
     */
    public void setWithAbsoluteExpiry(String key, String value, long expiresAtMs) {
        store.put(key, new StoreEntry(value, expiresAtMs));
    }
}
