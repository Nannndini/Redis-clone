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
     * Get all non-expired keys (string + list keys combined).
     */
    public java.util.Set<String> keys() {
        // Clean up expired keys lazily during iteration
        store.entrySet().removeIf(e -> e.getValue().isExpired());
        java.util.Set<String> allKeys = new java.util.HashSet<>(store.keySet());
        allKeys.addAll(lists.keySet());
        return allKeys;
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

    // ── List operations ─────────────────────────────────────────────────

    private final ConcurrentHashMap<String, java.util.LinkedList<String>> lists = new ConcurrentHashMap<>();

    /**
     * Push one or more values onto the head (left) of a list.
     * Creates the list if it doesn't exist.
     * @return the length of the list after the push.
     */
    public int lpush(String key, String... values) {
        lists.computeIfAbsent(key, k -> new java.util.LinkedList<>());
        java.util.LinkedList<String> list = lists.get(key);
        for (String v : values) {
            list.addFirst(v);
        }
        return list.size();
    }

    /**
     * Push one or more values onto the tail (right) of a list.
     * Creates the list if it doesn't exist.
     * @return the length of the list after the push.
     */
    public int rpush(String key, String... values) {
        lists.computeIfAbsent(key, k -> new java.util.LinkedList<>());
        java.util.LinkedList<String> list = lists.get(key);
        for (String v : values) {
            list.addLast(v);
        }
        return list.size();
    }

    /**
     * Pop a value from the head (left) of a list.
     * Removes the list if it becomes empty.
     * @return the popped value, or null if the list doesn't exist or is empty.
     */
    public String lpop(String key) {
        java.util.LinkedList<String> list = lists.get(key);
        if (list == null || list.isEmpty()) {
            return null;
        }
        String value = list.removeFirst();
        if (list.isEmpty()) {
            lists.remove(key);
        }
        return value;
    }

    /**
     * Get a subrange of elements from a list.
     * Negative indices count from the end (-1 = last element).
     * @return list of elements in the range, or empty list if key doesn't exist.
     */
    public java.util.List<String> lrange(String key, int start, int stop) {
        java.util.LinkedList<String> list = lists.get(key);
        if (list == null || list.isEmpty()) {
            return java.util.Collections.emptyList();
        }
        int size = list.size();

        // Normalize negative indices
        if (start < 0) start = size + start;
        if (stop < 0) stop = size + stop;

        // Clamp to valid range
        if (start < 0) start = 0;
        if (stop >= size) stop = size - 1;

        if (start > stop || start >= size) {
            return java.util.Collections.emptyList();
        }

        return new java.util.ArrayList<>(list.subList(start, stop + 1));
    }

    /**
     * Get the length of a list.
     * @return length, or 0 if the key doesn't exist.
     */
    public int llen(String key) {
        java.util.LinkedList<String> list = lists.get(key);
        return (list == null) ? 0 : list.size();
    }

    /**
     * Check if a list key exists and is non-empty.
     */
    public boolean listExists(String key) {
        java.util.LinkedList<String> list = lists.get(key);
        return list != null && !list.isEmpty();
    }

    /**
     * Pop a value from the tail (right) of a list.
     * Removes the list if it becomes empty.
     * @return the popped value, or null if the list doesn't exist or is empty.
     */
    public String rpop(String key) {
        java.util.LinkedList<String> list = lists.get(key);
        if (list == null || list.isEmpty()) {
            return null;
        }
        String value = list.removeLast();
        if (list.isEmpty()) {
            lists.remove(key);
        }
        return value;
    }

    // ── Type introspection ──────────────────────────────────────────────

    /**
     * Get the Redis type of a key: "string", "list", or "none".
     */
    public String getKeyType(String key) {
        StoreEntry entry = store.get(key);
        if (entry != null && !entry.isExpired()) {
            return "string";
        }
        if (entry != null && entry.isExpired()) {
            store.remove(key);
        }
        if (listExists(key)) {
            return "list";
        }
        return "none";
    }

    // ── Atomic increment ────────────────────────────────────────────────

    /**
     * Atomically increment the integer stored at key by 1.
     * If the key does not exist, it is set to 0 before incrementing.
     * @return the new value after incrementing.
     * @throws NumberFormatException if the stored value is not a valid integer.
     */
    public long incr(String key) {
        StoreEntry entry = store.get(key);
        long current = 0;
        if (entry != null) {
            if (entry.isExpired()) {
                store.remove(key);
            } else {
                current = Long.parseLong(entry.value); // throws if not a number
            }
        }
        long newValue = current + 1;
        store.put(key, new StoreEntry(String.valueOf(newValue), -1));
        return newValue;
    }
}
