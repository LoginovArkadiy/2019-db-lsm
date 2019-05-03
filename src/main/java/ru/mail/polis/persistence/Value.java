package ru.mail.polis.persistence;

import java.nio.ByteBuffer;

import org.jetbrains.annotations.NotNull;

public class Value implements Comparable<Value> {
    private final long ts;
    private final ByteBuffer data;

    private Value(final long ts, final ByteBuffer data) {
        assert (ts >= 0);
        this.ts = ts;
        this.data = data;
    }

    public static Value of(final ByteBuffer data) {
        return new Value(System.currentTimeMillis(), data.duplicate());
    }

    public static Value of(final long time, final ByteBuffer data) {
        return new Value(time, data.duplicate());
    }

    static Value tombstone() {
        return tombstone(System.currentTimeMillis());
    }

    static Value tombstone(long time) {
        return new Value(time, null);
    }

    boolean isRemoved() {
        return data == null;
    }

    ByteBuffer getData() {
        if (data == null) {
            throw new IllegalArgumentException("");
        }
        return data.asReadOnlyBuffer();
    }

    @Override
    public int compareTo(@NotNull Value o) {
        return Long.compare(o.ts, ts);
    }

    long getTimeStamp() {
        return ts;
    }
}
