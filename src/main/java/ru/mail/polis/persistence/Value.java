package ru.mail.polis.persistence;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

import org.jetbrains.annotations.NotNull;

public final class Value implements Comparable<Value> {
    private final long ts;
    private final ByteBuffer data;
    private static AtomicInteger nano = new AtomicInteger();
    private static long preTime = 0;

    private Value(final long ts, final ByteBuffer data) {
        assert ts >= 0;
        this.ts = ts;
        this.data = data;
    }

    public static Value of(final ByteBuffer data) {
        long time = System.currentTimeMillis() * 1_000 + nano.incrementAndGet();
        if (time - preTime > 1) {
            nano.set(0);
        }
        preTime = time;
        return new Value(System.currentTimeMillis() * 1_000_000 + nano.incrementAndGet(), data.duplicate());
    }

    public static Value of(final long time, final ByteBuffer data) {
        return new Value(time, data.duplicate());
    }

    static Value tombstone() {
        return tombstone(System.currentTimeMillis());
    }

    static Value tombstone(final long time) {
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
    public int compareTo(@NotNull final Value value) {
        return Long.compare(value.ts, ts);
    }

    long getTimeStamp() {
        return ts;
    }
}
