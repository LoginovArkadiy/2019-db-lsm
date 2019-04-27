package ru.mail.polis.persistence;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.jetbrains.annotations.NotNull;

import com.google.common.collect.Iterators;

public class MemTable implements Table {
    private final NavigableMap<ByteBuffer, Value> map = new TreeMap<>();
    private long sizeInBytes;

    @Override
    public long sizeInBytes() {
        return sizeInBytes;
    }

    @NotNull
    @Override
    public Iterator<Cell> iterator(@NotNull ByteBuffer from) {
        return Iterators.transform(
                map.tailMap(from).entrySet().iterator(),
                e -> {
                    if (e != null) {
                        return new Cell(e.getKey(), e.getValue());
                    }
                    return null;
                });
    }

    @Override
    public void upsert(@NotNull ByteBuffer key, @NotNull ByteBuffer value) {
        final Value previous = map.put(key, Value.of(value));
        if (previous == null) {
            sizeInBytes += key.remaining() + value.remaining();
        } else if (previous.isRemoved()) {
            sizeInBytes += value.remaining();
        } else {
            sizeInBytes += value.remaining() - previous.getData().remaining();
        }
    }

    @Override
    public void remove(@NotNull ByteBuffer key) {
        final Value previous = map.put(key, Value.tombstone());
        if (previous == null) {
            sizeInBytes += key.remaining();
        } else if (previous.isRemoved()) {
            //Do nothing
        } else {
            sizeInBytes -= previous.getData().remaining();
        }
    }
}
