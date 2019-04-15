/*
 * Copyright 2018 (c) Vadim Tsesko <incubos@yandex.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ru.mail.polis;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Storage interface.
 *
 * @author incubos
 */
public interface DAO extends Closeable {
    @NotNull
    Iterator<Record> iterator(@NotNull ByteBuffer from) throws IOException;

    /**
     * Get {@link Iterator} of {@link Record}s with keys between {@code from} inclusive and {@code to} exclusive.
     */
    @NotNull
    default Iterator<Record> range(
            @NotNull ByteBuffer from,
            @Nullable ByteBuffer to) throws IOException {
        if (to == null) {
            return iterator(from);
        }

        if (from.compareTo(to) > 0) {
            return Iters.empty();
        }

        final Record bound = new Record(to, ByteBuffer.allocate(0));
        return Iters.until(iterator(from), bound);
    }

    /**
     * Get value for the {@code key} or {@link NoSuchElementException} if no value present.
     */
    @NotNull
    default ByteBuffer get(@NotNull ByteBuffer key) throws IOException, NoSuchElementException {
        final Iterator<Record> iter = iterator(key);
        if (!iter.hasNext()) {
            throw new NoSuchElementException("Not found");
        }

        final Record next = iter.next();
        if (next.getKey().equals(key)) {
            return next.getValue();
        } else {
            throw new NoSuchElementException("Not found");
        }
    }

    void upsert(
            @NotNull ByteBuffer key,
            @NotNull ByteBuffer value) throws IOException;

    void remove(@NotNull ByteBuffer key) throws IOException;

    default void compact() throws IOException {
        // Implement me when you get to stage 3
    }
}
