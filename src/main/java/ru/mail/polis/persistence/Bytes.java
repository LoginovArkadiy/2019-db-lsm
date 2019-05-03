package ru.mail.polis.persistence;

import java.nio.ByteBuffer;

class Bytes {

    static ByteBuffer fromInt(final int value) {
        final ByteBuffer result = ByteBuffer.allocate(Integer.BYTES);
        return result.putInt(value).rewind();
    }

    static ByteBuffer fromLong(final long value) {
        final ByteBuffer result = ByteBuffer.allocate(Long.BYTES);
        return result.putLong(value).rewind();
    }
}
